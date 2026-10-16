from __future__ import annotations

import hashlib
import json
import re
import shlex
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from trilogy.constants import Rendering
    from trilogy.core.statements.execute import ProcessedQuery
    from trilogy.dialect.config import DialectConfig
    from trilogy.dialect.python_source import PythonDatasourceError
    from trilogy.engine import ResultProtocol
    from trilogy.executor import Executor
    from trilogy.io.contract import SourceRequest
    from trilogy.staging import StagingConfig


from trilogy.constants import logger
from trilogy.core.enums import (
    AddressType,
    FunctionType,
    GroupMode,
    JoinType,
    Modifier,
    UnnestMode,
)
from trilogy.core.models.core import CONCRETE_TYPES, DataType
from trilogy.core.models.datasource import Address
from trilogy.core.models.execute import (
    CTE,
    CompiledCTE,
    QueryDatasource,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.dialect.base import BaseDialect, is_array_arg
from trilogy.dialect.cancel import duckdb_side_cursor
from trilogy.utility import safe_open

SENTINAL_AUTO_CAPTURE_GROUP_VALUE = "-1"


def null_wrapper(
    lval: str,
    rval: str,
    modifiers: list[Modifier],
    jointype: JoinType | None = None,
) -> str:

    if Modifier.NULLABLE in modifiers:
        return f"{lval} is not distinct from {rval}"
    return f"{lval} = {rval}"


def generate_regex_extract(x: list[str]) -> str:
    if str(x[2]) == SENTINAL_AUTO_CAPTURE_GROUP_VALUE:
        regex = re.compile(x[1])
        if regex.groups == 0:
            search = 0
        else:
            search = 1
        return f"REGEXP_EXTRACT({x[0]},{x[1]},{search})"
    return f"REGEXP_EXTRACT({x[0]},{x[1]},{x[2]})"


def render_sort(args, types):
    if len(args) == 1:
        return f"list_sort({args[0]})"
    order = args[1].split(" ", 1)
    if len(order) == 1:
        return f"list_sort({args[0]}, '{order[0]}')"
    elif len(order) == 2:
        return f"list_sort({args[0]}, '{order[0]}', '{order[1]}')"


def render_log(args):
    if len(args) == 1:
        return f"log({args[0]})"
    elif len(args) == 2:
        if int(args[1]) == 10:
            return f"log({args[0]})"
        else:
            # change of base formula
            return f"log({args[0]})/log({args[1]})"
    else:
        raise ValueError("log function requires 1 or 2 arguments")


def map_date_part_specifier(specifier: str) -> str:
    """Map date part specifiers to DuckDB-compatible names"""
    mapping = {
        "day_of_week": "dow",
        # Add other mappings if needed
    }
    return mapping.get(specifier, specifier)


def date_trunc(args, types):
    if len(types) > 0 and types[0] == DataType.DATETIME:
        return f"cast(date_trunc('{args[1]}', {args[0]}) as datetime)"
    else:
        return f"date_trunc('{args[1]}', {args[0]})"


def handle_cast(args, types):
    if types[0] == DataType.TIMESTAMP and types[1] == DataType.DATE:
        return f"cast({args[0]}  AT TIME ZONE 'UTC' as date)"
    if types[0] == DataType.TIMESTAMP and types[1] == DataType.DATETIME:
        return f"cast({args[0]}  AT TIME ZONE 'UTC' as datetime)"
    return f"cast({args[0]} as {args[1]})"


def contains(args, types):
    # array membership is exact; substring search is case-insensitive
    if is_array_arg(types):
        return f"CONTAINS({args[0]}, {args[1]})"
    return f"CONTAINS(LOWER({args[0]}), LOWER({args[1]}))"


def date_part(args, types):
    if args[1] == "day_of_week":
        return f"date_part('{map_date_part_specifier(args[1])}', {args[0]})+1"
    return f"date_part('{map_date_part_specifier(args[1])}', {args[0]})"


def generate_simple_case(args):
    output_args = []
    for arg in args[1:]:
        if arg.strip().startswith("ELSE"):
            output_args.append(arg)
        else:
            output_args.append(f"WHEN {arg}")
    return f"CASE\n\t{args[0]}\n\t" + "\n\t".join(output_args) + "\n\tEND"


def render_geo_transform(args: list[str]) -> str:
    return f"ST_Transform({args[0]}, CONCAT('EPSG:', CAST({args[1]} AS VARCHAR)), CONCAT('EPSG:', CAST({args[2]} AS VARCHAR)))"


FUNCTION_MAP = {
    FunctionType.CAST: handle_cast,
    FunctionType.LOG: lambda args, types: render_log(args),
    FunctionType.SPLIT: lambda args, types: (
        f"STRING_SPLIT({','.join([f''' {a!s} ''' for a in args])})"
    ),
    ## Duckdb indexes from 1, not 0
    ## Duckdb uses list for array
    FunctionType.ARRAY_DISTINCT: lambda args, types: f"list_distinct({args[0]})",
    FunctionType.ARRAY_SUM: lambda args, types: f"list_sum({args[0]})",
    FunctionType.ARRAY_SORT: render_sort,
    FunctionType.ARRAY_TRANSFORM: lambda args, types: (
        f"list_transform({args[0]}, {args[1]} -> {args[2]})"
    ),
    # datetime is aliased,
    FunctionType.CURRENT_DATETIME: lambda x, types: "cast(get_current_timestamp() as datetime)",
    FunctionType.DATETIME: lambda x, types: f"cast({x[0]} as datetime)",
    FunctionType.TIMESTAMP: lambda x, types: f"cast({x[0]} as timestamp)",
    FunctionType.DATE: lambda x, types: f"cast({x[0]} as date)",
    FunctionType.DATE_TRUNCATE: lambda x, types: date_trunc(x, types),
    FunctionType.DATE_ADD: lambda x, types: f"date_add({x[0]}, {x[2]} * INTERVAL 1 {x[1]})",
    FunctionType.DATE_SUB: lambda x, types: f"date_add({x[0]}, -{x[2]} * INTERVAL 1 {x[1]})",
    FunctionType.DATE_PART: lambda x, types: date_part(x, types),
    FunctionType.DATE_DIFF: lambda x, types: f"date_diff('{x[2]}', {x[0]}, {x[1]})",
    # concat() skips NULLs, || (CONCAT_STRICT) propagates, concat_ws(sep, ...)
    # joins with a separator skipping NULLs — the reference semantics for all
    # dialects
    FunctionType.CONCAT: lambda x, types: f"CONCAT({', '.join(x)})",
    FunctionType.CONCAT_WS: lambda x, types: f"CONCAT_WS({', '.join(x)})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"dayofweek({x[0]})+1",
    # string
    FunctionType.CONTAINS: lambda x, types: contains(x, types),
    # regexp
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"REGEXP_MATCHES({x[0]},{x[1]})",
    FunctionType.REGEXP_EXTRACT: lambda x, types: generate_regex_extract(x),
    FunctionType.SIMPLE_CASE: lambda x, types: generate_simple_case(x),
    FunctionType.GEO_POINT: lambda x, types: f"ST_Point({x[0]}, {x[1]})",
    FunctionType.GEO_DISTANCE: lambda x, types: f"ST_Distance({x[0]}, {x[1]})",
    FunctionType.GEO_TRANSFORM: lambda x, types: render_geo_transform(x),
}

DATATYPE_MAP: dict[DataType, str] = {}


# Identifies the disabled-form `uv_run` guard in an existing macro body, so a
# connecting executor can tell "already set up" from "set up for the other
# config" with a read-only catalog lookup. Must appear in the disabled SQL below
# and never in the enabled one.
PYTHON_DATASOURCE_GUARD_MARKER = "enable_python_datasources=True in DuckDBConfig"


def get_python_datasource_setup_sql(
    enabled: bool,
    is_windows: bool = False,
    instance_id: str | None = None,
    staging: StagingConfig | None = None,
) -> str:
    """Return SQL to setup the uv_run macro for Python script datasources.
    Inspired by: https://sidequery.dev/blog/uv-run-duckdb

    Args:
        enabled: If True, installs extensions and creates working macro.
                 If False, creates macro that throws a clear error.
        is_windows: If True, uses temp file workaround for shellfs pipe bug.
        instance_id: Unique identifier for this executor instance (thread-safe).
        staging: Staging config for temp file location. Defaults to system tempdir.
    """
    if not enabled:  # Use a subquery that throws an error when evaluated
        # This ensures the error message is shown before column binding
        return """
CREATE OR REPLACE MACRO uv_run(script, args := '') AS TABLE
SELECT * FROM (
    SELECT CASE
        WHEN true THEN error('Python script datasources require enable_python_datasources=True in DuckDBConfig. '
                            || 'Set this in your trilogy.conf under [engine.config] or pass DuckDBConfig(enable_python_datasources=True) to the executor.')
    END as __error__
) WHERE __error__ IS NOT NULL;
"""

    if is_windows:
        import uuid

        from trilogy.staging import StagingConfig

        # Windows workaround: shellfs has a bug with Arrow IPC pipes on Windows.
        # We use a temp file approach: run script to file, then read file.
        # The read_json forces the shell command to complete before read_arrow.
        # Each uv_run call gets a unique file via md5(script||args) so multiple
        # calls in the same query don't overwrite each other.
        # The executor-namespaced subdir (UUID) isolates concurrent executors.

        unique_id = instance_id or str(uuid.uuid4())
        staging = staging or StagingConfig()
        base_dir = staging.prepare_executor_subdir(unique_id)
        python_executable = Path(sys.executable).as_posix()
        return f"""
INSTALL shellfs FROM community;
INSTALL arrow FROM community;
LOAD shellfs;
LOAD arrow;
SET VARIABLE __trilogy_uv_temp_dir = '{base_dir}';
CREATE OR REPLACE MACRO uv_run(script, args := '') AS TABLE
WITH __build AS MATERIALIZED (
SELECT a.name
FROM read_json('call "{python_executable}" -m trilogy.dialect.duckdb_uv "' || getvariable('__trilogy_uv_temp_dir') || md5(script || args) || '.arrow" "' || getvariable('__trilogy_uv_temp_dir') || md5(script || args) || '.err" "' || script || '" "' || args || '" |') AS a
LIMIT 1
)
-- __build writes the .arrow file as a side effect. It MUST be referenced (the
-- cross-join below) or some duckdb versions prune the unreferenced CTE and never
-- run the write. AS MATERIALIZED forces the CTE to execute to completion before
-- the outer read_arrow scan, so the file is fully written before it is read.
-- The wrapper keeps uv stderr in a sidecar file and retries transient uv cache
-- lock/contention errors before exposing the final failure to DuckDB.
SELECT r.* FROM __build b, read_arrow(getvariable('__trilogy_uv_temp_dir') || md5(script || args) || '.arrow') AS r;
"""
    else:
        import uuid

        from trilogy.staging import StagingConfig, StagingType

        # A shell redirect needs a local path, so a remote staging root falls
        # back to the system tempdir for the stderr sidecar.
        if staging is None or staging.staging_type != StagingType.LOCAL:
            staging = StagingConfig()
        err_dir = staging.prepare_executor_subdir(instance_id or str(uuid.uuid4()))
        return f"""
INSTALL shellfs FROM community;
INSTALL arrow FROM community;
LOAD shellfs;
LOAD arrow;
SET VARIABLE __trilogy_uv_temp_dir = '{err_dir}';
CREATE OR REPLACE MACRO uv_run(script, args := '') AS TABLE
    SELECT * FROM read_arrow(CASE WHEN suffix(script, '.py') THEN 'uv run --no-project --quiet ' ELSE '' END || script || ' ' || args || ' 2>''' || getvariable('__trilogy_uv_temp_dir') || md5(script || args) || '.err'' |');
"""


VGI_CATALOG = "trilogy_io"
# What a source run with --vgi serves (trilogy.io.vgi / the trilogy-io crate);
# each is attached under its own alias.
VGI_SOURCE_CATALOG = "trilogy_source"


def vgi_location(argv: list[str], is_windows: bool) -> str:
    """``argv`` as the shell command the vgi extension launches (and pools)."""
    if not is_windows:
        return shlex.join(argv)
    command = subprocess.list2cmdline(argv)
    # cmd.exe /c strips the first and last quote of a line that starts with
    # one, so a quoted executable needs an outer pair to survive.
    return f'"{command}"' if command.startswith('"') else command


def vgi_worker_command(python_executable: str, is_windows: bool) -> str:
    """The command that runs the bridge worker."""
    return vgi_location(
        [python_executable, "-m", "trilogy.dialect.duckdb_vgi"], is_windows
    )


def resident_command(location: str, version: str | None = None) -> list[str]:
    """How to start a source as its own VGI worker (``--vgi``).

    vgi pools worker processes by their command line, so ``version`` goes on it
    (after ``--vgi``, which is all a served source reads): otherwise a worker for
    an edited source is drawn from the pool still running the old code.
    """
    from trilogy.dialect.python_source import UV_RUN_PREFIX
    from trilogy.io.errors import VGI_FLAG

    tail = [VGI_FLAG, "--vgi-version", version] if version else [VGI_FLAG]
    if Path(location).suffix == ".py":
        # The script's own dependencies need not include vgi.
        return [*UV_RUN_PREFIX, "--with", "vgi-python", location, *tail]
    return [location, *tail]


RESIDENT_ALIAS_PREFIX = "trilogy_src_"


def source_version(location: str) -> str:
    """Modification times of the code a source runs. A python script may import
    the modules beside it, so its version covers every ``.py`` in its directory."""
    path = Path(location)
    files = sorted(path.parent.glob("*.py")) if path.suffix == ".py" else [path]
    stamps = []
    for file in files:
        try:
            stamps.append(f"{file.name}:{file.stat().st_mtime_ns}")
        except OSError:
            stamps.append(f"{file.name}:0")
    return "|".join(stamps)


def resident_alias(location: str) -> str:
    """A catalog alias per source *version*: an edited or rebuilt source gets a
    fresh worker rather than the pooled one still running the old code."""
    version = source_version(location)
    digest = hashlib.md5(f"{location}|{version}".encode()).hexdigest()[:12]
    return f"{RESIDENT_ALIAS_PREFIX}{digest}"


def detach_resident(connection: Any, alias: str) -> None:
    """Drop a superseded worker rather than leave it idling out its pool timeout."""
    side = duckdb_side_cursor(connection)
    if side is None:
        return
    try:
        side.execute(f"DETACH DATABASE IF EXISTS {alias}")
    except Exception as e:
        logger.info("could not detach superseded resident %s: %s", alias, e)
    finally:
        side.close()


# How long an idle resident worker is kept. The vgi default (5s) is shorter than
# the gap between an agent's queries, so each query would restart the source
# (uv, interpreter, imports: seconds) and lose the point of a resident worker.
RESIDENT_IDLE_SECONDS = 3600


def attach_resident(connection: Any, location: str) -> str | None:
    """Attach ``location`` as a pooled worker; its alias, or None if it cannot serve.

    Runs on its own side cursor: an attach is database-wide, a failed one must
    not abort the transaction the query is about to run in, and separate
    cursors let several sources start at once.
    """
    side = duckdb_side_cursor(connection)
    if side is None:
        return None
    alias = resident_alias(location)
    version = alias.removeprefix(RESIDENT_ALIAS_PREFIX)
    command = vgi_location(resident_command(location, version), sys.platform == "win32")
    escaped = command.replace("'", "''")
    try:
        side.execute(
            f"ATTACH IF NOT EXISTS '{VGI_SOURCE_CATALOG}' AS {alias} "
            f"(TYPE vgi, LOCATION '{escaped}', "
            f"POOL_TIMEOUT {RESIDENT_IDLE_SECONDS})"
        )
    except Exception as e:
        logger.info("%s is not resident, using the VGI bridge: %s", location, e)
        return None
    finally:
        side.close()
    return alias


VGI_SETUP_SQL = """
INSTALL vgi FROM community;
LOAD vgi;
"""


def bridge_attach_sql(python_executable: str, is_windows: bool) -> str:
    location = vgi_worker_command(python_executable, is_windows).replace("'", "''")
    return f"ATTACH IF NOT EXISTS '{VGI_CATALOG}' (TYPE vgi, LOCATION '{location}')"


def attach_bridge(connection: Any) -> None:
    """Attach the bridge worker that serves sources which cannot run resident.

    Lazy: a model whose sources all serve themselves never starts it.
    """
    side = duckdb_side_cursor(connection)
    if side is None:
        return
    try:
        side.execute(
            bridge_attach_sql(Path(sys.executable).as_posix(), sys.platform == "win32")
        )
    finally:
        side.close()


# DuckDB reports a failed shellfs pipe only by its command line; both uv_run
# forms above embed the script and its stderr sidecar there.
_PIPE_FAILURE = re.compile(r"Pipe process exited abnormally code=(-?\d+): (.*?) \|")
_PIPE_SCRIPT = re.compile(
    r'duckdb_uv "[^"]*" "[^"]*" "([^"]*)"|--quiet (\S+)|^(?!uv |call )(\S+) .*2>'
)
_PIPE_STDERR = re.compile(r"""["']([^"']+\.err)["']""")


def vgi_failure(message: str) -> PythonDatasourceError | None:
    """The script failure the VGI bridge worker reported, if ``message`` holds one."""
    from trilogy.dialect.python_source import VGI_FAILURE_MARKER, PythonDatasourceError

    start = message.find(VGI_FAILURE_MARKER)
    if start == -1:
        return None
    try:
        payload, _ = json.JSONDecoder().raw_decode(
            message, start + len(VGI_FAILURE_MARKER) + 1
        )
    except json.JSONDecodeError:
        return None
    return PythonDatasourceError(
        payload["script"], payload["return_code"], payload["stderr"]
    )


def python_datasource_failure(error: Exception) -> PythonDatasourceError | None:
    """The script failure behind a DuckDB ``uv_run`` pipe error, if it is one."""
    from trilogy.dialect.python_source import PythonDatasourceError

    vgi = vgi_failure(str(error))
    if vgi is not None:
        return vgi
    failure = _PIPE_FAILURE.search(str(error))
    if failure is None:
        return None
    command = failure.group(2)
    script = _PIPE_SCRIPT.search(command)
    sidecar = _PIPE_STDERR.search(command)
    stderr = ""
    if sidecar is not None:
        try:
            stderr = Path(sidecar.group(1)).read_text(
                encoding="utf-8", errors="replace"
            )
        except OSError:
            pass
    return PythonDatasourceError(
        next(g for g in script.groups() if g) if script else command,
        int(failure.group(1)),
        stderr,
    )


def reads_program(cte: CTE | UnionCTE) -> bool:
    """Whether the CTE's own FROM/JOINs run a program datasource."""
    return any(
        not isinstance(datasource, QueryDatasource)
        and isinstance(datasource.address, Address)
        and datasource.address.type.is_program
        for datasource in cte.source.datasources
    )


def rescanned_ctes(ctes: list[CTE | UnionCTE]) -> set[str]:
    """CTEs DuckDB would evaluate more than once if inlined.

    A CTE read by two consumers, or by a recursive CTE (whose step re-reads
    it every iteration), is otherwise inlined at each reference.
    """
    consumers: dict[str, int] = {}
    recursive: set[str] = set()
    pending = list(ctes)
    while pending:
        cte = pending.pop()
        if isinstance(cte, (UnionCTE, RecursiveCTE)):
            pending.extend(cte.internal_ctes)
        for parent in {p.name for p in cte.parent_ctes}:
            consumers[parent] = consumers.get(parent, 0) + 1
            if isinstance(cte, RecursiveCTE):
                recursive.add(parent)
    return {name for name, count in consumers.items() if count > 1} | recursive


def get_gcs_setup_sql(enabled: bool) -> str:
    """Return SQL to setup GCS extension with optional HMAC credentials.

    Args:
        enabled: If True, installs httpfs. If credentials are available,
                 also creates a secret for authenticated access.
                 If False, does nothing.

    Environment variables (optional, required only for write access):
        GOOGLE_HMAC_KEY: GCS HMAC access key ID
        GOOGLE_HMAC_SECRET: GCS HMAC secret key
    """
    if not enabled:
        return ""

    key_id = environ.get("GOOGLE_HMAC_KEY")
    secret = environ.get("GOOGLE_HMAC_SECRET")

    # Always install httpfs for read access to public buckets
    base_sql = """
INSTALL httpfs;
LOAD httpfs;
"""

    # If credentials are available, create a secret for authenticated access
    if key_id and secret:
        return base_sql + f"""
CREATE OR REPLACE SECRET __trilogy_gcs_secret (
    TYPE gcs,
    KEY_ID '{key_id}',
    SECRET '{secret}'
);
"""
    return base_sql


def check_gcs_write_credentials() -> None:
    """Validate that GCS write credentials are available.

    Raises ValueError if GOOGLE_HMAC_KEY and GOOGLE_HMAC_SECRET are not set.
    Call this before attempting to write to GCS.
    """
    key_id = environ.get("GOOGLE_HMAC_KEY")
    secret = environ.get("GOOGLE_HMAC_SECRET")

    if not key_id or not secret:
        raise ValueError(
            "Writing to GCS requires GOOGLE_HMAC_KEY and GOOGLE_HMAC_SECRET "
            "environment variables to be set"
        )


# Fixed seed for reservoir sampling during ingest introspection, so detected
# grain (and the generated .preql) is reproducible across runs.
DUCKDB_SAMPLE_SEED = 42


class DuckDBDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        **DATATYPE_MAP,
    }
    QUOTE_CHARACTER = '"'
    LIMIT_PARENTHESIZED = True
    SUPPORTS_QUALIFY = True
    UNNEST_MODE = UnnestMode.DIRECT
    GROUP_MODE = GroupMode.BY_INDEX
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_ALIAS_IN_HAVING = True
    SUPPORTS_RESULT_SUMMARY = True
    NULL_WRAPPER = staticmethod(null_wrapper)
    TABLE_NOT_FOUND_PATTERN = "Catalog Error: Table with name"
    # <=1.4: `... URL "x": 404 (Not Found)`; >=1.5: `... on 'x' (HTTP 404 Not Found)`
    HTTP_NOT_FOUND_PATTERN = r"404[\s(]*Not Found"
    COLUMN_NOT_FOUND_PATTERN = "does not have a column named"
    # A half-written parquet: no footer magic (truncated upload), nothing to
    # read at all (zero bytes), or a footer thrift decode that fails.
    CORRUPT_SOURCE_PATTERN = (
        r"No magic bytes found at end of file"
        r"|too small to be a Parquet file"
        r"|TProtocolException"
    )

    def summarize_result(
        self, query: ProcessedQuery, run_sql: Callable[[str], ResultProtocol]
    ) -> tuple[list[dict], int] | None:
        """Full-result column stats via DuckDB ``SUMMARIZE`` over the un-limited
        query (one pass; ``distinct`` is ``approx_unique`` — HLL-approximate)."""
        sql = self.compile_without_limit(query)
        result = run_sql(f"SUMMARIZE (\n{sql}\n)")
        ix = {name: i for i, name in enumerate(result.keys())}
        stats: list[dict] = []
        total = 0
        for r in result.fetchall():
            count = r[ix["count"]] or 0
            total = count
            npct = r[ix["null_percentage"]]
            nulls = round(count * float(npct) / 100) if npct is not None else 0
            entry: dict = {
                "column": r[ix["column_name"]],
                "non_null": count - nulls,
                "nulls": nulls,
                # HLL-approximate, and named so: it can exceed the row count, which
                # reads as a bug if the key claims to be exact. Use count_distinct
                # for a true cardinality.
                "distinct_approx": r[ix["approx_unique"]],
            }
            mn, mx = r[ix["min"]], r[ix["max"]]
            if mn is not None:
                entry["min"] = mn
            if mx is not None:
                entry["max"] = mx
            stats.append(entry)
        return stats, total

    def __init__(
        self,
        rendering: Rendering | None = None,
        config: DialectConfig | None = None,
        staging: StagingConfig | None = None,
        instance_id: str | None = None,
    ):
        super().__init__(
            rendering=rendering,
            config=config,
            staging=staging,
            instance_id=instance_id,
        )
        from trilogy.dialect.config import DuckDBConfig, PythonDatasourceTransport

        self._vgi = (
            isinstance(config, DuckDBConfig)
            and config.enable_python_datasources
            and config.python_datasource_transport == PythonDatasourceTransport.VGI
        )
        # Sources prepare_sources started as their own pooled worker, by
        # location -> attached alias. Anything absent renders via the bridge.
        self._residents: dict[str, str] = {}
        self._not_resident: set[str] = set()
        self._bridge_attached = False
        # Resident workers have to be attached before a query references them.
        self.REQUIRES_SOURCE_PREPARATION = self._vgi
        if isinstance(config, DuckDBConfig) and config.gcs_cache_bust:
            import random

            self._gcs_cache_bust_token: str | None = str(random.randint(1, 2**31))
        else:
            self._gcs_cache_bust_token = None

    _GCS_PREFIXES = ("gcs://", "gs://", "https://storage.googleapis.com")

    def prepare_sources(self, addresses: Iterable[Address], executor: Executor) -> None:
        """Start each program source as its own pooled VGI worker, once per version.

        Sources are started lazily, by the first query that reads them, and
        all of a query's new sources start in parallel: the query waits for the
        slowest startup rather than the sum. A source that cannot serve itself
        (a raw script, a pytrilogy without ``--vgi``, a binary built without
        the ``vgi`` feature) fails to attach and is read through the bridge,
        which is itself attached only once a query needs it.
        """
        programs = list(
            dict.fromkeys(a.location for a in addresses if a.type.is_program)
        )
        # Re-checked per query, so a source edited under a running server is
        # served by a fresh worker from the next query on.
        pending = [
            p
            for p in programs
            if p not in self._not_resident
            and self._residents.get(p) != resident_alias(p)
        ]
        if pending:
            with ThreadPoolExecutor(max_workers=min(len(pending), 8)) as pool:
                aliases = list(
                    pool.map(partial(attach_resident, executor.connection), pending)
                )
            for location, alias in zip(pending, aliases):
                superseded = self._residents.pop(location, None)
                if alias is None:
                    self._not_resident.add(location)
                else:
                    self._residents[location] = alias
                if superseded is not None and superseded != alias:
                    detach_resident(executor.connection, superseded)
        if not self._bridge_attached and any(p in self._not_resident for p in programs):
            attach_bridge(executor.connection)
            self._bridge_attached = True

    def generate_ctes(self, query: ProcessedQuery) -> list[CompiledCTE]:
        # An inlined script read reruns the program per reference (per
        # iteration under recursion): slower, and each run a new snapshot.
        compiled = super().generate_ctes(query)
        rescanned = rescanned_ctes(query.ctes)
        for cte, out in zip(query.ctes[:-1], compiled[:-1]):
            out.materialized = cte.name in rescanned and reads_program(cte)
        return compiled

    def _maybe_bust_gcs_url(self, url: str) -> str:
        if self._gcs_cache_bust_token and url.startswith(self._GCS_PREFIXES):
            return f"{url}?cache_bust={self._gcs_cache_bust_token}"
        return url

    def render_source(
        self, address: Address, request: SourceRequest | None = None
    ) -> str:
        hive = ", hive_partitioning=true" if address.partition_columns else ""
        if address.additional_locations:
            paths = ", ".join(
                f"'{self._maybe_bust_gcs_url(p)}'" for p in address.all_locations
            )
            location_arg = f"[{paths}]"
        else:
            location_arg = f"'{self._maybe_bust_gcs_url(address.location)}'"
        if address.type == AddressType.CSV:
            return f"read_csv({location_arg}{hive})"
        if address.type == AddressType.TSV:
            return f"read_csv({location_arg}, delim='\\t'{hive})"
        if address.type == AddressType.PARQUET:
            return f"read_parquet({location_arg}{hive})"
        if address.type.is_program:
            from trilogy.dialect.config import DuckDBConfig

            if not (
                isinstance(self.config, DuckDBConfig)
                and self.config.enable_python_datasources
            ):
                raise ValueError(
                    "Python script datasources require enable_python_datasources=True in DuckDBConfig. "
                    "Set this in your trilogy.conf under [engine.config] or pass "
                    "DuckDBConfig(enable_python_datasources=True) to the executor."
                )
            if self.staging and self.instance_id:
                self.staging.prepare_executor_subdir(self.instance_id)
            from trilogy.dialect.source_pushdown import render_args

            args = " ".join(filter(None, [address.source_args, render_args(request)]))
            if self._vgi:
                escaped = args.replace("'", "''")
                alias = self._residents.get(address.location)
                if alias is not None:
                    return f"{alias}.main.source('{escaped}')"
                return f"{VGI_CATALOG}.main.script('{address.location}', '{escaped}')"
            if not args:
                return f"uv_run('{address.location}')"
            # Filter tokens are single-quoted for the shell (see
            # source_pushdown.render_args), so the SQL literal doubles them.
            # Everything else render_args emits is already SQL-safe; see
            # source_pushdown._is_transport_safe.
            escaped = args.replace("'", "''")
            return f"uv_run('{address.location}', args := '{escaped}')"
        if address.type == AddressType.SQL:
            with safe_open(address.location) as f:
                sql_content = f.read().strip()
            return f"({sql_content})"
        return super().render_source(address, request)

    def get_file_columns(
        self, executor, address: Address
    ) -> dict[str, DataType] | None:
        rows = executor.execute_raw_sql(
            f"DESCRIBE SELECT * FROM {self.render_source(address)}"
        ).fetchall()
        return {row[0].lower(): self.normalize_db_type(row[1]) for row in rows}

    # DuckDB information_schema returns type names (e.g. "INTEGER", "VARCHAR") that
    # differ from the DDL tokens in DATATYPE_MAP (e.g. "int", "string").
    DB_COLUMN_TYPE_MAP: ClassVar[dict[str, DataType]] = {
        **BaseDialect.DB_COLUMN_TYPE_MAP,
        "int1": DataType.INTEGER,
        "hugeint": DataType.BIGINT,
        "logical": DataType.BOOL,
        "timestamp": DataType.DATETIME,
    }

    def refine_runtime_value_type_for_validation(
        self,
        executor,
        value: Any,
        inferred_type: CONCRETE_TYPES,
        expected_type: CONCRETE_TYPES,
        result_type: CONCRETE_TYPES | None = None,
    ) -> CONCRETE_TYPES:
        if (
            inferred_type == DataType.BYTES
            and expected_type == DataType.GEOGRAPHY
            and result_type == DataType.GEOGRAPHY
        ):
            return DataType.GEOGRAPHY

        return inferred_type

    def get_result_column_types_for_validation(
        self, result: Any
    ) -> dict[str, CONCRETE_TYPES] | None:
        cursor = getattr(result, "cursor", None)
        description = getattr(cursor, "description", None)
        if not description:
            return None

        output: dict[str, CONCRETE_TYPES] = {}
        for column in description:
            if len(column) < 2:
                continue
            name = str(column[0]).lower()
            datatype = self.normalize_db_type(str(column[1]))
            output[name] = datatype
        return output

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Get primary key columns by joining key_column_usage with table_constraints."""
        pk_query = f"""
        SELECT kcu.column_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_name = tc.table_name
        WHERE kcu.table_name = '{table_name}'
            AND tc.constraint_type = 'PRIMARY KEY'
        """

        if schema:
            pk_query += f" AND kcu.table_schema = '{schema}'"

        pk_query += " ORDER BY kcu.ordinal_position"

        rows = executor.execute_raw_sql(pk_query).fetchall()
        if rows:
            return [row[0] for row in rows]

        return []

    def get_table_sample(
        self,
        executor,
        table_name: str,
        schema: str | None = None,
        sample_size: int = 10000,
    ) -> list[tuple]:
        """Reservoir-sample rows for introspection instead of a plain LIMIT.

        A ``LIMIT`` returns the first physical rows, which in a clustered table
        share a leading key value (e.g. all one ``date_sk``) — that hides true
        grain columns and fakes uniqueness on the rest, so grain detection picks
        a spurious key. Reservoir sampling spans the whole table; the fixed seed
        keeps the sample — and thus generated .preql — reproducible across runs.
        """
        qualified_name = f"{schema}.{table_name}" if schema else table_name
        query = (
            f"SELECT * FROM {self.safe_quote(qualified_name)} "
            f"USING SAMPLE {sample_size} ROWS (reservoir, {DUCKDB_SAMPLE_SEED})"
        )
        return executor.execute_raw_sql(query).fetchall()
