import glob as glob_module
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any

from trilogy import Executor
from trilogy.constants import logger
from trilogy.core.enums import Purpose
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.build import Factory
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    FreshnessLag,
    RawColumnExpr,
    UpdateKey,
    UpdateKeys,
    UpdateKeyType,
)
from trilogy.core.models.execute import CTE
from trilogy.execution.state.cache import ColumnStatsCache
from trilogy.execution.state.exceptions import (
    UNRESOLVABLE_ERRORS,
    is_missing_source_error,
    is_schema_mismatch_error,
)
from trilogy.execution.state.isolation import hidden_datasources


@dataclass
class DatasourceWatermark:
    """Watermark values for one datasource, keyed by watermark key.

    The key is the concept's **full address** as known to the emitting
    environment (``order_item.created_at.date``), or the literal
    ``"update_time"`` for a table-mtime watermark, which has no concept.
    Addresses are the one deterministic identity inside an environment —
    names collide (``events.created_at`` and ``orders.created_at`` are both
    ``created_at``) and a derived property's dotted name is ambiguous
    against them. Watermarks never cross environments by key: a snapshot
    read by a different model is re-keyed through the physical column
    binding (``snapshot._rekey_for``), and cross-script root injection is
    keyed by datasource identifier, which only matches when the namespaces
    — and therefore the addresses — match too.
    """

    keys: dict[str, UpdateKey]


class RefreshKind(Enum):
    SQL = "sql"
    SCRIPT = "script"


@dataclass
class StaleAsset:
    """Represents an asset that needs to be refreshed."""

    datasource_id: str
    reason: str
    filters: UpdateKeys = field(default_factory=UpdateKeys)
    kind: RefreshKind = RefreshKind.SQL
    # Stale slices of a partitioned asset, when the verdict was reached per
    # partition. The refresh narrows its select to exactly these, so a hole in
    # the middle of a range is filled without rebuilding its healthy neighbours.
    # Mutually exclusive with ``filters``: a missing slice may hold rows OLDER
    # than the incremental watermark, so ANDing the two would filter out the
    # very rows the refresh exists to write. Typed loosely to keep this module
    # free of a partitions.py import (which imports watermarks.py).
    partitions: list = field(default_factory=list)
    # The caller asked for this refresh by name (``--force``, ``--partition``)
    # rather than a probe deciding it. Execution must not re-derive it from live
    # staleness: a forced rebuild is wanted regardless, and a targeted slice may
    # look fresh (a backfill of a day the watermark is already past) while still
    # being exactly what the run was told to load.
    explicit: bool = False


def _compare_watermark_values(a: str | float | date, b: str | float | date) -> int:
    """Compare two watermark values, returning -1, 0, or 1.

    Handles type mismatches by comparing string representations.
    """
    if type(a) is type(b):
        if isinstance(a, datetime):
            a_aware = a.tzinfo is not None
            b_aware = b.tzinfo is not None  # type: ignore[union-attr]
            if a_aware != b_aware:
                raise TypeError(
                    f"offset-naive and offset-aware datetimes: {a!r} vs {b!r}"
                )
        if a < b:  # type: ignore[operator]
            return -1
        elif a > b:  # type: ignore[operator]
            return 1
        return 0
    sa, sb = str(a), str(b)
    if sa < sb:
        return -1
    elif sa > sb:
        return 1
    return 0


def _watermark_distance(
    current: str | float | date, expected: str | float | date
) -> timedelta | float | None:
    """How far ``current`` trails ``expected``, or None if not measurable.

    Ordering (``_compare_watermark_values``) is enough to say *whether* an asset
    is behind; a tolerance needs *how far*, which only exists for temporal and
    numeric values — not hashes or opaque strings.
    """
    if isinstance(current, datetime) and isinstance(expected, datetime):
        if (current.tzinfo is None) != (expected.tzinfo is None):
            raise TypeError(
                f"offset-naive and offset-aware datetimes: {current!r} vs {expected!r}"
            )
        return expected - current
    if isinstance(current, datetime) or isinstance(expected, datetime):
        return None
    if isinstance(current, date) and isinstance(expected, date):
        return expected - current
    if isinstance(current, bool) or isinstance(expected, bool):
        return None
    if isinstance(current, (int, float)) and isinstance(expected, (int, float)):
        return expected - current
    return None


def within_allowed_lag(
    current: str | float | date | None,
    expected: str | float | date,
    lag: FreshnessLag,
    key: str,
) -> bool:
    """Whether trailing ``expected`` by this much is still fresh.

    A missing value is never lag — an asset with no rows is empty, not behind.
    A tolerance that can't be applied to these values is a modelling error and
    raises rather than silently deciding either way.
    """
    if current is None:
        return False
    distance = _watermark_distance(current, expected)
    if distance is None:
        raise TypeError(
            f"`within` lag is set for '{key}' but its watermark values are not"
            f" measurable ({type(current).__name__} vs {type(expected).__name__});"
            " lag requires temporal or numeric watermarks"
        )
    tolerance = lag.as_timedelta
    if isinstance(distance, timedelta) and tolerance is not None:
        return distance <= tolerance
    if not isinstance(distance, timedelta) and tolerance is None:
        return distance <= lag.value
    raise TypeError(
        f"`within {lag.render()}` does not fit the watermark for '{key}'"
        f" ({type(current).__name__});"
        " use a unit for temporal watermarks and a bare number otherwise"
    )


def _execute_raw_sql_scalar(
    query: str, executor: Executor
) -> str | int | float | datetime | date | None:
    """Execute a raw SQL query and return the first column of the first row.

    Returns None if the source is missing; rolls back and suppresses the error.
    Re-raises all other exceptions.
    """
    dialect = executor.generator
    try:
        result = executor.execute_raw_sql(query).fetchone()
        return result[0] if result else None
    except Exception as e:
        if is_missing_source_error(e, dialect) or is_schema_mismatch_error(e, dialect):
            executor.connection.rollback()
            return None
        raise


def _resolve_table_ref(datasource: Datasource, executor: Executor) -> str:
    if isinstance(datasource.address, Address):
        return executor.generator.render_source(datasource.address)
    return datasource.safe_address


_CLOUD_PREFIXES = (
    "s3://",
    "gs://",
    "gcs://",
    "abfs://",
    "az://",
    "http://",
    "https://",
)


def is_missing_local_file(datasource: Datasource) -> bool:
    """Return True if the datasource points to a local file pattern that matches no files."""
    if not isinstance(datasource.address, Address) or not datasource.address.is_file:
        return False
    location = datasource.address.location
    if any(location.startswith(p) for p in _CLOUD_PREFIXES):
        return False
    return len(glob_module.glob(location)) == 0


def has_schema_mismatch(
    datasource: Datasource,
    executor: Executor,
    cache: ColumnStatsCache | None = None,
) -> bool:
    """Return True if the existing table's columns (names or types) differ from the definition."""
    if isinstance(datasource.address, Address) and (
        datasource.address.is_file or datasource.address.is_query
    ):
        return False
    table_name = datasource.safe_address
    if cache is not None:
        hit, actual = cache.get_columns(table_name)
        if not hit:
            actual = executor.generator.get_table_columns(executor, table_name)
            cache.set_columns(table_name, actual)
    else:
        actual = executor.generator.get_table_columns(executor, table_name)
    if actual is None:
        return False
    expected = {}
    for col in datasource.columns:
        concept = executor.environment.concepts[col.concept.address]
        # A concrete alias IS the physical column name — it is what the persist
        # DDL writes — so it, not the concept's safe_address, is what an
        # existing table must be compared against. Declaring
        # `carrier_code: flight.carrier.code` otherwise reads as a permanent
        # schema mismatch against a table trilogy itself just created. A
        # non-concrete alias (raw expression or function) names no column of its
        # own, so the concept's address stays the best available guess.
        name = col.alias if isinstance(col.alias, str) else concept.safe_address
        expected[name.lower()] = concept.datatype.data_type
    if set(actual) != set(expected):
        return True
    # Check types where the dialect can resolve them (skip UNKNOWN — can't map the type)
    from trilogy.core.models.core import DataType

    return any(
        actual[name] != expected[name]
        for name in expected
        if actual.get(name, DataType.UNKNOWN) != DataType.UNKNOWN
    )


def get_last_update_time_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    update_time = executor.generator.get_table_last_modified(
        executor, datasource.safe_address
    )
    return DatasourceWatermark(
        keys={
            "update_time": UpdateKey(
                concept_name="update_time",
                type=UpdateKeyType.UPDATE_TIME,
                value=update_time,
            )
        }
    )


def get_unique_key_hash_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    key_columns: list[ColumnAssignment] = [
        col
        for col in datasource.columns
        if executor.environment.concepts[col.concept.address].purpose == Purpose.KEY
    ]

    if not key_columns:
        return DatasourceWatermark(keys={})

    table_ref = _resolve_table_ref(datasource, executor)
    dialect = executor.generator
    watermarks = {}

    for col in key_columns:
        if isinstance(col.alias, str):
            column_name = col.alias
        elif isinstance(col.alias, RawColumnExpr):
            column_name = col.alias.text
        else:
            column_name = str(col.alias)
        hash_expr = dialect.hash_column_value(column_name)
        checksum_expr = dialect.aggregate_checksum(hash_expr)
        query = f"SELECT {checksum_expr} as checksum FROM {table_ref}"
        checksum_value = _execute_raw_sql_scalar(query, executor)

        watermarks[col.concept.address] = UpdateKey(
            concept_name=col.concept.address,
            type=UpdateKeyType.KEY_HASH,
            value=checksum_value,
        )

    return DatasourceWatermark(keys=watermarks)


def _get_max_watermarks(
    concept_refs: list[ConceptRef],
    datasource: Datasource,
    executor: Executor,
    key_type: UpdateKeyType,
) -> DatasourceWatermark:
    """Fetch MAX watermarks for concept refs using the appropriate query expression."""
    if not concept_refs:
        return DatasourceWatermark(keys={})

    table_ref = _resolve_table_ref(datasource, executor)
    factory = Factory(environment=executor.environment)
    dialect = executor.generator
    output_addresses = {c.address for c in datasource.output_concepts}
    watermarks = {}

    for concept_ref in concept_refs:
        concept = executor.environment.concepts[concept_ref.address]
        build_concept = factory.build(concept)
        build_datasource = factory.build(datasource)
        cte: CTE = CTE.from_datasource(build_datasource)

        if concept.address in output_addresses:
            query = f"SELECT MAX({dialect.render_concept_sql(build_concept, cte=cte, alias=False)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"
        elif build_concept.lineage is None:
            raise ValueError(
                f"Concept '{concept.address}' is set as a freshness field but does not"
                f" exist on datasource '{datasource.identifier}' and cannot be derived"
                f" from other datasource fields. Add it to the datasource column list"
                f" or change the freshness field."
            )
        else:
            query = f"SELECT MAX({dialect.render_expr(build_concept.lineage, cte=cte)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"

        max_value = _execute_raw_sql_scalar(query, executor)

        watermarks[concept.address] = UpdateKey(
            concept_name=concept.address,
            type=key_type,
            value=max_value,
        )

    return DatasourceWatermark(keys=watermarks)


def get_incremental_key_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    return _get_max_watermarks(
        datasource.incremental_by, datasource, executor, UpdateKeyType.INCREMENTAL_KEY
    )


def get_freshness_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    return _get_max_watermarks(
        datasource.freshness_by, datasource, executor, UpdateKeyType.UPDATE_TIME
    )


def _probe_root_max_values(
    concept_addresses: list[str],
    executor: Executor,
    root_assets: set[str],
) -> list[Any] | None:
    """One root-only MAX probe statement; None when planning fails.

    Only planning failures are absorbed (:data:`UNRESOLVABLE_ERRORS`)."""
    non_roots = [
        ds_id
        for ds_id in list(executor.environment.datasources)
        if ds_id not in root_assets
    ]
    # Positional aliases: an address contains dots, which no alias can carry.
    selected = ", ".join(
        f"MAX({address}) -> _wm_max_{i}" for i, address in enumerate(concept_addresses)
    )
    with hidden_datasources(executor.environment, non_roots):
        try:
            result = executor.execute_ephemeral(f"SELECT {selected};")
            row = result.fetchone() if result else None
        except UNRESOLVABLE_ERRORS as e:
            logger.debug(
                "[STATE_STORE] no root-derived expectation for %s: %s",
                concept_addresses,
                e,
            )
            return None
    if row is None:
        return [None] * len(concept_addresses)
    return list(row)


def get_concept_max_watermarks_abstract(
    concept_addresses: list[str],
    executor: Executor,
    root_assets: set[str],
) -> dict[str, UpdateKey]:
    """Compute MAX watermarks for derived concepts using only root datasources.

    Temporarily hides non-root datasources so the query planner is forced to
    resolve each concept exclusively from authoritative root sources. All
    concepts are grain-() scalars, so they batch into a single planned
    statement — one plan instead of one per concept.

    A concept the roots cannot answer yields a null value, not an exception —
    which is what the caller already codes against (it keeps the key only ``if
    wm.value is not None``), and the rule ``probe_expected_partitions`` follows
    for the partition-level version of the same question. It is also the normal
    answer for a model declaring no ``root`` at all: everything is hidden, so
    raising took the whole snapshot down and such a project could not report
    state. A batched statement fails planning as a unit, so on failure each
    concept is re-probed alone to preserve that per-concept contract.
    """
    unique = list(dict.fromkeys(concept_addresses))
    values: dict[str, Any] = {}
    if unique:
        batched = _probe_root_max_values(unique, executor, root_assets)
        if batched is not None:
            values = dict(zip(unique, batched))
        else:
            for address in unique:
                single = _probe_root_max_values([address], executor, root_assets)
                values[address] = None if single is None else single[0]
    return {
        address: UpdateKey(
            concept_name=address,
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=values[address],
        )
        for address in unique
    }


def get_concept_max_watermark_abstract(
    concept_address: str,
    executor: Executor,
    root_assets: set[str],
) -> UpdateKey:
    """Single-concept form of :func:`get_concept_max_watermarks_abstract`."""
    return get_concept_max_watermarks_abstract(
        [concept_address], executor, root_assets
    )[concept_address]


def get_concept_max_watermarks(
    datasource: Datasource,
    concept_refs: list[ConceptRef],
    executor: Executor,
) -> DatasourceWatermark:
    """Fetch MAX watermarks for given concept refs from a root datasource.

    Used to auto-watermark roots when non-root datasources reference those concepts
    in their freshness_by/incremental_by without requiring explicit root declarations.
    """
    table_ref = _resolve_table_ref(datasource, executor)
    output_addresses = {c.address for c in datasource.output_concepts}
    factory = Factory(environment=executor.environment)
    dialect = executor.generator
    watermarks = {}

    for concept_ref in concept_refs:
        if concept_ref.address not in output_addresses:
            continue
        concept = executor.environment.concepts[concept_ref.address]
        build_concept = factory.build(concept)
        build_datasource = factory.build(datasource)
        cte: CTE = CTE.from_datasource(build_datasource)
        query = f"SELECT MAX({dialect.render_concept_sql(build_concept, cte=cte, alias=False)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"

        max_value = _execute_raw_sql_scalar(query, executor)

        watermarks[concept.address] = UpdateKey(
            concept_name=concept.address,
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=max_value,
        )

    return DatasourceWatermark(keys=watermarks)


def run_freshness_probe(probe_path: str) -> bool:
    """Run a probe script to check datasource freshness.

    The script should exit 0 and print a truthy value (true/1/yes) if up-to-date,
    or a falsy value (false/0/no) if stale. A non-zero exit code raises RuntimeError.
    """
    result = subprocess.run(
        ["uv", "run", "--no-project", "--quiet", probe_path],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Freshness probe '{probe_path}' failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout.strip().lower() in ("true", "1", "yes")


def run_refresh_script(script_path: str, cwd: str | None = None) -> None:
    """Run a refresh script to make a refreshable-root datasource fresh.

    Exit code 0 = success; any non-zero code raises RuntimeError. stdout/stderr
    are forwarded to the trilogy logger; the script is opaque to trilogy beyond
    its exit code.
    """
    result = subprocess.run(
        ["uv", "run", "--no-project", "--quiet", script_path],
        capture_output=True,
        text=True,
        cwd=cwd,
        check=False,
    )
    if result.stdout:
        logger.info("refresh_script %s stdout: %s", script_path, result.stdout.strip())
    if result.stderr:
        logger.info("refresh_script %s stderr: %s", script_path, result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(
            f"Refresh script '{script_path}' failed (exit {result.returncode}): {result.stderr.strip()}"
        )
