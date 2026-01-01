import re
from os import environ
from pathlib import Path
from typing import Any, Callable, Mapping

from jinja2 import Template

from trilogy.core.enums import (
    AddressType,
    FunctionType,
    Modifier,
    UnnestMode,
    WindowType,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Address
from trilogy.dialect.base import BaseDialect

WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

SENTINAL_AUTO_CAPTURE_GROUP_VALUE = "-1"


def null_wrapper(
    lval: str,
    rval: str,
    modifiers: list[Modifier],
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


FUNCTION_MAP = {
    FunctionType.COUNT: lambda args, types: f"count({args[0]})",
    FunctionType.SUM: lambda args, types: f"sum({args[0]})",
    FunctionType.AVG: lambda args, types: f"avg({args[0]})",
    FunctionType.LENGTH: lambda args, types: f"length({args[0]})",
    FunctionType.LOG: lambda args, types: render_log(args),
    FunctionType.LIKE: lambda args, types: (
        f" CASE WHEN {args[0]} like {args[1]} THEN True ELSE False END"
    ),
    FunctionType.CONCAT: lambda args, types: (
        f"CONCAT({','.join([f''' {str(a)} ''' for a in args])})"
    ),
    FunctionType.SPLIT: lambda args, types: (
        f"STRING_SPLIT({','.join([f''' {str(a)} ''' for a in args])})"
    ),
    ## Duckdb indexes from 1, not 0
    FunctionType.INDEX_ACCESS: lambda args, types: (f"{args[0]}[{args[1]}]"),
    ## Duckdb uses list for array
    FunctionType.ARRAY_DISTINCT: lambda args, types: f"list_distinct({args[0]})",
    FunctionType.ARRAY_SUM: lambda args, types: f"list_sum({args[0]})",
    FunctionType.ARRAY_SORT: render_sort,
    FunctionType.ARRAY_TRANSFORM: lambda args, types: (
        f"list_transform({args[0]}, {args[1]} -> {args[2]})"
    ),
    FunctionType.ARRAY_AGG: lambda args, types: f"array_agg({args[0]})",
    # datetime is aliased,
    FunctionType.CURRENT_DATETIME: lambda x, types: "cast(get_current_timestamp() as datetime)",
    FunctionType.DATETIME: lambda x, types: f"cast({x[0]} as datetime)",
    FunctionType.TIMESTAMP: lambda x, types: f"cast({x[0]} as timestamp)",
    FunctionType.DATE: lambda x, types: f"cast({x[0]} as date)",
    FunctionType.DATE_TRUNCATE: lambda x, types: f"date_trunc('{x[1]}', {x[0]})",
    FunctionType.DATE_ADD: lambda x, types: f"date_add({x[0]}, {x[2]} * INTERVAL 1 {x[1]})",
    FunctionType.DATE_SUB: lambda x, types: f"date_add({x[0]}, -{x[2]} * INTERVAL 1 {x[1]})",
    FunctionType.DATE_PART: lambda x, types: f"date_part('{map_date_part_specifier(x[1])}', {x[0]})",
    FunctionType.DATE_DIFF: lambda x, types: f"date_diff('{x[2]}', {x[0]}, {x[1]})",
    FunctionType.CONCAT: lambda x, types: f"({' || '.join(x)})",
    FunctionType.DATE_LITERAL: lambda x, types: f"date '{x}'",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"datetime '{x}'",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"dayofweek({x[0]})",
    # string
    FunctionType.CONTAINS: lambda x, types: f"CONTAINS(LOWER({x[0]}), LOWER({x[1]}))",
    # regexp
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"REGEXP_MATCHES({x[0]},{x[1]})",
    FunctionType.REGEXP_EXTRACT: lambda x, types: generate_regex_extract(x),
}

# if an aggregate function is called on a source that is at the same grain as the aggregate
# we may return a static value
FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args, types: f"CASE WHEN{args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args, types: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args, types: f"{args[0]}",
    FunctionType.AVG: lambda args, types: f"{args[0]}",
    FunctionType.MAX: lambda args, types: f"{args[0]}",
    FunctionType.MIN: lambda args, types: f"{args[0]}",
}

DATATYPE_MAP: dict[DataType, str] = {}


def get_python_datasource_setup_sql(enabled: bool, is_windows: bool = False) -> str:
    """Return SQL to setup the uv_run macro for Python script datasources.
    Inspired by: https://sidequery.dev/blog/uv-run-duckdb

    Args:
        enabled: If True, installs extensions and creates working macro.
                 If False, creates macro that throws a clear error.
        is_windows: If True, uses temp file workaround for shellfs pipe bug.
    """
    if enabled:
        if is_windows:
            import atexit
            import os
            import tempfile

            # Windows workaround: shellfs has a bug with Arrow IPC pipes on Windows.
            # We use a temp file approach: run script to file, then read file.
            # The read_json forces the shell command to complete before read_arrow.
            # Using getvariable() defers file path resolution until execution.
            # Include PID in filename to avoid conflicts between parallel processes.
            # Use Path.resolve() to avoid 8.3 short names (e.g. RUNNER~1) on CI.

            temp_file = (
                str(Path(tempfile.gettempdir()).resolve()).replace("\\", "/")
                + f"/trilogy_uv_run_{os.getpid()}.arrow"
            )

            def cleanup_temp_file() -> None:
                try:
                    os.unlink(temp_file)
                except OSError:
                    pass

            atexit.register(cleanup_temp_file)
            return f"""
INSTALL shellfs FROM community;
INSTALL arrow FROM community;
LOAD shellfs;
LOAD arrow;

SET VARIABLE __trilogy_uv_temp_file = '{temp_file}';

CREATE OR REPLACE MACRO uv_run(script, args := '') AS TABLE
WITH __build AS (
    SELECT a.name
    FROM read_json('uv run --quiet ' || script || ' ' || args || ' > {temp_file} && echo {{"name": "done"}} |') AS a
    LIMIT 1
)
SELECT * FROM read_arrow(getvariable('__trilogy_uv_temp_file'));
"""
        else:
            return """
INSTALL shellfs FROM community;
INSTALL arrow FROM community;
LOAD shellfs;
LOAD arrow;

CREATE OR REPLACE MACRO uv_run(script, args := '') AS TABLE
SELECT * FROM read_arrow('uv run --quiet ' || script || ' ' || args || ' |');
"""
    else:
        # Use a subquery that throws an error when evaluated
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


DUCKDB_TEMPLATE = Template(
    """{%- if output %}
{{output}}
{% endif %}{%- if ctes %}
WITH {% if recursive%}RECURSIVE{% endif %}{% for cte in ctes %}
{{cte.name}} as (
{{cte.statement}}){% if not loop.last %},{% else %}
{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{%- else -%}{%- if comment -%}
-- {{ comment }}
{%- endif %}SELECT
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
    {{ base }}{% endif %}{% if joins %}
{%- for join in joins %}
    {{ join }}{% endfor %}{% endif %}
{%- if where %}
WHERE
    {{ where }}
{% endif -%}{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
    {{ having }}
{% endif %}{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT ({{ limit }}){% endif %}{% endif %}
"""
)


class DuckDBDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = {**BaseDialect.WINDOW_FUNCTION_MAP, **WINDOW_FUNCTION_MAP}
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    DATATYPE_MAP = {**BaseDialect.DATATYPE_MAP, **DATATYPE_MAP}
    QUOTE_CHARACTER = '"'
    SQL_TEMPLATE = DUCKDB_TEMPLATE
    UNNEST_MODE = UnnestMode.DIRECT
    NULL_WRAPPER = staticmethod(null_wrapper)
    TABLE_NOT_FOUND_PATTERN = "Catalog Error: Table with name"
    HTTP_NOT_FOUND_PATTERN = "404 (Not Found)"

    def render_source(self, address: Address) -> str:
        if address.type == AddressType.CSV:
            return f"read_csv('{address.location}')"
        if address.type == AddressType.TSV:
            return f"read_csv('{address.location}', delim='\\t')"
        if address.type == AddressType.PARQUET:
            return f"read_parquet('{address.location}')"
        if address.type == AddressType.PYTHON_SCRIPT:
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
            return f"uv_run('{address.location}')"
        if address.type == AddressType.SQL:
            with open(address.location, "r") as f:
                sql_content = f.read().strip()
            return f"({sql_content})"
        return super().render_source(address)

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[tuple]:
        """Returns a list of tuples: (column_name, data_type, is_nullable, column_comment)."""
        column_query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_comment
        FROM information_schema.columns
        WHERE table_name = ?
        """
        params = [table_name]

        if schema:
            column_query += " AND table_schema = ?"
            params.append(schema)

        column_query += " ORDER BY ordinal_position"

        # DuckDB supports parameterized queries
        rows = executor.execute_raw_sql(
            column_query.replace("?", "'{}'").format(*params)
        ).fetchall()
        return rows

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Get primary key columns by joining key_column_usage with table_constraints."""
        pk_query = """
        SELECT kcu.column_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_name = tc.table_name
        WHERE kcu.table_name = '{}'
            AND tc.constraint_type = 'PRIMARY KEY'
        """.format(
            table_name
        )

        if schema:
            pk_query += " AND kcu.table_schema = '{}'".format(schema)

        pk_query += " ORDER BY kcu.ordinal_position"

        rows = executor.execute_raw_sql(pk_query).fetchall()
        if rows:
            return [row[0] for row in rows]

        return []
