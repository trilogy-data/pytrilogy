import re
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, ClassVar

from jinja2 import Template

from trilogy.constants import logger
from trilogy.core.enums import (
    AddressType,
    FunctionType,
    JoinType,
    Modifier,
    UnnestMode,
)
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    DataType,
)
from trilogy.core.models.datasource import Address
from trilogy.core.models.execute import CompiledCTE
from trilogy.core.statements.execute import CreateTableInfo, ProcessedQueryPersist
from trilogy.dialect.base import (
    BaseDialect,
    TableColumn,
    safe_quote,
)
from trilogy.dialect.base import null_wrapper as base_null_wrapper
from trilogy.dialect.bigquery_engine import BigQueryConnection
from trilogy.dialect.bigquery_staging import BigQueryPythonStaging

if TYPE_CHECKING:
    from trilogy.executor import Executor
    from trilogy.io.contract import SourceRequest

LOGGER_PREFIX = "[BIGQUERY_DIALECT]"


def transform_date_part(part: str) -> str:
    part_upper = part.upper()
    if part_upper == "DAY_OF_WEEK":
        return "DAYOFWEEK"
    return part_upper


def handle_length(args, types: list[DataType] | None = None) -> str:
    arg = args[0]
    if types and types[0].data_type == DataType.ARRAY:
        return f"ARRAY_LENGTH({arg})"
    return f"LENGTH({arg})"


def render_geo_transform(args: list[str]) -> str:
    source_srid = str(args[1]).strip().strip("'\"")
    target_srid = str(args[2]).strip().strip("'\"")
    if source_srid != "4326" or target_srid != "4326":
        raise ValueError(
            "BigQuery only supports geo_transform(..., 4326, 4326); "
            f"got ({args[1]}, {args[2]})"
        )
    return f"{args[0]}"


# A bare column reference, quoted the way every join key this module renders is
# (`alias`.`column`, or a bare `column` when the key is the rendering branch's
# own). Anything else -- a COALESCE over several sources, a CAST, a function --
# is an expression as far as BigQuery's join planner is concerned.
FIELD_REFERENCE = re.compile(r"`[^`]+`(?:\.`[^`]+`)*")


def null_wrapper(
    lval: str, rval: str, modifiers: list[Modifier], jointype: JoinType | None = None
) -> str:
    """Null-safe join key, in a shape BigQuery's FULL join planner accepts.

    A FULL OUTER JOIN's ON clause must contain "an equality of fields from both
    sides of the join". A top-level ``X = Y`` satisfies that whatever X and Y
    are, but the base dialect's null-safe expansion
    ``(l = r or (l is null and r is null))`` is an OR, and BigQuery only reads
    an OR back as a join key while *both* operands are plain fields -- an
    expression on either side is rejected, as is ``IS NOT DISTINCT FROM``. The
    expression case is the ordinary one: a key merged across several
    row-preserving sources renders as ``coalesce(a.x, b.x, c.x) = d.x``.

    ``TO_JSON_STRING`` on both sides restores a top-level equality and is
    null-safe for free (NULL encodes as bare ``null``, the string 'null' as
    ``"null"``). It costs a computed hash-join key, so field-keyed joins keep
    the cheaper expansion. Encoded FLOAT64s compare textually, so NaN matches
    itself and ``-0.0`` stops matching ``0.0``; a float join key is
    pathological, and the alternative is a query that does not run.

    Only FULL is restricted, and only the OR form -- everything else falls
    through to the base wrapper. Details, and the planner fixes that would make
    this dead code, in ``docs/handoff_bigquery_full_join_merged_keys.md``.
    """
    if (
        jointype is JoinType.FULL
        and Modifier.NULLABLE in modifiers
        and not (FIELD_REFERENCE.fullmatch(lval) and FIELD_REFERENCE.fullmatch(rval))
    ):
        return f"TO_JSON_STRING({lval}) = TO_JSON_STRING({rval})"
    return base_null_wrapper(lval, rval, modifiers, jointype)


FUNCTION_MAP = {
    FunctionType.LENGTH: lambda x, types: handle_length(x, types),
    FunctionType.IS_NULL: lambda x, types: f"{x[0]} IS NULL",
    FunctionType.MINUTE: lambda x, types: f"EXTRACT(MINUTE from {x[0]})",
    FunctionType.SECOND: lambda x, types: f"EXTRACT(SECOND from {x[0]})",
    FunctionType.HOUR: lambda x, types: f"EXTRACT(HOUR from {x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"EXTRACT(DAYOFWEEK from {x[0]})-1",  # BigQuery's DAYOFWEEK returns 1 for Sunday
    FunctionType.DAY: lambda x, types: f"EXTRACT(DAY from {x[0]})",
    FunctionType.YEAR: lambda x, types: f"EXTRACT(YEAR from {x[0]})",
    FunctionType.MONTH: lambda x, types: f"EXTRACT(MONTH from {x[0]})",
    FunctionType.WEEK: lambda x, types: f"EXTRACT(WEEK from {x[0]})",
    FunctionType.QUARTER: lambda x, types: f"EXTRACT(QUARTER from {x[0]})",
    # math
    FunctionType.POWER: lambda x, types: f"POWER({x[0]}, {x[1]})",
    FunctionType.DIVIDE: lambda x, types: f"COALESCE(SAFE_DIVIDE({x[0]},{x[1]}),0)",
    FunctionType.DATE_ADD: lambda x, types: f"DATE_ADD({x[0]}, INTERVAL {x[2]} {x[1]})",
    FunctionType.DATE_SUB: lambda x, types: f"DATE_SUB({x[0]}, INTERVAL {x[2]} {x[1]})",
    FunctionType.DATE_PART: lambda x, types: f"EXTRACT({transform_date_part(x[1])} FROM {x[0]})",
    FunctionType.DATE_DIFF: lambda x, types: f"DATE_DIFF({x[1]}, {x[0]}, {x[2]})",
    FunctionType.MONTH_NAME: lambda x, types: f"FORMAT_DATE('%B', {x[0]})",
    FunctionType.DAY_NAME: lambda x, types: f"FORMAT_DATE('%A', {x[0]})",
    FunctionType.FORMAT_TIME: lambda x, types: f"FORMAT_DATETIME({x[1]}, {x[0]})",
    FunctionType.PARSE_TIME: lambda x, types: f"PARSE_DATETIME({x[1]}, {x[0]})",
    # string
    FunctionType.CONTAINS: lambda x, types: f"CONTAINS_SUBSTR({x[0]}, {x[1]})",
    FunctionType.RANDOM: lambda x, types: f"FLOOR(RAND()*{x[0]})",
    FunctionType.ARRAY_SUM: lambda x, types: f"(select sum(x) from unnest({x[0]}) as x)",
    FunctionType.ARRAY_DISTINCT: lambda x, types: f"ARRAY(SELECT DISTINCT element FROM UNNEST({x[0]}) AS element)",
    FunctionType.ARRAY_SORT: lambda x, types: f"ARRAY(SELECT element FROM UNNEST({x[0]}) AS element ORDER BY element)",
    FunctionType.GEO_FROM_TEXT: lambda x, types: f"ST_GEOGFROMTEXT({x[0]})",
    FunctionType.GEO_POINT: lambda x, types: f"ST_GEOGPOINT({x[0]}, {x[1]})",
    FunctionType.GEO_DISTANCE: lambda x, types: f"ST_DISTANCE({x[0]}, {x[1]})",
    FunctionType.GEO_CENTROID: lambda x, types: f"ST_CENTROID({x[0]})",
    FunctionType.GEO_TRANSFORM: lambda x, types: render_geo_transform(x),
    # aggregate
    FunctionType.BOOL_AND: lambda x, types: f"LOGICAL_AND({x[0]})",
    FunctionType.BOOL_OR: lambda x, types: f"LOGICAL_OR({x[0]})",
    # native CONCAT propagates NULL; wrap to match the null-skipping semantics.
    # ARRAY_TO_STRING omits NULL elements when no null_text is given.
    FunctionType.CONCAT: lambda x, types: (
        "CONCAT(" + ", ".join([f"COALESCE({a}, '')" for a in x]) + ")"
    ),
    FunctionType.CONCAT_WS: lambda x, types: (
        f"ARRAY_TO_STRING([{', '.join(x[1:])}], {x[0]})"
    ),
}

DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "STRING",
    DataType.INTEGER: "INT64",
    DataType.FLOAT: "FLOAT64",
    DataType.DOUBLE: "FLOAT64",
    DataType.BOOL: "BOOL",
    DataType.NUMERIC: "NUMERIC",
    DataType.MAP: "MAP",
    DataType.DATE: "DATE",
    DataType.DATETIME: "DATETIME",
    DataType.TIMESTAMP: "TIMESTAMP",
}


BQ_CREATE_TABLE_SQL_TEMPLATE = Template("""
CREATE {% if create_mode == "create_or_replace" %}OR REPLACE TABLE{% elif create_mode == "create_if_not_exists" %}TABLE IF NOT EXISTS{% else %}TABLE{% endif %} {{ name}} (
{%- for column in columns %}
    `{{ column.name }}` {{ type_map[column.name] }}{% if description_map[column.name] %} OPTIONS(description={{ description_map[column.name] }}){% endif %}{% if not loop.last %},{% endif %}
{%- endfor %}
)
{%- if partition_clause %}
{{ partition_clause }}
{%- endif %};
""".strip())

# BigQuery rejects a bare TIMESTAMP/DATETIME column in PARTITION BY; it must be
# truncated to a partitionable granularity. Integer range partitioning needs
# explicit bounds we have no way to declare, so it is not offered.
PARTITION_EXPRESSIONS: dict[DataType, str] = {
    DataType.DATE: "{column}",
    DataType.DATETIME: "DATETIME_TRUNC({column}, DAY)",
    DataType.TIMESTAMP: "TIMESTAMP_TRUNC({column}, DAY)",
}

MAX_IDENTIFIER_LENGTH = 50

#: Alias for the target of a partitioned append's DELETE. BigQuery cannot
#: correlate through a qualified table name, so the target needs a bare one.
PARTITION_DELETE_ALIAS = "trilogy_delete_target"


def parse_bigquery_table_name(
    table_name: str, schema: str | None = None
) -> tuple[str, str | None]:
    """Parse BigQuery table names supporting project.dataset.table format."""
    if "." in table_name and not schema:
        parts = table_name.split(".")
        if len(parts) == 2:
            schema = parts[0]
            table_name = parts[1]
        elif len(parts) == 3:
            # project.dataset.table format
            schema = f"{parts[0]}.{parts[1]}"
            table_name = parts[2]
    return table_name, schema


class BigqueryDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    QUOTE_CHARACTER = "`"
    NULL_WRAPPER = staticmethod(null_wrapper)
    CREATE_TABLE_SQL_TEMPLATE = BQ_CREATE_TABLE_SQL_TEMPLATE
    UNNEST_MODE = UnnestMode.CROSS_JOIN_UNNEST
    DATATYPE_MAP = DATATYPE_MAP
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_QUALIFY = True
    # python datasources have to be staged to GCS before a query can name them
    REQUIRES_SOURCE_PREPARATION = True
    # `404 Not found: Table proj:ds.tbl was not found in location US`
    TABLE_NOT_FOUND_PATTERN = r"Not found: (Table|Dataset)"
    # unqualified `Unrecognized name: col`; qualified `Name col not found inside base`.
    # A staleness probe names a column the model declares, so a table built before
    # that column existed must read as stale rather than failing the probe.
    COLUMN_NOT_FOUND_PATTERN = r"Unrecognized name|Name .+ not found inside"
    # Anything the DDL can partition on can also key an append, so both read the
    # same map rather than drifting apart over DATETIME.
    SUPPORTED_PARTITION_KEY_TYPES: ClassVar[set[DataType]] = set(PARTITION_EXPRESSIONS)
    # BigQuery requires an explicit DISTINCT on set operators.
    SET_OPERATOR_MAP: ClassVar[dict[str, str]] = {
        **BaseDialect.SET_OPERATOR_MAP,
        "EXCEPT": "EXCEPT DISTINCT",
        "INTERSECT": "INTERSECT DISTINCT",
    }

    _python_staging: BigQueryPythonStaging | None = None

    def python_staging(self) -> BigQueryPythonStaging:
        """Resolve (once) where python datasource output is staged.

        Raises with the missing setting named, rather than emitting SQL against
        a table that could never exist.
        """
        if self._python_staging is not None:
            return self._python_staging
        from trilogy.dialect.config import BigQueryConfig

        config = self.config
        if (
            not isinstance(config, BigQueryConfig)
            or not config.enable_python_datasources
        ):
            raise ValueError(
                "Python script datasources require enable_python_datasources=True in "
                "BigQueryConfig. Set this in your trilogy.conf under [engine.config] "
                "or pass BigQueryConfig(enable_python_datasources=True) to the executor."
            )
        root_uri = config.staging_uri or (self.staging.path if self.staging else None)
        if not root_uri or not root_uri.startswith(("gs://", "gcs://")):
            raise ValueError(
                "Python script datasources on BigQuery require a GCS staging "
                "location. Set staging_uri='gs://bucket/prefix' in BigQueryConfig, "
                "or [staging] path in your trilogy.conf."
            )
        if config.use_sqlalchemy and not config.staging_dataset:
            raise ValueError(
                "use_sqlalchemy=True cannot stage python datasources as per-job temp "
                "tables - SQLAlchemy cannot attach job configuration. Set "
                "staging_dataset in BigQueryConfig to use persistent external tables, "
                "or drop use_sqlalchemy to use the native BigQuery engine."
            )
        if config.staging_dataset:
            logger.info(
                "%s staging_dataset is set, so python datasources use persistent "
                "external tables instead of per-job temp definitions",
                LOGGER_PREFIX,
            )
        self._python_staging = BigQueryPythonStaging(
            root_uri=root_uri,
            dataset=config.staging_dataset,
            project=config.project,
            instance_id=self.instance_id,
        )
        return self._python_staging

    def render_source(
        self, address: Address, request: "SourceRequest | None" = None
    ) -> str:
        if address.type == AddressType.PYTHON_SCRIPT:
            return self.python_staging().table_reference(address)
        return super().render_source(address, request)

    def prepare_sources(
        self, addresses: Iterable[Address], executor: "Executor"
    ) -> None:
        scripts = [a for a in addresses if a.type == AddressType.PYTHON_SCRIPT]
        if not scripts:
            return
        staging = self.python_staging()
        if staging.uses_external_tables:
            for address in scripts:
                staging.materialize(address, executor.execute_raw_sql)
            return
        connection = executor.connection
        if isinstance(connection, BigQueryConnection):
            for address in scripts:
                uri = staging.stage(address)
                if uri is None:
                    continue
                connection.register_external_table(
                    staging.table_name(address), staging.external_config(uri)
                )
            return
        raise ValueError(
            "Per-job temp external tables need the native BigQuery engine, but this "
            f"executor is connected via {type(connection).__name__}. Either drop "
            "use_sqlalchemy=True, or set staging_dataset in BigQueryConfig to stage "
            "through persistent external tables instead."
        )

    def teardown(self) -> None:
        if self._python_staging is None:
            return
        for uri in self._python_staging.cleanup():
            logger.info("%s removed staged object %s", LOGGER_PREFIX, uri)

    def render_string_literal(self, value: str) -> str:
        # BigQuery treats backslash as an escape character in string literals;
        # a verbatim `\.` (e.g. from a regex) is an illegal escape sequence.
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def hash_column_value(self, column_name: str) -> str:
        return f"FARM_FINGERPRINT(CAST({safe_quote(column_name, self.QUOTE_CHARACTER)} AS STRING))"

    def aggregate_checksum(self, hash_expr: str) -> str:
        return f"BIT_XOR({hash_expr})"

    # BQ DATATYPE_MAP uses canonical names (INT64, STRING, FLOAT64, …) that match
    # information_schema exactly; extend base with legacy aliases BQ also accepts.
    DB_COLUMN_TYPE_MAP: ClassVar[dict[str, DataType]] = {
        **BaseDialect.DB_COLUMN_TYPE_MAP,
        "int64": DataType.INTEGER,
        "float64": DataType.FLOAT,
        "bool": DataType.BOOL,
        "datetime": DataType.DATETIME,
        "timestamp": DataType.TIMESTAMP,
        # legacy aliases
        "integer": DataType.INTEGER,
        "int": DataType.INTEGER,
        "float": DataType.FLOAT,
        "boolean": DataType.BOOL,
    }

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        """BigQuery uses dataset instead of schema and supports project.dataset.table format."""
        table_name, schema = parse_bigquery_table_name(table_name, schema)

        column_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            '' as column_comment
        FROM `{schema}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        rows = executor.execute_raw_sql(column_query).fetchall()
        return self._columns_from_info_schema_rows(rows)

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """BigQuery doesn't enforce primary keys; rely on data-driven grain detection."""
        table_name, schema = parse_bigquery_table_name(table_name, schema)

        pk_query = f"""
        SELECT column_name
        FROM `{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
        WHERE table_name = '{table_name}'
        AND constraint_name LIKE '%PRIMARY%'
        """

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[0] for row in rows]

    def render_array_member_source(
        self, array_sql: str, from_clause: str | None, member_type: CONCRETE_TYPES
    ) -> tuple[str, str]:
        """UNNEST is a table operator in BigQuery, not a set-returning scalar, so
        it joins against the source rather than sitting in its select list."""
        source = f"unnest({array_sql}) as {self.ARRAY_MEMBER_COLUMN}"
        if from_clause:
            source = f"{from_clause}, {source}"
        return source, self.ARRAY_MEMBER_COLUMN

    def render_partition_clause(self, target: CreateTableInfo) -> str:
        if not target.partition_keys:
            return ""
        if len(target.partition_keys) > 1:
            raise ValueError(
                f"BigQuery supports a single partition column, but {target.name} "
                f"declares {', '.join(target.partition_keys)}."
            )
        key = target.partition_keys[0]
        dtype = {c.name: c.type for c in target.columns}[key]
        quoted = safe_quote(key, self.QUOTE_CHARACTER)
        expr = PARTITION_EXPRESSIONS.get(dtype) if isinstance(dtype, DataType) else None
        if expr is None:
            raise ValueError(
                f"BigQuery cannot partition {target.name} by {key}: partitioning "
                f"requires a date/time column, but {key} is {dtype}."
            )
        return f"PARTITION BY {expr.format(column=quoted)}"

    def render_partition_delete(
        self, target: str, staged: str, partition_by: list[str]
    ) -> str:
        """The shared correlated delete, with the target aliased.

        BigQuery cannot correlate through a qualified table name: given
        ``project.dataset.table.column`` inside the subquery it tries to resolve
        ``project`` as a name and fails with ``Unrecognized name``. Aliasing the
        delete target is the only spelling that works for a three-part address,
        and it is harmless for a bare one."""
        matches = self.partition_key_match(staged, PARTITION_DELETE_ALIAS, partition_by)
        return (
            f"DELETE FROM {target} AS {PARTITION_DELETE_ALIAS} WHERE EXISTS"
            f" (SELECT 1 FROM {staged} WHERE {matches})"
        )

    def render_staging_create(self, target: str, staged: str) -> str:
        """BigQuery spells the modifier ``TEMP``, and a temp table may not be
        qualified — which the shared staging name already satisfies."""
        return f"CREATE TEMP TABLE {staged} AS SELECT * FROM {target} LIMIT 0"

    def generate_partitioned_insert_statements(
        self,
        query: ProcessedQueryPersist,
        recursive: bool,
        compiled_ctes: list[CompiledCTE],
    ) -> list[str]:
        """The shared staged replace, as a single script rather than five
        statements.

        A BigQuery temp table lives for the length of the multi-statement query
        that declared it, so the staged form only holds together when its steps
        share one job — every other dialect can issue them as separate calls
        against a session-scoped temp table.

        This replaced a scripted ``EXECUTE IMMEDIATE`` loop that ran a DELETE
        and an INSERT per distinct partition value. Beyond costing 2N jobs, that
        loop built its predicate with ``FORMAT('... = "%t"')`` — so it only ever
        matched a DATE-shaped key, read the first partition column alone, and
        could not clear the NULL slice that ``ARRAY_AGG(DISTINCT ...)`` drops.
        The shared form is null-safe and multi-key by construction."""
        return [
            ";\n".join(
                super().generate_partitioned_insert_statements(
                    query, recursive, compiled_ctes
                )
            )
        ]
