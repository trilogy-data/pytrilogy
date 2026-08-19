from collections.abc import Callable
from typing import Any, ClassVar

from jinja2 import Template

from trilogy.core.enums import (
    ComparisonOperator,
    CreateMode,
    DatePart,
    FunctionType,
    JoinType,
    Modifier,
    Ordering,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.statements.execute import CreateTableInfo
from trilogy.dialect.base import AGGREGATE_GRAIN_MATCH_MAP, BaseDialect, TableColumn


def date_truncate(expr: str, part: str) -> str:
    grain = DatePart(part)
    formats = {
        DatePart.YEAR: "%Y-01-01 00:00:00",
        DatePart.MONTH: "%Y-%m-01 00:00:00",
        DatePart.DAY: "%Y-%m-%d 00:00:00",
        DatePart.HOUR: "%Y-%m-%d %H:00:00",
        DatePart.MINUTE: "%Y-%m-%d %H:%i:00",
        DatePart.SECOND: "%Y-%m-%d %H:%i:%s",
    }
    if grain in formats:
        return f"TIMESTAMP(DATE_FORMAT({expr}, '{formats[grain]}'))"
    if grain == DatePart.WEEK:
        return f"TIMESTAMP(DATE_SUB(DATE({expr}), INTERVAL WEEKDAY({expr}) DAY))"
    if grain == DatePart.QUARTER:
        return (
            f"TIMESTAMP(DATE_ADD(MAKEDATE(YEAR({expr}), 1), "
            f"INTERVAL (QUARTER({expr}) - 1) QUARTER))"
        )
    raise NotImplementedError(f"Date truncation not implemented for grain {grain}")


def date_interval(function: str, args: list[str]) -> str:
    grain = DatePart(args[1]).value.upper()
    return f"{function}({args[0]}, INTERVAL {args[2]} {grain})"


def date_diff(first: str, second: str, part: str) -> str:
    grain = DatePart(part).value.upper()
    return f"TIMESTAMPDIFF({grain}, {first}, {second})"


def concat_ignore_nulls(args: list[str]) -> str:
    values = ", ".join(f"COALESCE({arg}, '')" for arg in args)
    return f"CONCAT({values})"


def render_cast(args: list[str]) -> str:
    # TEXT is a valid column type but not a valid MySQL CAST target. Trilogy's
    # string datatype renders as TEXT for DDL, so translate it at the function
    # boundary where MySQL requires CHAR instead.
    target = "CHAR" if args[1].upper() == "TEXT" else args[1]
    return f"CAST({args[0]} AS {target})"


def render_ordering(rendered: str, order: Ordering) -> str:
    # MySQL has no NULLS FIRST/LAST; NULLs always sort first ascending and last
    # descending. The other two placements are emulated with a leading
    # `<expr> IS NULL` term, which sorts in the same direction either way
    # (asc puts the null group last, desc puts it first).
    direction = "desc" if order.value.startswith("desc") else "asc"
    if order in (Ordering.ASC_NULLS_LAST, Ordering.DESC_NULLS_FIRST):
        return f"({rendered}) IS NULL {direction}, {rendered} {direction}"
    return f"{rendered} {direction}"


def null_safe_join_key(
    lval: str, rval: str, modifiers: list[Modifier], jointype: JoinType | None = None
) -> str:
    # `<=>` is MySQL's null-safe equality. The base dialect's OR-expansion is
    # equivalent but never index-assisted, and every lowered FULL join emits one
    # of these per key.
    if Modifier.NULLABLE in modifiers:
        return f"({lval} <=> {rval})"
    return f"{lval} = {rval}"


FUNCTION_MAP = {
    FunctionType.IS_NOT_DISTINCT: lambda x, types: f"({x[0]} <=> {x[1]})",
    FunctionType.POWER: lambda x, types: f"POWER({x[0]}, {x[1]})",
    FunctionType.RANDOM: lambda x, types: "RAND()",
    FunctionType.BOOL_OR: lambda x, types: f"MAX(CAST({x[0]} AS UNSIGNED))",
    FunctionType.BOOL_AND: lambda x, types: f"MIN(CAST({x[0]} AS UNSIGNED))",
    FunctionType.STRPOS: lambda x, types: f"LOCATE({x[1]}, {x[0]})",
    FunctionType.CONTAINS: lambda x, types: f"(LOCATE({x[1]}, {x[0]}) > 0)",
    FunctionType.CONCAT: lambda x, types: concat_ignore_nulls(x),
    FunctionType.CONCAT_STRICT: lambda x, types: f"CONCAT({', '.join(x)})",
    FunctionType.CAST: lambda x, types: render_cast(x),
    FunctionType.DATE_LITERAL: lambda x, types: f"DATE('{x}')",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"TIMESTAMP('{x}')",
    FunctionType.DATE_TRUNCATE: lambda x, types: date_truncate(x[0], x[1]),
    FunctionType.DATE_ADD: lambda x, types: date_interval("DATE_ADD", x),
    FunctionType.DATE_SUB: lambda x, types: date_interval("DATE_SUB", x),
    FunctionType.DATE_PART: lambda x, types: (
        f"DAYOFWEEK({x[0]})"
        if DatePart(x[1]) == DatePart.DAY_OF_WEEK
        else f"EXTRACT({DatePart(x[1]).value.upper()} FROM {x[0]})"
    ),
    FunctionType.DATE_DIFF: lambda x, types: date_diff(x[0], x[1], x[2]),
    FunctionType.DAY_OF_WEEK: lambda x, types: f"DAYOFWEEK({x[0]})",
    FunctionType.FORMAT_TIME: lambda x, types: f"DATE_FORMAT({x[0]}, {x[1]})",
    FunctionType.PARSE_TIME: lambda x, types: f"STR_TO_DATE({x[0]}, {x[1]})",
    FunctionType.CURRENT_DATETIME: lambda x, types: "CURRENT_TIMESTAMP()",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    **AGGREGATE_GRAIN_MATCH_MAP,
}

DATATYPE_MAP = {
    DataType.STRING: "TEXT",
    DataType.BYTES: "BLOB",
    DataType.INTEGER: "INTEGER",
    DataType.BIGINT: "BIGINT",
    DataType.FLOAT: "FLOAT",
    DataType.DOUBLE: "DOUBLE",
    DataType.BOOL: "BOOLEAN",
    DataType.NUMERIC: "DECIMAL",
    DataType.DATE: "DATE",
    DataType.DATETIME: "DATETIME",
    DataType.TIMESTAMP: "TIMESTAMP",
}

MYSQL_SQL_TEMPLATE = Template("""{%- if output %}
{{output}}
{% endif %}{%- if ctes %}
WITH {% if recursive %}RECURSIVE {% endif %}{% for cte in ctes %}
{{cte.name}} AS (
{{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{%- else -%}
SELECT
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
LIMIT {{ limit }}{% endif %}{% endif %}
""")


class MySQLDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    FUNCTION_GRAIN_MATCH_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        **DATATYPE_MAP,
    }
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = MYSQL_SQL_TEMPLATE
    SUPPORTS_ALIAS_IN_HAVING = True
    SUPPORTS_FULL_JOIN = False
    SUPPORTS_ARRAYS = False
    NULL_WRAPPER = staticmethod(null_safe_join_key)
    TABLE_NOT_FOUND_PATTERN = "doesn't exist"
    COLUMN_NOT_FOUND_PATTERN = "unknown column"

    def partition_key_match(
        self, left: str, right: str, partition_by: list[str]
    ) -> str:
        """MySQL spells null-safe equality ``<=>``, so the base's explicit
        ``IS NULL`` arms collapse to one operator."""
        return " AND ".join(
            f"{left}.{self.quote(key)} <=> {right}.{self.quote(key)}"
            for key in partition_by
        )

    def render_partition_delete(
        self, target: str, staged: str, partition_by: list[str]
    ) -> str:
        """MySQL rejects the base's correlated ``EXISTS`` ("You can't specify
        target table for delete"), so the key match rides a multi-table DELETE
        join instead. The table name doubles as its own alias."""
        matches = self.partition_key_match(target, staged, partition_by)
        return f"DELETE {target} FROM {target} JOIN {staged} ON {matches}"

    def render_ordering(self, rendered: str, order: Ordering) -> str:
        return render_ordering(rendered, order)

    def render_string_literal(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def compile_create_table_statements(
        self, target: CreateTableInfo, create_mode: CreateMode
    ) -> list[str]:
        statements = super().compile_create_table_statements(target, create_mode)
        if create_mode == CreateMode.CREATE_OR_REPLACE:
            return [
                f"DROP TABLE IF EXISTS {self.safe_quote(target.name)};",
                *[
                    s.replace("CREATE OR REPLACE TABLE", "CREATE TABLE", 1)
                    for s in statements
                ],
            ]
        return statements

    def render_comparison(
        self,
        left,
        right,
        operator: ComparisonOperator,
        cte: CTE | UnionCTE | None = None,
        cte_map: dict[str, CTE | UnionCTE] | None = None,
        raise_invalid: bool = False,
        materialized_addresses: set[str] | None = None,
    ) -> str:
        if operator in (ComparisonOperator.ILIKE, ComparisonOperator.NOT_ILIKE):
            left_sql = self.render_expr(
                left,
                cte=cte,
                cte_map=cte_map,
                raise_invalid=raise_invalid,
                materialized_addresses=materialized_addresses,
            )
            right_sql = self.render_expr(
                right,
                cte=cte,
                cte_map=cte_map,
                raise_invalid=raise_invalid,
                materialized_addresses=materialized_addresses,
            )
            negate = "NOT " if operator == ComparisonOperator.NOT_ILIKE else ""
            return f"({negate}LOWER({left_sql}) LIKE LOWER({right_sql}))"
        return super().render_comparison(
            left,
            right,
            operator,
            cte=cte,
            cte_map=cte_map,
            raise_invalid=raise_invalid,
            materialized_addresses=materialized_addresses,
        )

    def get_table_schema(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        schema_filter = (
            f"table_schema = '{schema}'" if schema else "table_schema = DATABASE()"
        )
        query = f"""
        SELECT column_name, data_type, is_nullable, column_comment
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND {schema_filter}
        ORDER BY ordinal_position
        """
        return self._columns_from_info_schema_rows(
            executor.execute_raw_sql(query).fetchall()
        )

    def get_table_primary_keys(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[str]:
        schema_filter = (
            f"table_schema = '{schema}'" if schema else "table_schema = DATABASE()"
        )
        query = f"""
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_name = '{table_name}'
          AND constraint_name = 'PRIMARY'
          AND {schema_filter}
        ORDER BY ordinal_position
        """
        return [row[0] for row in executor.execute_raw_sql(query).fetchall()]

    def list_tables(
        self, executor: Any, schema: str | None = None
    ) -> list[tuple[str, str]]:
        schema_filter = (
            f"table_schema = '{schema}'" if schema else "table_schema = DATABASE()"
        )
        query = f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE {schema_filter}
        ORDER BY table_name
        """
        return executor.execute_raw_sql(query).fetchall()
