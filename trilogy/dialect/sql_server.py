from typing import Any, Callable, Mapping

from jinja2 import Template

from trilogy.core.enums import FunctionType, WindowType
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedQuery,
    ProcessedQueryPersist,
)
from trilogy.dialect.base import BaseDialect
from trilogy.utility import string_to_hash

WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

FUNCTION_MAP = {
    FunctionType.COUNT: lambda args, types: f"count({args[0]})",
    FunctionType.SUM: lambda args, types: f"sum({args[0]})",
    FunctionType.AVG: lambda args, types: f"avg({args[0]})",
    FunctionType.LENGTH: lambda args, types: f"length({args[0]})",
    FunctionType.LIKE: lambda args, types: (
        f" CASE WHEN {args[0]} like {args[1]} THEN True ELSE False END"
    ),
    FunctionType.CONCAT: lambda args, types: (
        f"CONCAT({','.join([f''' '{a}' ''' for a in args])})"
    ),
}

# if an aggregate function is called on a source that is at the same grain as the aggregate
# we may return a static value
FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args, types: f"CASE WHEN{args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args, types: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args, types: f"{args[0]}",
    FunctionType.AVG: lambda args, types: f"{args[0]}",
}

TSQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}{{full_select}}
{%- else -%}{%- if comment %}
-- {{ comment }}{%- endif -%}
SELECT
{%- if limit is not none %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
    {{ base }}{% endif %}{% if joins %}
{% for join in joins %}
{{ join }}
{% endfor %}{% endif %}
{% if where %}WHERE
    {{ where }}
{% endif %}
{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}{% if having %}
HAVING
\t{{ having }}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}{% endif %}
"""
)

MAX_IDENTIFIER_LENGTH = 128


class SqlServerDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = {**BaseDialect.WINDOW_FUNCTION_MAP, **WINDOW_FUNCTION_MAP}
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    QUOTE_CHARACTER = '"'
    SQL_TEMPLATE = TSQL_TEMPLATE

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[tuple]:
        """Defaults to 'dbo' schema if none specified."""
        if not schema:
            schema = "dbo"

        column_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            '' as column_comment
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        AND table_schema = '{schema}'
        ORDER BY ordinal_position
        """

        rows = executor.execute_raw_sql(column_query).fetchall()
        return rows

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Uses sys catalog views for more reliable constraint information."""
        if not schema:
            schema = "dbo"

        pk_query = f"""
        SELECT c.name
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.is_primary_key = 1
        AND t.name = '{table_name}'
        AND s.name = '{schema}'
        ORDER BY ic.key_ordinal
        """

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[0] for row in rows]

    def compile_statement(self, query: PROCESSED_STATEMENT_TYPES) -> str:
        base = super().compile_statement(query)
        if isinstance(query, (ProcessedQuery, ProcessedQueryPersist)):
            for cte in query.ctes:
                if len(cte.name) > MAX_IDENTIFIER_LENGTH:
                    new_name = f"rhash_{string_to_hash(cte.name)}"
                    base = base.replace(cte.name, new_name)
        return base
