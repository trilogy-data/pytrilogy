from typing import Mapping, Callable, Any

from jinja2 import Template
from trilogy.utility import string_to_hash


from trilogy.core.enums import FunctionType, WindowType
from trilogy.core.models import (
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedShowStatement,
    ProcessedRawSQLStatement,
)
from trilogy.dialect.base import BaseDialect

WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

FUNCTION_MAP = {
    FunctionType.COUNT: lambda args: f"count({args[0]})",
    FunctionType.SUM: lambda args: f"sum({args[0]})",
    FunctionType.AVG: lambda args: f"avg({args[0]})",
    FunctionType.LENGTH: lambda args: f"length({args[0]})",
    FunctionType.LIKE: lambda args: (
        f" CASE WHEN {args[0]} like {args[1]} THEN True ELSE False END"
    ),
    FunctionType.CONCAT: lambda args: (
        f"CONCAT({','.join([f''' '{a}' ''' for a in args])})"
    ),
}

# if an aggregate function is called on a source that is at the same grain as the aggregate
# we may return a static value
FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT: lambda args: "1",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
}

TSQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}{{full_select}}
{%- else -%}{%- if comment %}
-- {{ comment }}{% endif %}
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

    def compile_statement(
        self,
        query: (
            ProcessedQuery
            | ProcessedQueryPersist
            | ProcessedShowStatement
            | ProcessedRawSQLStatement
        ),
    ) -> str:
        base = super().compile_statement(query)
        if isinstance(base, (ProcessedQuery, ProcessedQueryPersist)):
            for cte in query.ctes:
                if len(cte.name) > MAX_IDENTIFIER_LENGTH:
                    new_name = f"rhash_{string_to_hash(cte.name)}"
                    base = base.replace(cte.name, new_name)
        return base
