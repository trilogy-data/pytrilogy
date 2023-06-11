from typing import Mapping, Callable, Any

from jinja2 import Template

from preql.core.enums import FunctionType, WindowType
from preql.dialect.base import BaseDialect


WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

FUNCTION_MAP = {
    FunctionType.COUNT: lambda x: f"count({x[0]})",
    FunctionType.SUM: lambda x: f"sum({x[0]})",
    FunctionType.LENGTH: lambda x: f"length({x[0]})",
    FunctionType.AVG: lambda x: f"avg({x[0]})",
    FunctionType.LIKE: lambda x: (
        f" CASE WHEN {x[0]} like {x[1]} THEN True ELSE False END"
    ),
    FunctionType.MINUTE: lambda x: f"EXTRACT(MINUTE from {x[0]})",
    FunctionType.SECOND: lambda x: f"EXTRACT(SECOND from {x[0]})",
    FunctionType.HOUR: lambda x: f"EXTRACT(HOUR from {x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x: f"EXTRACT(DAYOFWEEK from {x[0]})",
    FunctionType.DAY: lambda x: f"EXTRACT(DAY from {x[0]})",
    FunctionType.YEAR: lambda x: f"EXTRACT(YEAR from {x[0]})",
    FunctionType.MONTH: lambda x: f"EXTRACT(MONTH from {x[0]})",
    FunctionType.WEEK: lambda x: f"EXTRACT(WEEK from {x[0]})",
    FunctionType.QUARTER: lambda x: f"EXTRACT(QUARTER from {x[0]})",
    # math
    FunctionType.DIVIDE: lambda x: f"SAFE_DIVIDE({x[0]},{x[1]})",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT: lambda args: f"{args[0]}",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
}

BQ_SQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
SELECT

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
{%- if group_by %}GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT {{ limit }}{% endif %}
"""
)
MAX_IDENTIFIER_LENGTH = 50


class BigqueryDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = {**BaseDialect.WINDOW_FUNCTION_MAP, **WINDOW_FUNCTION_MAP}
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = BQ_SQL_TEMPLATE
