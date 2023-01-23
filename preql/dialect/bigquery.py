from jinja2 import Template

from preql.core.enums import FunctionType, WindowType
from preql.dialect.base import BaseDialect

WINDOW_FUNCTION_MAP = {
    WindowType.ROW_NUMBER: lambda window, sort, order: f"row_number() over ( order by {sort} {order})"
}

FUNCTION_MAP = {
    FunctionType.COUNT: lambda x: f"count({x[0]})",
    FunctionType.SUM: lambda x: f"sum({x[0]})",
    FunctionType.LENGTH: lambda x: f"length({x[0]})",
    FunctionType.AVG: lambda x: f"avg({x[0]})",
    FunctionType.LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 1 ELSE 0 END",
    FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
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
FROM
    {{ base }}{% if joins %}
{% for join in joins %}
{{ join }}
{% endfor %}{% endif %}
{% if where %}WHERE
    {{ where }}
{% endif %}
{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if limit %}
LIMIT {{ limit }}{% endif %}
"""
)


class BigqueryDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = WINDOW_FUNCTION_MAP
    FUNCTION_MAP = FUNCTION_MAP
    FUNCTION_GRAIN_MATCH_MAP = FUNCTION_GRAIN_MATCH_MAP
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = BQ_SQL_TEMPLATE
