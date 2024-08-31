from typing import Mapping, Callable, Any

from jinja2 import Template

from trilogy.core.enums import FunctionType, WindowType, DatePart
from trilogy.dialect.base import BaseDialect


def date_diff(first: str, second: str, grain: DatePart) -> str:
    grain = DatePart(grain)
    if grain == DatePart.YEAR:
        return f"date_part('year', {second}) - date_part('year', {first})"
    elif grain == DatePart.MONTH:
        return f"12 * {date_diff(first, second, DatePart.YEAR)} + date_part('month', {second}) - date_part('month', {first})"
    elif grain == DatePart.DAY:
        return f"date_part('day', {second} - {first})"
    elif grain == DatePart.HOUR:
        return f"{date_diff(first, second, DatePart.DAY)} *24 + date_part('hour', {second} - {first})"
    elif grain == DatePart.MINUTE:
        return f"{date_diff(first, second, DatePart.HOUR)} *60 + date_part('minute', {second} - {first})"
    elif grain == DatePart.SECOND:
        return f"{date_diff(first, second, DatePart.MINUTE)} *60 + date_part('second', {second} - {first})"
    else:
        raise NotImplementedError(f"Date diff not implemented for grain {grain}")


WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

FUNCTION_MAP = {
    FunctionType.SPLIT: lambda x: f"string_to_array({x[0]}, {x[1]})",
    FunctionType.DATE_TRUNCATE: lambda x: f"date_trunc('{x[1]}', {x[0]})",
    FunctionType.DATE_ADD: lambda x: f"({x[0]} + INTERVAL '{x[2]} {x[1]}')",
    FunctionType.DATE_PART: lambda x: f"date_part('{x[1]}', {x[0]})",
    FunctionType.DATE_DIFF: lambda x: date_diff(x[0], x[1], x[2]),
    FunctionType.IS_NULL: lambda x: f"{x[0]} IS NULL",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT: lambda args: f"{args[0]}",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
}

PG_SQL_TEMPLATE = Template(
    """{%- if output %}
DROP TABLE IF EXISTS {{ output.address.location }};
CREATE TABLE {{ output.address.location }} AS
{% endif %}{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{%- else -%}
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
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
\t{{ having }}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT {{ limit }}{% endif %}{% endif %}
"""
)

MAX_IDENTIFIER_LENGTH = 50


class PostgresDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = {**BaseDialect.WINDOW_FUNCTION_MAP, **WINDOW_FUNCTION_MAP}
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    QUOTE_CHARACTER = '"'
    SQL_TEMPLATE = PG_SQL_TEMPLATE
