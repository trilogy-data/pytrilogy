import re
from typing import Any, Callable, Mapping

from jinja2 import Template

from trilogy.core.enums import FunctionType, UnnestMode, WindowType
from trilogy.core.models.core import DataType
from trilogy.dialect.base import BaseDialect

WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

SENTINAL_AUTO_CAPTURE_GROUP_VALUE = "-1"


def generate_regex_extract(x: list[str]) -> str:
    if str(x[2]) == SENTINAL_AUTO_CAPTURE_GROUP_VALUE:
        regex = re.compile(x[1])
        if regex.groups == 0:
            search = 0
        else:
            search = 1
        return f"REGEXP_EXTRACT({x[0]},{x[1]},{search})"
    return f"REGEXP_EXTRACT({x[0]},{x[1]},{x[2]})"


FUNCTION_MAP = {
    FunctionType.COUNT: lambda args: f"count({args[0]})",
    FunctionType.SUM: lambda args: f"sum({args[0]})",
    FunctionType.AVG: lambda args: f"avg({args[0]})",
    FunctionType.LENGTH: lambda args: f"length({args[0]})",
    FunctionType.LIKE: lambda args: (
        f" CASE WHEN {args[0]} like {args[1]} THEN True ELSE False END"
    ),
    FunctionType.CONCAT: lambda args: (
        f"CONCAT({','.join([f''' {str(a)} ''' for a in args])})"
    ),
    FunctionType.SPLIT: lambda args: (
        f"STRING_SPLIT({','.join([f''' {str(a)} ''' for a in args])})"
    ),
    ## Duckdb indexes from 1, not 0
    FunctionType.INDEX_ACCESS: lambda args: (f"{args[0]}[{args[1]}]"),
    # datetime is aliased
    FunctionType.CURRENT_DATETIME: lambda x: "cast(get_current_timestamp() as datetime)",
    FunctionType.DATE: lambda x: f"cast({x[0]} as date)",
    FunctionType.DATE_TRUNCATE: lambda x: f"date_trunc('{x[1]}', {x[0]})",
    FunctionType.DATE_ADD: lambda x: f"date_add({x[0]}, {x[2]} * INTERVAL 1 {x[1]})",
    FunctionType.DATE_PART: lambda x: f"date_part('{x[1]}', {x[0]})",
    FunctionType.DATE_DIFF: lambda x: f"date_diff('{x[2]}', {x[0]}, {x[1]})",
    FunctionType.CONCAT: lambda x: f"({' || '.join(x)})",
    FunctionType.DATE_LITERAL: lambda x: f"date '{x}'",
    FunctionType.DATETIME_LITERAL: lambda x: f"datetime '{x}'",
    # string
    FunctionType.CONTAINS: lambda x: f"CONTAINS(LOWER({x[0]}), LOWER({x[1]}))",
    # regexp
    FunctionType.REGEXP_CONTAINS: lambda x: f"REGEXP_MATCHES({x[0]},{x[1]})",
    FunctionType.REGEXP_EXTRACT: lambda x: generate_regex_extract(x),
}

# if an aggregate function is called on a source that is at the same grain as the aggregate
# we may return a static value
FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args: f"CASE WHEN{args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
    FunctionType.MAX: lambda args: f"{args[0]}",
    FunctionType.MIN: lambda args: f"{args[0]}",
}

DATATYPE_MAP: dict[DataType, str] = {}


DUCKDB_TEMPLATE = Template(
    """{%- if output %}
CREATE OR REPLACE TABLE {{ output.address.location }} AS
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
