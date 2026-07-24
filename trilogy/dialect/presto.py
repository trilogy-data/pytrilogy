from collections.abc import Callable
from typing import ClassVar

from jinja2 import Template

from trilogy.core.enums import FunctionType, GroupMode, UnnestMode
from trilogy.core.models.core import DataType
from trilogy.dialect.base import AGGREGATE_GRAIN_MATCH_MAP, BaseDialect

FUNCTION_MAP = {
    FunctionType.COUNT: lambda x, types: f"count({x[0]})",
    FunctionType.SUM: lambda x, types: f"sum({x[0]})",
    FunctionType.LENGTH: lambda x, types: f"length({x[0]})",
    FunctionType.AVG: lambda x, types: f"avg({x[0]})",
    FunctionType.INDEX_ACCESS: lambda x, types: f"element_at({x[0]},{x[1]})",
    FunctionType.MAP_ACCESS: lambda x, types: f"{x[0]}[{x[1]}]",
    FunctionType.MINUTE: lambda x, types: f"EXTRACT(MINUTE from {x[0]})",
    FunctionType.SECOND: lambda x, types: f"EXTRACT(SECOND from {x[0]})",
    FunctionType.HOUR: lambda x, types: f"EXTRACT(HOUR from {x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"EXTRACT(DAYOFWEEK from {x[0]})",
    FunctionType.DAY: lambda x, types: f"EXTRACT(DAY from {x[0]})",
    FunctionType.YEAR: lambda x, types: f"EXTRACT(YEAR from {x[0]})",
    FunctionType.MONTH: lambda x, types: f"EXTRACT(MONTH from {x[0]})",
    FunctionType.WEEK: lambda x, types: f"EXTRACT(WEEK from {x[0]})",
    FunctionType.QUARTER: lambda x, types: f"EXTRACT(QUARTER from {x[0]})",
    # math
    FunctionType.DIVIDE: lambda x, types: f"{x[0]}/{x[1]}",
    FunctionType.DATE_ADD: lambda x, types: f"date_add({x[1]},{x[2]}, {x[0]})",
    FunctionType.DATE_SUB: lambda x, types: f"date_add({x[1]},-{x[2]}, {x[0]})",
    FunctionType.CURRENT_DATE: lambda x, types: "CURRENT_DATE",
    FunctionType.CURRENT_DATETIME: lambda x, types: "CURRENT_TIMESTAMP",
    FunctionType.ARRAY: lambda x, types: f"ARRAY[{', '.join(x)}]",
    # regex
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"REGEXP_LIKE({x[0]}, {x[1]})",
    # native concat propagates NULL; wrap to match the null-skipping semantics.
    # array_join omits NULL elements when no null_replacement is given.
    FunctionType.CONCAT: lambda x, types: (
        "concat(" + ", ".join([f"coalesce({a}, '')" for a in x]) + ")"
    ),
    FunctionType.CONCAT_WS: lambda x, types: (
        f"array_join(ARRAY[{', '.join(x[1:])}], {x[0]})"
    ),
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    **AGGREGATE_GRAIN_MATCH_MAP,
}

SQL_TEMPLATE = Template("""{%- if output %}
{{output}}
{% endif %}{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif -%}
{%- if full_select -%}
{{full_select}}
{%- else %}SELECT
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
{%- if group_by %}GROUP BY{% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
\t{{ having }}{% endif %}{% if qualify %}
QUALIFY
\t{{ qualify }}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT {{ limit }}{% endif %}{% endif %}
""")
MAX_IDENTIFIER_LENGTH = 50


class PrestoDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    FUNCTION_GRAIN_MATCH_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    QUOTE_CHARACTER = '"'
    SQL_TEMPLATE = SQL_TEMPLATE
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        DataType.NUMERIC: "DECIMAL",
        DataType.STRING: "VARCHAR",
    }
    UNNEST_MODE = UnnestMode.PRESTO
    GROUP_MODE = GroupMode.BY_INDEX
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_QUALIFY = True
    ALIAS_ORDER_REFERENCING_ALLOWED = (
        False  # some complex presto functions don't support aliasing
    )

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Presto/Trino don't enforce PKs; rely on data-driven grain detection."""
        return []


class TrinoDialect(PrestoDialect):
    pass
