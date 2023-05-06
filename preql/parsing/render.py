from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from preql.core.models import Select
from jinja2 import Template

QUERY_TEMPLATE = Template(
    """
SELECT
{%- if limit is not none %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if where %}WHERE
    {{ where }}
{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
"""
)


def render_query(query: "Select") -> str:
    return QUERY_TEMPLATE.render(
        select_columns=[c.concept.address for c in query.selection],
        order_by=[f"{c.expr} {c.order}" for c in query.order_by],
    )
