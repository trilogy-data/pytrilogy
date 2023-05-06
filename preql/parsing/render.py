from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from preql.core.models import Select, SelectItem, Concept, ConceptTransform
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


def render_select_item():
    pass


def render_select(item: Union[SelectItem, Concept, ConceptTransform]):
    pass


def render_query(query: "Select") -> str:
    return QUERY_TEMPLATE.render(
        select_columns=[c.concept.address for c in query.selection],
        order_by=[f"{c.expr} {c.order}" for c in query.order_by],
    )
