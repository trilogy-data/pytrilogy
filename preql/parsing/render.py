from functools import singledispatchmethod

from jinja2 import Template

from preql.constants import DEFAULT_NAMESPACE
from preql.core.models import (
    Concept,
    ConceptTransform,
    Function,
    OrderItem,
    Select,
    SelectItem,
)

QUERY_TEMPLATE = Template(
    """SELECT{%- for select in select_columns %}
    {{ select }},{% endfor %}{% if where %}WHERE
    {{ where }}
{% endif %}{%- if order_by %}
ORDER BY{% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}{%- if limit is not none %}
LIMIT {{ limit }}{% endif %};"""
)


class Renderer:
    @singledispatchmethod
    def to_string(self, arg):
        raise NotImplementedError("Cannot render type {}".format(type(arg)))

    @to_string.register
    def _(self, arg: Select):
        return QUERY_TEMPLATE.render(
            select_columns=[self.to_string(c) for c in arg.selection],
            order_by=[self.to_string(c) for c in arg.order_by.items]
            if arg.order_by
            else None,
            limit=arg.limit,
        )

    @to_string.register
    def _(self, arg: "SelectItem"):
        return self.to_string(arg.content)

    @to_string.register
    def _(self, arg: "Concept"):
        if arg.namespace == DEFAULT_NAMESPACE:
            return arg.name
        return arg.address

    @to_string.register
    def _(self, arg: "ConceptTransform"):
        return f"{self.to_string(arg.function)}->{arg.output.name}"

    @to_string.register
    def _(self, arg: "Function"):
        inputs = ",".join(self.to_string(c) for c in arg.arguments)
        return f"{arg.operator}({inputs})"

    @to_string.register
    def _(self, arg: "OrderItem"):
        return f"{self.to_string(arg.expr)} {arg.order.value}"


def render_query(query: "Select") -> str:
    return Renderer().to_string(query)
