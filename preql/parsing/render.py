from functools import singledispatchmethod

from jinja2 import Template

from preql.constants import DEFAULT_NAMESPACE
from preql.core.enums import Purpose, DataType, ConceptSource
from preql.core.models import (
    Address,
    Query,
    Concept,
    ConceptTransform,
    Function,
    Grain,
    OrderItem,
    Select,
    SelectItem,
    WhereClause,
    Conditional,
    Comparison,
    Environment,
    ConceptDeclaration,
    Datasource,
    WindowItem,
    FilterItem,
    ColumnAssignment,
    CaseElse,
    CaseWhen,
    Import,
)


from collections import defaultdict


QUERY_TEMPLATE = Template(
    """SELECT{%- for select in select_columns %}
    {{ select }},{% endfor %}{% if where %}
WHERE
    {{ where }}{% endif %}{%- if order_by %}
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
    def _(self, arg: Environment):
        output_concepts = []
        constants: list[Concept] = []
        keys: list[Concept] = []
        properties = defaultdict(list)
        metrics = []
        # first, keys
        for concept in arg.concepts.values():
            if concept.namespace != DEFAULT_NAMESPACE:
                continue
            if (
                concept.metadata
                and concept.metadata.concept_source == ConceptSource.AUTO_DERIVED
            ):
                continue
            elif not concept.lineage and concept.purpose == Purpose.CONSTANT:
                constants.append(concept)
            elif not concept.lineage and concept.purpose == Purpose.KEY:
                keys.append(concept)

            elif not concept.lineage and concept.purpose == Purpose.PROPERTY:
                if concept.keys:
                    # avoid duplicate declarations
                    # but we need better composite key support
                    for key in concept.keys[:1]:
                        properties[key.name].append(concept)
                else:
                    keys.append(concept)
            else:
                metrics.append(concept)

        output_concepts = constants
        for key in keys:
            output_concepts += [key]
            output_concepts += properties.get(key.name, [])
        output_concepts += metrics

        rendered_concepts = [
            self.to_string(ConceptDeclaration(concept=concept))
            for concept in output_concepts
        ]

        rendered_datasources = [
            # extra padding between datasources
            # todo: make this more generic
            self.to_string(datasource) + "\n"
            for datasource in arg.datasources.values()
            if datasource.namespace == DEFAULT_NAMESPACE
        ]
        rendered_imports = [
            self.to_string(import_statement)
            for import_statement in arg.imports.values()
        ]
        components = []
        if rendered_imports:
            components.append(rendered_imports)
        if rendered_concepts:
            components.append(rendered_concepts)
        if rendered_datasources:
            components.append(rendered_datasources)
        final = "\n\n".join("\n".join(x) for x in components)
        return final

    @to_string.register
    def _(self, arg: Datasource):
        assignments = ",\n\t".join([self.to_string(x) for x in arg.columns])
        return f"""datasource {arg.name} (
    {assignments}
    ) 
{self.to_string(arg.grain)} 
{self.to_string(arg.address)};"""

    @to_string.register
    def _(self, arg: "Grain"):
        components = ",".join(self.to_string(x) for x in arg.components)
        return f"grain ({components})"

    @to_string.register
    def _(self, arg: "Query"):
        return f"""query {arg.text}"""

    @to_string.register
    def _(self, arg: "CaseWhen"):
        return f"""WHEN {arg.comparison} THEN {self.to_string(arg.expr)}"""

    @to_string.register
    def _(self, arg: "CaseElse"):
        return f"""ELSE {self.to_string(arg.expr)}"""

    @to_string.register
    def _(self, arg: DataType):
        return arg.value

    @to_string.register
    def _(self, arg: list):
        base = ", ".join([self.to_string(x) for x in arg])
        return f"[{base}]"

    @to_string.register
    def _(self, arg: "Address"):
        return f"address {arg.location}"

    @to_string.register
    def _(self, arg: "ColumnAssignment"):
        return f"{arg.alias}:{self.to_string(arg.concept)}"

    @to_string.register
    def _(self, arg: "ConceptDeclaration"):
        concept = arg.concept
        if concept.metadata and concept.metadata.description:
            base_description = concept.metadata.description
        else:
            base_description = None
        if not concept.lineage:
            if concept.purpose == Purpose.PROPERTY and concept.keys:
                output = f"{concept.purpose.value} {concept.keys[0].name}.{concept.name} {concept.datatype.value};"
            else:
                output = (
                    f"{concept.purpose.value} {concept.name} {concept.datatype.value};"
                )
        else:
            output = f"{concept.purpose.value} {concept.name} <- {self.to_string(concept.lineage)};"
        if base_description:
            output += f" # {base_description}"
        return output

    @to_string.register
    def _(self, arg: Select):
        return QUERY_TEMPLATE.render(
            select_columns=[self.to_string(c) for c in arg.selection],
            where=self.to_string(arg.where_clause) if arg.where_clause else None,
            order_by=[self.to_string(c) for c in arg.order_by.items]
            if arg.order_by
            else None,
            limit=arg.limit,
        )

    @to_string.register
    def _(self, arg: "WhereClause"):
        return f"{self.to_string(arg.conditional)}"

    @to_string.register
    def _(self, arg: "Conditional"):
        return f"({self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)})"

    @to_string.register
    def _(self, arg: "Comparison"):
        return f"{self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)}"

    @to_string.register
    def _(self, arg: "SelectItem"):
        return self.to_string(arg.content)

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} OVER {over}"
            )
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):
        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} OVER {over}"
            )
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):
        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "Import"):
        return f"import {arg.path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} OVER {over}"
            )
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):
        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "Import"):
        return f"import {arg.path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} OVER {over}"
            )
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):
        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "Import"):
        return f"import {arg.path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} OVER {over}"
            )
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):
        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "Import"):
        return f"import {arg.path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} OVER {over}"
            )
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):
        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "Import"):
        return f"import {arg.path} as {arg.alias};"

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
        return f"{arg.operator.value}({inputs})"

    @to_string.register
    def _(self, arg: "OrderItem"):
        return f"{self.to_string(arg.expr)} {arg.order.value}"

    @to_string.register
    def _(self, arg: int):
        return f"{arg}"

    @to_string.register
    def _(self, arg: str):
        return f"'{arg}'"

    @to_string.register
    def _(self, arg: float):
        return f"{arg}"

    @to_string.register
    def _(self, arg: bool):
        return f"{arg}"


def render_query(query: "Select") -> str:
    return Renderer().to_string(query)


def render_environment(environment: "Environment") -> str:
    return Renderer().to_string(environment)
