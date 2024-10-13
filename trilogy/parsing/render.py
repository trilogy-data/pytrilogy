from functools import singledispatchmethod

from jinja2 import Template

from trilogy.constants import DEFAULT_NAMESPACE, MagicConstants, VIRTUAL_CONCEPT_PREFIX
from trilogy.core.enums import Purpose, ConceptSource, DatePart, FunctionType
from trilogy.core.models import (
    DataType,
    Address,
    Query,
    Concept,
    ConceptTransform,
    Function,
    Grain,
    OrderItem,
    SelectStatement,
    SelectItem,
    WhereClause,
    Conditional,
    Comparison,
    Environment,
    ConceptDeclarationStatement,
    ConceptDerivation,
    Datasource,
    WindowItem,
    FilterItem,
    ColumnAssignment,
    CaseElse,
    CaseWhen,
    ImportStatement,
    Parenthetical,
    AggregateWrapper,
    PersistStatement,
    ListWrapper,
    RowsetDerivationStatement,
    MultiSelectStatement,
    OrderBy,
    AlignClause,
    AlignItem,
    RawSQLStatement,
    NumericType,
    MergeStatementV2,
)
from trilogy.core.enums import Modifier

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
            # don't render anything that came from an import
            if concept.namespace in arg.imports:
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
            self.to_string(ConceptDeclarationStatement(concept=concept))
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
        base = f"""datasource {arg.name} (
    {assignments}
    ) 
{self.to_string(arg.grain)} 
{self.to_string(arg.address)}"""
        if arg.where:
            base += f"\n{self.to_string(arg.where)}"
        base += ";"
        return base

    @to_string.register
    def _(self, arg: "Grain"):
        components = ",".join(self.to_string(x) for x in arg.components)
        return f"grain ({components})"

    @to_string.register
    def _(self, arg: "Query"):
        return f"""query {arg.text}"""

    @to_string.register
    def _(self, arg: RowsetDerivationStatement):
        return f"""rowset {arg.name} <- {self.to_string(arg.select)}"""

    @to_string.register
    def _(self, arg: "CaseWhen"):
        return (
            f"""WHEN {self.to_string(arg.comparison)} THEN {self.to_string(arg.expr)}"""
        )

    @to_string.register
    def _(self, arg: "CaseElse"):
        return f"""ELSE {self.to_string(arg.expr)}"""

    @to_string.register
    def _(self, arg: "Parenthetical"):
        return f"""({self.to_string(arg.content)})"""

    @to_string.register
    def _(self, arg: DataType):
        return arg.value

    @to_string.register
    def _(self, arg: "NumericType"):
        return f"""Numeric({arg.precision},{arg.scale})"""

    @to_string.register
    def _(self, arg: ListWrapper):
        return "[" + ", ".join([self.to_string(x) for x in arg]) + "]"

    @to_string.register
    def _(self, arg: DatePart):
        return arg.value

    @to_string.register
    def _(self, arg: "Address"):
        if arg.is_query:
            return f"query '''{arg.location}'''"
        return f"address {arg.location}"

    @to_string.register
    def _(self, arg: "RawSQLStatement"):
        return f"raw_sql('''{arg.text}''');"

    @to_string.register
    def _(self, arg: "MagicConstants"):
        if arg == MagicConstants.NULL:
            return "null"
        return arg.value

    @to_string.register
    def _(self, arg: "ColumnAssignment"):
        return f"{arg.alias}: {self.to_string(arg.concept)}"

    @to_string.register
    def _(self, arg: "ConceptDeclarationStatement"):
        concept = arg.concept
        if concept.metadata and concept.metadata.description:
            base_description = concept.metadata.description
        else:
            base_description = None
        if concept.namespace:
            namespace = f"{concept.namespace}."
        else:
            namespace = ""
        if not concept.lineage:
            if concept.purpose == Purpose.PROPERTY and concept.keys:
                output = f"{concept.purpose.value} {namespace}{concept.keys[0].name}.{concept.name} {concept.datatype.value};"
            else:
                output = f"{concept.purpose.value} {namespace}{concept.name} {concept.datatype.value};"
        else:
            output = f"{concept.purpose.value} {namespace}{concept.name} <- {self.to_string(concept.lineage)};"
        if base_description:
            output += f" # {base_description}"
        return output

    @to_string.register
    def _(self, arg: ConceptDerivation):
        # this is identical rendering;
        return self.to_string(ConceptDeclarationStatement(concept=arg.concept))

    @to_string.register
    def _(self, arg: PersistStatement):
        return f"PERSIST {arg.identifier} INTO {arg.address.location} FROM {self.to_string(arg.select)}"

    @to_string.register
    def _(self, arg: SelectItem):
        prefixes = []
        if Modifier.HIDDEN in arg.modifiers:
            prefixes.append("--")
        if Modifier.PARTIAL in arg.modifiers:
            prefixes.append("~")
        final = "".join(prefixes)
        return f"{final}{self.to_string(arg.content)}"

    @to_string.register
    def _(self, arg: SelectStatement):
        return QUERY_TEMPLATE.render(
            select_columns=[self.to_string(c) for c in arg.selection],
            where=self.to_string(arg.where_clause) if arg.where_clause else None,
            order_by=(
                [self.to_string(c) for c in arg.order_by.items]
                if arg.order_by
                else None
            ),
            limit=arg.limit,
        )

    @to_string.register
    def _(self, arg: MultiSelectStatement):
        base = "\nMERGE\n".join([self.to_string(select)[:-1] for select in arg.selects])
        base += self.to_string(arg.align)
        if arg.where_clause:
            base += f"\nWHERE\n{self.to_string(arg.where_clause)}"
        if arg.order_by:
            base += f"\nORDER BY\n\t{self.to_string(arg.order_by)}"
        if arg.limit:
            base += f"\nLIMIT {arg.limit}"
        base += "\n;"
        return base

    @to_string.register
    def _(self, arg: AlignClause):
        return "\nALIGN\n\t" + ",\n\t".join([self.to_string(c) for c in arg.items])

    @to_string.register
    def _(self, arg: AlignItem):
        return f"{arg.alias}:{','.join([self.to_string(c) for c in arg.concepts])}"

    @to_string.register
    def _(self, arg: OrderBy):
        return ",\t".join([self.to_string(c) for c in arg.items])

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
    def _(self, arg: "ImportStatement"):
        return f"import {arg.path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "Concept"):
        if arg.name.startswith(VIRTUAL_CONCEPT_PREFIX):
            return self.to_string(arg.lineage)
        if arg.namespace == DEFAULT_NAMESPACE:
            return arg.name
        return arg.address

    @to_string.register
    def _(self, arg: "ConceptTransform"):
        return f"{self.to_string(arg.function)}->{arg.output.name}"

    @to_string.register
    def _(self, arg: "Function"):
        inputs = ",".join(self.to_string(c) for c in arg.arguments)
        if arg.operator == FunctionType.CONSTANT:
            return f"{inputs}"
        if arg.operator == FunctionType.INDEX_ACCESS:
            return f"{self.to_string(arg.arguments[0])}[{self.to_string(arg.arguments[1])}]"
        return f"{arg.operator.value}({inputs})"

    @to_string.register
    def _(self, arg: "OrderItem"):
        return f"{self.to_string(arg.expr)} {arg.order.value}"

    @to_string.register
    def _(self, arg: AggregateWrapper):
        if arg.by:
            return f"{self.to_string(arg.function)} by {self.to_string(arg.by)}"
        return f"{self.to_string(arg.function)}"

    @to_string.register
    def _(self, arg: MergeStatementV2):
        return f"MERGE {self.to_string(arg.source)} into {''.join([self.to_string(modifier) for modifier in arg.modifiers])}{self.to_string(arg.target)};"

    @to_string.register
    def _(self, arg: Modifier):
        if arg == Modifier.PARTIAL:
            return "~"
        if arg == Modifier.HIDDEN:
            return "--"
        return arg.value

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

    @to_string.register
    def _(self, arg: list):
        base = ", ".join([self.to_string(x) for x in arg])
        return f"[{base}]"


def render_query(query: "SelectStatement") -> str:
    return Renderer().to_string(query)


def render_environment(environment: "Environment") -> str:
    return Renderer().to_string(environment)
