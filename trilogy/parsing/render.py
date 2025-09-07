from collections import defaultdict
from datetime import date, datetime
from functools import singledispatchmethod
from typing import Any

from jinja2 import Template

from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX, MagicConstants
from trilogy.core.enums import ConceptSource, DatePart, FunctionType, Modifier, Purpose
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    CaseElse,
    CaseWhen,
    Comment,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    OrderBy,
    OrderItem,
    Parenthetical,
    SubselectComparison,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    ListWrapper,
    MapWrapper,
    NumericType,
    TraitDataType,
    TupleWrapper,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    Query,
    RawColumnExpr,
)
from trilogy.core.models.environment import Environment, Import
from trilogy.core.statements.author import (
    ArgBinding,
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ConceptTransform,
    CopyStatement,
    FunctionDeclaration,
    ImportStatement,
    KeyMergeStatement,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
    TypeDeclaration,
)

QUERY_TEMPLATE = Template(
    """{% if where %}WHERE
    {{ where }}
{% endif %}SELECT{%- for select in select_columns %}
    {{ select }},{% endfor %}{% if having %}
HAVING
    {{ having }}
{% endif %}{%- if order_by %}
ORDER BY{% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{%- if limit is not none %}
LIMIT {{ limit }}{% endif %}
;"""
)


class Renderer:

    def __init__(self, environment: Environment | None = None):
        self.environment = environment

    def render_statement_string(self, list_of_statements: list[Any]) -> str:
        new = []
        last_statement_type = None
        for stmt in list_of_statements:
            stmt_type = type(stmt)
            if last_statement_type is None:
                pass
            elif last_statement_type == Comment:
                new.append("\n")
            elif stmt_type != last_statement_type:
                new.append("\n\n")
            else:
                new.append("\n")
            new.append(Renderer().to_string(stmt))
            last_statement_type = stmt_type
        return "".join(new)

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
            if "__preql_internal" in concept.address:
                continue

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
                    for key in sorted(list(concept.keys))[:1]:
                        properties[key].append(concept)
                else:
                    keys.append(concept)
            else:
                metrics.append(concept)

        output_concepts = constants
        for key_concept in keys:
            output_concepts += [key_concept]
            output_concepts += properties.get(key_concept.name, [])
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
        rendered_imports = []
        for _, imports in arg.imports.items():
            for import_statement in imports:
                rendered_imports.append(self.to_string(import_statement))
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
    def _(self, arg: TypeDeclaration):
        return f"type {arg.type.name} {self.to_string(arg.type.type)};"

    @to_string.register
    def _(self, arg: ArgBinding):
        if arg.default:
            return f"{arg.name}={self.to_string(arg.default)}"
        return f"{arg.name}"

    @to_string.register
    def _(self, arg: FunctionDeclaration):
        args = ", ".join([self.to_string(x) for x in arg.args])
        return f"def {arg.name}({args}) -> {self.to_string(arg.expr)};"

    @to_string.register
    def _(self, arg: Datasource):
        assignments = ",\n    ".join([self.to_string(x) for x in arg.columns])
        if arg.non_partial_for:
            non_partial = f"\ncomplete where {self.to_string(arg.non_partial_for)}"
        else:
            non_partial = ""
        base = f"""datasource {arg.name} (
    {assignments}
    )
{self.to_string(arg.grain) if arg.grain.components else ''}{non_partial}
{self.to_string(arg.address)}"""

        if arg.where:
            base += f"\nwhere {self.to_string(arg.where)}"

        base += ";"
        return base

    @to_string.register
    def _(self, arg: "Grain"):
        final = []
        for comp in arg.components:
            if comp.startswith(DEFAULT_NAMESPACE):
                final.append(comp.split(".", 1)[1])
            else:
                final.append(comp)
        final = sorted(final)
        components = ",".join(x for x in final)
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
    def _(self, arg: "FunctionCallWrapper"):
        args = [self.to_string(c) for c in arg.args]
        arg_string = ", ".join(args)
        return f"""@{arg.name}({arg_string})"""

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
    def _(self, arg: TraitDataType):
        traits = "::".join([x for x in arg.traits])
        return f"{self.to_string(arg.data_type)}::{traits}"

    @to_string.register
    def _(self, arg: ListWrapper):
        return "[" + ", ".join([self.to_string(x) for x in arg]) + "]"

    @to_string.register
    def _(self, arg: TupleWrapper):
        return "(" + ", ".join([self.to_string(x) for x in arg]) + ")"

    @to_string.register
    def _(self, arg: MapWrapper):
        def process_key_value(key, value):
            return f"{self.to_string(key)}: {self.to_string(value)}"

        return (
            "{"
            + ", ".join([process_key_value(key, value) for key, value in arg.items()])
            + "}"
        )

    @to_string.register
    def _(self, arg: DatePart):
        return arg.value

    @to_string.register
    def _(self, arg: "Address"):
        if arg.is_query:
            if arg.location.startswith("("):
                return f"query '''{arg.location[1:-1]}'''"
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
        if arg.modifiers:
            modifiers = "".join(
                [self.to_string(modifier) for modifier in sorted(arg.modifiers)]
            )
        else:
            modifiers = ""
        if isinstance(arg.alias, str):
            return f"{arg.alias}: {modifiers}{self.to_string(arg.concept)}"
        return f"{self.to_string(arg.alias)}: {modifiers}{self.to_string(arg.concept)}"

    @to_string.register
    def _(self, arg: "RawColumnExpr"):
        return f"raw('''{arg.text}''')"

    @to_string.register
    def _(self, arg: "ConceptDeclarationStatement"):
        concept = arg.concept
        if concept.metadata and concept.metadata.description:
            base_description = concept.metadata.description
        else:
            base_description = None
        if concept.namespace and concept.namespace != DEFAULT_NAMESPACE:
            namespace = f"{concept.namespace}."
        else:
            namespace = ""
        if not concept.lineage:
            if concept.purpose == Purpose.PROPERTY and concept.keys:
                if len(concept.keys) == 1:
                    output = f"{concept.purpose.value} {self.to_string(ConceptRef(address=list(concept.keys)[0]))}.{namespace}{concept.name} {self.to_string(concept.datatype)};"
                else:
                    keys = ",".join(
                        sorted(
                            list(
                                self.to_string(ConceptRef(address=x))
                                for x in concept.keys
                            )
                        )
                    )
                    output = f"{concept.purpose.value} <{keys}>.{namespace}{concept.name} {self.to_string(concept.datatype)};"
            else:
                output = f"{concept.purpose.value} {namespace}{concept.name} {self.to_string(concept.datatype)};"
        else:
            output = f"{concept.purpose.value} {namespace}{concept.name} <- {self.to_string(concept.lineage)};"
        if base_description:
            output += f" #{base_description}"
        return output

    @to_string.register
    def _(self, arg: ArrayType):
        return f"list<{self.to_string(arg.value_data_type)}>"

    @to_string.register
    def _(self, arg: DataType):
        return arg.value

    @to_string.register
    def _(self, arg: date):
        return f"'{arg.isoformat()}'::date"

    @to_string.register
    def _(self, arg: datetime):
        return f"'{arg.isoformat()}'::datetime"

    @to_string.register
    def _(self, arg: ConceptDerivationStatement):
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
            having=self.to_string(arg.having_clause) if arg.having_clause else None,
            order_by=(
                [self.to_string(c) for c in arg.order_by.items]
                if arg.order_by
                else None
            ),
            limit=arg.limit,
        )

    @to_string.register
    def _(self, arg: MultiSelectStatement):
        base = "\nMERGE\n".join([self.to_string(select)[:-2] for select in arg.selects])
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
    def _(self, arg: CopyStatement):
        return f"COPY INTO {arg.target_type.value.upper()} '{arg.target}' FROM {self.to_string(arg.select)}"

    @to_string.register
    def _(self, arg: AlignClause):
        return "\nALIGN\n\t" + ",\n\t".join([self.to_string(c) for c in arg.items])

    @to_string.register
    def _(self, arg: AlignItem):
        return f"{arg.alias}:{','.join([self.to_string(c) for c in arg.concepts])}"

    @to_string.register
    def _(self, arg: OrderBy):
        return ",\n".join([self.to_string(c) for c in arg.items])

    @to_string.register
    def _(self, arg: "WhereClause"):
        base = f"{self.to_string(arg.conditional)}"
        if base[0] == "(" and base[-1] == ")":
            return base[1:-1]
        return base

    @to_string.register
    def _(self, arg: "Conditional"):
        return f"({self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)})"

    @to_string.register
    def _(self, arg: "SubselectComparison"):
        return f"{self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)}"

    @to_string.register
    def _(self, arg: "Comparison"):
        return f"{self.to_string(arg.left)} {arg.operator.value} {self.to_string(arg.right)}"

    @to_string.register
    def _(self, arg: "Comment"):
        return f"{arg.text}"

    @to_string.register
    def _(self, arg: "WindowItem"):
        over = ",".join(self.to_string(c) for c in arg.over)
        order = ",".join(self.to_string(c) for c in arg.order_by)
        if over and order:
            return (
                f"{arg.type.value} {self.to_string(arg.content)} by {order} over {over}"
            )
        elif over:
            return f"{arg.type.value} {self.to_string(arg.content)} over {over}"
        return f"{arg.type.value} {self.to_string(arg.content)} by {order}"

    @to_string.register
    def _(self, arg: "FilterItem"):

        return f"filter {self.to_string(arg.content)} where {self.to_string(arg.where)}"

    @to_string.register
    def _(self, arg: "ConceptRef"):
        if arg.name.startswith(VIRTUAL_CONCEPT_PREFIX) and self.environment:
            return self.to_string(self.environment.concepts[arg.address])
        ns, base = arg.address.rsplit(".", 1)
        if ns == DEFAULT_NAMESPACE:
            return base
        return arg.address

    @to_string.register
    def _(self, arg: "ImportStatement"):
        path: str = str(arg.path).replace("\\", ".")
        path = path.replace("/", ".")
        if path.endswith(".preql"):
            path = path.rsplit(".", 1)[0]
        if path.startswith("."):
            path = path[1:]
        if arg.alias == DEFAULT_NAMESPACE or not arg.alias:
            return f"import {path};"
        return f"import {path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "Import"):
        path: str = str(arg.path).replace("\\", ".")
        path = path.replace("/", ".")
        if path.endswith(".preql"):
            path = path.rsplit(".", 1)[0]
        if path.startswith("."):
            path = path[1:]
        if arg.alias == DEFAULT_NAMESPACE or not arg.alias:
            return f"import {path};"
        return f"import {path} as {arg.alias};"

    @to_string.register
    def _(self, arg: "Concept"):
        if arg.name.startswith(VIRTUAL_CONCEPT_PREFIX):
            return self.to_string(arg.lineage)
        if arg.namespace == DEFAULT_NAMESPACE:
            return arg.name
        return arg.address

    @to_string.register
    def _(self, arg: "ConceptTransform"):
        return f"{self.to_string(arg.function)} -> {arg.output.name}"

    @to_string.register
    def _(self, arg: "Function"):
        args = [self.to_string(c) for c in arg.arguments]

        if arg.operator == FunctionType.SUBTRACT:
            return " - ".join(args)
        if arg.operator == FunctionType.ADD:
            return " + ".join(args)
        if arg.operator == FunctionType.MULTIPLY:
            return " * ".join(args)
        if arg.operator == FunctionType.DIVIDE:
            return " / ".join(args)
        if arg.operator == FunctionType.MOD:
            return f"{args[0]} % {args[1]}"
        if arg.operator == FunctionType.PARENTHETICAL:
            return f"({args[0]})"
        if arg.operator == FunctionType.GROUP:
            arg_string = ", ".join(args[1:])
            if len(args) == 1:
                return f"group({args[0]})"
            return f"group({args[0]}) by {arg_string}"
        inputs = ",".join(args)

        if arg.operator == FunctionType.CONSTANT:
            return f"{inputs}"
        if arg.operator == FunctionType.CAST:
            return f"CAST({self.to_string(arg.arguments[0])} AS {self.to_string(arg.arguments[1])})"
        if arg.operator == FunctionType.INDEX_ACCESS:
            return f"{self.to_string(arg.arguments[0])}[{self.to_string(arg.arguments[1])}]"

        if arg.operator == FunctionType.CASE:
            inputs = "\n\t".join(args)
            return f"CASE\n\t{inputs}\nEND"
        return f"{arg.operator.value}({inputs})"

    @to_string.register
    def _(self, arg: "OrderItem"):
        return f"{self.to_string(arg.expr)} {arg.order.value}"

    @to_string.register
    def _(self, arg: AggregateWrapper):
        if arg.by:
            by = ", ".join([self.to_string(x) for x in arg.by])
            return f"{self.to_string(arg.function)} by {by}"
        return f"{self.to_string(arg.function)}"

    @to_string.register
    def _(self, arg: MergeStatementV2):
        if len(arg.sources) == 1:
            return f"MERGE {self.to_string(arg.sources[0])} into {''.join([self.to_string(modifier) for modifier in arg.modifiers])}{self.to_string(arg.targets[arg.sources[0].address])};"
        return f"MERGE {arg.source_wildcard}.* into {''.join([self.to_string(modifier) for modifier in arg.modifiers])}{arg.target_wildcard}.*;"

    @to_string.register
    def _(self, arg: KeyMergeStatement):
        keys = ", ".join(sorted(list(arg.keys)))
        return f"MERGE PROPERTY <{keys}> from {arg.target.address};"

    @to_string.register
    def _(self, arg: Modifier):
        if arg == Modifier.PARTIAL:
            return "~"
        elif arg == Modifier.HIDDEN:
            return "--"
        elif arg == Modifier.NULLABLE:
            return "?"
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
