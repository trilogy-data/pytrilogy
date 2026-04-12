from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, cast

from trilogy.constants import DEFAULT_NAMESPACE, NULL_VALUE, Parsing
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    Derivation,
    FunctionType,
    Granularity,
    Modifier,
    Purpose,
    ShowCategory,
)
from trilogy.core.exceptions import InvalidSyntaxException, MissingParameterException
from trilogy.core.functions import FunctionFactory
from trilogy.core.internal import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    AggregateWrapper,
    Comment,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    Metadata,
    Parenthetical,
    SubselectComparison,
    SubselectItem,
    WindowItem,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    EnumType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TraitDataType,
    TupleWrapper,
    arg_to_datatype,
    dict_to_map_wrapper,
    is_compatible_datatype,
    list_to_wrapper,
    tuple_to_wrapper,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ShowStatement,
)
from trilogy.parsing.common import arbitrary_to_concept, constant_to_concept
from trilogy.parsing.exceptions import ParseError
from trilogy.parsing.v2.syntax import (
    SyntaxDocument,
    SyntaxElement,
    SyntaxMeta,
    SyntaxToken,
)

CONSTANT_TYPES = (int, float, str, bool, ListWrapper, TupleWrapper, MapWrapper)
MAX_PARSE_DEPTH = 10
TRANSPARENT_NODES = {
    "comparison_root",
    "sum_chain",
    "product_chain",
    "atom",
}


class ParsePass(Enum):
    INITIAL = 1
    VALIDATION = 2


class UnsupportedSyntaxError(NotImplementedError):
    pass


@dataclass
class HydrationContext:
    environment: Environment
    parse_address: str = "root"
    token_address: Path | str = "root"
    parse_config: Parsing | None = None
    max_parse_depth: int = MAX_PARSE_DEPTH


def metadata_from_meta(
    meta: SyntaxMeta | None,
    description: str | None = None,
    concept_source: ConceptSource = ConceptSource.MANUAL,
) -> Metadata:
    return Metadata(
        description=description,
        line_number=meta.line if meta else None,
        column=meta.column if meta else None,
        end_line=meta.end_line if meta else None,
        end_column=meta.end_column if meta else None,
        concept_source=concept_source,
    )


def parse_concept_reference(
    name: str,
    environment: Environment,
    purpose: Purpose | None = None,
) -> tuple[str, str, str, str | None]:
    parent = None
    if "." in name:
        if purpose == Purpose.PROPERTY:
            parent, name = name.rsplit(".", 1)
            namespace = environment.concepts[parent].namespace or DEFAULT_NAMESPACE
            lookup = f"{namespace}.{name}"
        else:
            namespace, name = name.rsplit(".", 1)
            lookup = f"{namespace}.{name}"
    else:
        namespace = environment.namespace or DEFAULT_NAMESPACE
        lookup = name
    return lookup, namespace, name, parent


def rehydrate_lineage(
    lineage: Any,
    environment: Environment,
    function_factory: FunctionFactory,
) -> Any:
    if isinstance(lineage, Function):
        rehydrated = [
            rehydrate_lineage(x, environment, function_factory)
            for x in lineage.arguments
        ]
        return function_factory.create_function(
            rehydrated,
            operator=lineage.operator,
        )
    if isinstance(lineage, Parenthetical):
        lineage.content = rehydrate_lineage(
            lineage.content, environment, function_factory
        )
        return lineage
    if isinstance(lineage, WindowItem):
        assert isinstance(lineage.content, ConceptRef)
        lineage.content.datatype = environment.concepts[
            lineage.content.address
        ].datatype
        return lineage
    if isinstance(lineage, AggregateWrapper):
        lineage.function = rehydrate_lineage(
            lineage.function, environment, function_factory
        )
        return lineage
    return lineage


def rehydrate_concept_lineage(
    concept: Concept,
    environment: Environment,
    function_factory: FunctionFactory,
) -> Concept:
    concept.lineage = rehydrate_lineage(concept.lineage, environment, function_factory)
    if hasattr(concept.lineage, "output_datatype"):
        concept.datatype = concept.lineage.output_datatype
    return concept


def core_meta(meta: SyntaxMeta | None) -> Any:
    return cast(Any, meta)


class NativeHydrator:
    def __init__(self, context: HydrationContext) -> None:
        self.environment = context.environment
        self.parse_address = context.parse_address
        self.token_address = context.token_address
        self.parse_config = context.parse_config
        self.max_parse_depth = context.max_parse_depth
        self.text_lookup: dict[Path | str, str] = {}
        self.function_factory = FunctionFactory(self.environment)
        self.parse_pass = ParsePass.INITIAL

    def set_text(self, text: str) -> None:
        self.text_lookup[self.token_address] = text

    def prepare_parse(self) -> None:
        self.parse_pass = ParsePass.INITIAL
        self.environment.concepts.fail_on_missing = False

    def parse(self, document: SyntaxDocument) -> list[Any]:
        self.set_text(document.text)
        self.prepare_parse()
        self._run_pass(document.forms, ParsePass.INITIAL)
        output = self._run_pass(document.forms, ParsePass.VALIDATION)
        self.environment.concepts.undefined = {}
        self._rehydrate_unknown_lineages()
        self.environment.concepts.fail_on_missing = True
        return [item for item in output if item]

    def _run_pass(
        self,
        forms: list[SyntaxElement],
        parse_pass: ParsePass,
    ) -> list[Any]:
        self.parse_pass = parse_pass
        return [self.transform(form) for form in forms]

    def _rehydrate_unknown_lineages(self) -> None:
        passed = False
        passes = 0
        while not passed:
            new_passed = True
            for key, concept in self.environment.concepts.items():
                if concept.datatype == DataType.UNKNOWN and concept.lineage:
                    self.environment.concepts[key] = rehydrate_concept_lineage(
                        concept,
                        self.environment,
                        self.function_factory,
                    )
                    new_passed = False
            passes += 1
            if passes > self.max_parse_depth:
                break
            passed = new_passed

    def transform(self, element: SyntaxElement) -> Any:
        if isinstance(element, SyntaxToken):
            return self.transform_token(element)
        transformed = [self.transform(child) for child in element.children]
        handler = getattr(self, element.name, None)
        if handler:
            return handler(element.meta, transformed)
        if len(transformed) == 1 and element.name in TRANSPARENT_NODES:
            return transformed[0]
        raise UnsupportedSyntaxError(f"No v2 hydrator for syntax node '{element.name}'")

    def transform_token(self, token: SyntaxToken) -> Any:
        handler = getattr(self, token.name, None)
        if handler:
            return handler(token)
        return token.value

    def start(self, meta: SyntaxMeta | None, args: list[Any]) -> list[Any]:
        return args

    def block(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        output = args[0]
        if isinstance(output, ConceptDeclarationStatement):
            comments = [x for x in args[1:] if isinstance(x, Comment)]
            if comments:
                output.concept.metadata.description = "\n".join(
                    comment.text.split("#")[1].rstrip() for comment in comments
                )
        return output

    def concept(
        self, meta: SyntaxMeta | None, args: list[Any]
    ) -> ConceptDeclarationStatement:
        if isinstance(args[0], list):
            concept = args[0][0]
        elif isinstance(args[0], Concept):
            concept = args[0]
        else:
            concept = args[0].concept
        if concept.metadata:
            concept.metadata.line_number = meta.line if meta else None
            concept.metadata.column = meta.column if meta else None
            concept.metadata.end_line = meta.end_line if meta else None
            concept.metadata.end_column = meta.end_column if meta else None
        return ConceptDeclarationStatement(concept=concept)

    def parameter_default(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        return args[0]

    def parameter_declaration(
        self,
        meta: SyntaxMeta | None,
        args: list[Any],
    ) -> Any:
        metadata = Metadata()
        default = None
        name = args[0]
        datatype = args[1]
        for arg in args[2:]:
            if isinstance(arg, Metadata):
                metadata = arg
            elif not isinstance(arg, Modifier):
                default = arg
        _, _, name, _ = parse_concept_reference(name, self.environment)
        raw = self.environment.parameters.get(name, default)
        if raw is None:
            raise MissingParameterException(
                f'This script requires parameter "{name}" to be set in environment.'
            )
        if datatype == DataType.INTEGER:
            value: Any = int(raw)
        elif datatype == DataType.FLOAT:
            value = float(raw)
        elif datatype == DataType.BOOL:
            value = bool(raw)
        elif datatype == DataType.STRING:
            value = str(raw)
        elif datatype == DataType.DATE:
            value = raw if isinstance(raw, date) else date.fromisoformat(raw)
        elif datatype == DataType.DATETIME:
            value = raw if isinstance(raw, datetime) else datetime.fromisoformat(raw)
        else:
            raise ParseError(f"Unsupported datatype {datatype} for parameter {name}.")
        return self.constant_derivation(
            meta,
            [Purpose.CONSTANT, name, value, metadata],
        )

    def concept_declaration(
        self,
        meta: SyntaxMeta | None,
        args: list[Any],
    ) -> ConceptDeclarationStatement:
        metadata = Metadata()
        modifiers = []
        purpose = args[0]
        name = args[1]
        datatype = args[2]
        for arg in args:
            if isinstance(arg, Metadata):
                metadata = arg
            if isinstance(arg, Modifier):
                modifiers.append(arg)
        _, namespace, name, _ = parse_concept_reference(name, self.environment)
        concept = Concept(
            name=name,
            datatype=datatype,
            purpose=purpose,
            metadata=metadata,
            namespace=namespace,
            modifiers=modifiers,
            derivation=Derivation.ROOT,
            granularity=Granularity.MULTI_ROW,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line if meta else None
            concept.metadata.column = meta.column if meta else None
            concept.metadata.end_line = meta.end_line if meta else None
            concept.metadata.end_column = meta.end_column if meta else None
        self.environment.add_concept(concept, meta=core_meta(meta))
        return ConceptDeclarationStatement(concept=concept)

    def concept_property_declaration(
        self,
        meta: SyntaxMeta | None,
        args: list[Any],
    ) -> Concept:
        unique = False
        if args[0] != Purpose.PROPERTY:
            unique = True
            args = args[1:]
        metadata = Metadata()
        modifiers = []
        for arg in args:
            if isinstance(arg, Metadata):
                metadata = arg
            if isinstance(arg, Modifier):
                modifiers.append(arg)
        declaration = args[1]
        if not isinstance(declaration, tuple):
            if "." not in declaration:
                raise ParseError(
                    f"Property declaration {declaration} must be fully qualified with a parent key"
                )
            grain, name = declaration.rsplit(".", 1)
            parent = self.environment.concepts[grain]
            parents = [parent]
            namespace = parent.namespace
        else:
            parents, name = declaration
            namespace = self.environment.namespace or DEFAULT_NAMESPACE
        grain_components = {x.address for x in parents}
        all_rows_addr = f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"
        is_abstract_grain = grain_components == {all_rows_addr}
        concept = Concept(
            name=name,
            datatype=args[2],
            purpose=Purpose.PROPERTY if not unique else Purpose.UNIQUE_PROPERTY,
            metadata=metadata,
            grain=Grain(components=grain_components),
            namespace=namespace,
            keys=grain_components,
            modifiers=modifiers,
            granularity=(
                Granularity.SINGLE_ROW if is_abstract_grain else Granularity.MULTI_ROW
            ),
        )
        self.environment.add_concept(concept, core_meta(meta))
        return concept

    def concept_derivation(
        self,
        meta: SyntaxMeta | None,
        args: list[Any],
    ) -> ConceptDerivationStatement:
        metadata = args[3] if len(args) > 3 else None
        purpose = args[0]
        raw_name = args[1]
        if isinstance(raw_name, str):
            _, namespace, name, parent_concept = parse_concept_reference(
                raw_name,
                self.environment,
                purpose,
            )
            keys = (
                [self.environment.concepts[parent_concept].address]
                if parent_concept
                else []
            )
        else:
            keys, name = raw_name
            keys = [x.address for x in keys]
            namespaces = {x.rsplit(".", 1)[0] for x in keys}
            namespace = (
                self.environment.namespace or DEFAULT_NAMESPACE
                if len(namespaces) != 1
                else namespaces.pop()
            )
        source_value = args[2]
        while isinstance(source_value, Parenthetical):
            source_value = source_value.content
        if isinstance(
            source_value,
            (
                FilterItem,
                WindowItem,
                AggregateWrapper,
                Function,
                FunctionCallWrapper,
                Comparison,
                SubselectItem,
            ),
        ):
            concept = arbitrary_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                environment=self.environment,
                metadata=metadata,
            )
            if purpose == Purpose.KEY and concept.purpose != Purpose.KEY:
                concept.purpose = Purpose.KEY
            elif (
                purpose
                and purpose != Purpose.AUTO
                and concept.purpose != purpose
                and purpose != Purpose.CONSTANT
            ):
                raise SyntaxError(
                    f'Concept {name} purpose {concept.purpose} does not match declared purpose {purpose}. Suggest defaulting to "auto"'
                )
            if purpose == Purpose.PROPERTY and keys:
                concept.keys = set(keys)
        elif isinstance(source_value, CONSTANT_TYPES):
            concept = constant_to_concept(
                source_value,
                name=name,
                namespace=namespace,
                metadata=metadata,
            )
        elif isinstance(source_value, ConceptRef):
            concept = arbitrary_to_concept(
                self.function_factory.create_function(
                    [source_value],
                    FunctionType.ALIAS,
                    meta=core_meta(meta),
                ),
                name=name,
                namespace=namespace,
                environment=self.environment,
                metadata=metadata,
            )
        else:
            snippet = ""
            if meta and meta.start_pos is not None and meta.end_pos is not None:
                snippet = self.text_lookup[self.token_address][
                    meta.start_pos : meta.end_pos
                ]
            raise SyntaxError(
                f"Received invalid type {type(source_value)} {source_value} as input to concept derivation: `{snippet}`"
            )
        if concept.metadata:
            concept.metadata.line_number = meta.line if meta else None
            concept.metadata.column = meta.column if meta else None
            concept.metadata.end_line = meta.end_line if meta else None
            concept.metadata.end_column = meta.end_column if meta else None
        self.environment.add_concept(concept, meta=core_meta(meta))
        return ConceptDerivationStatement(concept=concept)

    def constant_derivation(self, meta: SyntaxMeta | None, args: list[Any]) -> Concept:
        metadata = args[3] if len(args) > 3 else None
        name = args[1]
        constant = args[2]
        _, namespace, name, _ = parse_concept_reference(name, self.environment)
        concept = Concept(
            name=name,
            datatype=arg_to_datatype(constant),
            purpose=Purpose.CONSTANT,
            metadata=metadata_from_meta(meta) if not metadata else metadata,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                output_datatype=arg_to_datatype(constant),
                output_purpose=Purpose.CONSTANT,
                arguments=[constant],
            ),
            grain=Grain(components=set()),
            namespace=namespace,
            granularity=Granularity.SINGLE_ROW,
        )
        if concept.metadata:
            concept.metadata.line_number = meta.line if meta else None
            concept.metadata.column = meta.column if meta else None
            concept.metadata.end_line = meta.end_line if meta else None
            concept.metadata.end_column = meta.end_column if meta else None
        self.environment.add_concept(concept, core_meta(meta))
        return concept

    def show_category(self, meta: SyntaxMeta | None, args: list[Any]) -> ShowCategory:
        return ShowCategory(args[0])

    def show_statement(self, meta: SyntaxMeta | None, args: list[Any]) -> ShowStatement:
        return ShowStatement(content=args[0])

    def concept_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> ConceptRef:
        address = args[0]
        if "." not in address and self.environment.namespace == DEFAULT_NAMESPACE:
            address = f"{DEFAULT_NAMESPACE}.{address}"
        mapping = self.environment.concepts[address]
        return ConceptRef(
            address=mapping.address,
            metadata=metadata_from_meta(meta),
            datatype=mapping.output_datatype,
        )

    def data_type(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        resolved = args[0]
        traits = args[1:]
        base: Any
        if isinstance(
            resolved,
            (StructType, ArrayType, NumericType, MapType, EnumType),
        ):
            base = resolved
        else:
            base = DataType(str(resolved).lower())
        if traits:
            for trait in traits:
                if trait not in self.environment.data_types:
                    raise TypeError(f"Invalid type (trait) {trait} for {base}")
                matched = self.environment.data_types[trait]
                if not is_compatible_datatype(matched.type, base):
                    raise TypeError(
                        f"Invalid type (trait) {trait} for {base}. Trait expects type {matched.type}, has {base}"
                    )
            return TraitDataType(type=base, traits=traits)
        return base

    def concept_nullable_modifier(
        self,
        meta: SyntaxMeta | None,
        args: list[Any],
    ) -> Modifier:
        return Modifier.NULLABLE

    def metadata(self, meta: SyntaxMeta | None, args: list[Any]) -> Metadata:
        pairs = {key: val for key, val in zip(args[::2], args[1::2])}
        return Metadata(**pairs)

    def int_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> int:
        return int("".join(str(arg) for arg in args))

    def float_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> float:
        return float(args[0])

    def bool_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> bool:
        return str(args[0]).capitalize() == "True"

    def null_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        return NULL_VALUE

    def string_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> str:
        return args[0] if args else ""

    def array_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> ListWrapper:
        return list_to_wrapper(args)

    def tuple_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> TupleWrapper:
        return tuple_to_wrapper(args)

    def map_lit(self, meta: SyntaxMeta | None, args: list[Any]) -> MapWrapper:
        return dict_to_map_wrapper(dict(zip(args[::2], args[1::2])))

    def literal(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        return args[0]

    def product_operator(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i])
            right = args[i + 1]
            operator = {
                "*": FunctionType.MULTIPLY,
                "**": FunctionType.POWER,
                "/": FunctionType.DIVIDE,
                "%": FunctionType.MOD,
            }.get(op)
            if operator is None:
                raise ValueError(f"Unknown operator: {op}")
            result = self.function_factory.create_function(
                [result, right], operator=operator
            )
        return result

    def sum_operator(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]).lower()
            right = args[i + 1]
            operator = {
                "+": FunctionType.ADD,
                "-": FunctionType.SUBTRACT,
                "||": FunctionType.CONCAT,
                "like": FunctionType.LIKE,
                "ilike": FunctionType.ILIKE,
            }.get(op)
            if operator is None:
                raise ValueError(f"Unknown operator: {op}")
            result = self.function_factory.create_function(
                [result, right],
                operator=operator,
                meta=core_meta(meta),
            )
        return result

    def comparison(self, meta: SyntaxMeta | None, args: list[Any]) -> Any:
        if len(args) == 1:
            return args[0]
        left = args[0]
        operator = args[1]
        right = args[2]
        if operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
            return SubselectComparison(left=left, right=right, operator=operator)
        for concept_side, value_side in ((left, right), (right, left)):
            if isinstance(concept_side, ConceptRef) and isinstance(
                concept_side.datatype,
                EnumType,
            ):
                if (
                    isinstance(value_side, (str, int))
                    and value_side not in concept_side.datatype.values
                ):
                    raise InvalidSyntaxException(
                        f"Value {value_side!r} is not a valid member of enum {concept_side.datatype} for '{concept_side.address}'"
                    )
        return Comparison(left=left, right=right, operator=operator)

    def between_comparison(
        self, meta: SyntaxMeta | None, args: list[Any]
    ) -> Conditional:
        return Conditional(
            left=Comparison(
                left=args[0], right=args[1], operator=ComparisonOperator.GTE
            ),
            right=Comparison(
                left=args[0], right=args[2], operator=ComparisonOperator.LTE
            ),
            operator=BooleanOperator.AND,
        )

    def parenthetical(self, meta: SyntaxMeta | None, args: list[Any]) -> Parenthetical:
        return Parenthetical(content=args[0])

    def prop_ident(
        self, meta: SyntaxMeta | None, args: list[Any]
    ) -> tuple[list[Concept], str]:
        return [self.environment.concepts[grain] for grain in args[:-1]], args[-1]

    def prop_ident_wildcard(
        self,
        meta: SyntaxMeta | None,
        args: list[Any],
    ) -> tuple[list[Concept], str]:
        return [
            self.environment.concepts[f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"]
        ], str(args[0])

    def PARSE_COMMENT(self, token: SyntaxToken) -> Comment:
        return Comment(text=token.value.rstrip())

    def IDENTIFIER(self, token: SyntaxToken) -> str:
        return token.value

    def QUOTED_IDENTIFIER(self, token: SyntaxToken) -> str:
        return token.value[1:-1]

    def STRING_CHARS(self, token: SyntaxToken) -> str:
        return token.value

    def SINGLE_STRING_CHARS(self, token: SyntaxToken) -> str:
        return token.value

    def DOUBLE_STRING_CHARS(self, token: SyntaxToken) -> str:
        return token.value

    def MULTILINE_STRING(self, token: SyntaxToken) -> str:
        return token.value[3:-3]

    def PURPOSE(self, token: SyntaxToken) -> Purpose:
        return Purpose(token.value)

    def AUTO(self, token: SyntaxToken) -> Purpose:
        return Purpose.AUTO

    def CONST(self, token: SyntaxToken) -> Purpose:
        return Purpose.CONSTANT

    def PROPERTY(self, token: SyntaxToken) -> Purpose:
        return Purpose.PROPERTY

    def UNIQUE(self, token: SyntaxToken) -> str:
        return token.value

    def COMPARISON_OPERATOR(self, token: SyntaxToken) -> ComparisonOperator:
        return ComparisonOperator(token.value.strip())

    def PLUS_OR_MINUS(self, token: SyntaxToken) -> str:
        return token.value

    def MULTIPLY_DIVIDE_PERCENT(self, token: SyntaxToken) -> str:
        return token.value

    def CONCEPTS(self, token: SyntaxToken) -> str:
        return token.value

    def DATASOURCES(self, token: SyntaxToken) -> str:
        return token.value
