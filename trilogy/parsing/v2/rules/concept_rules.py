from __future__ import annotations

import difflib
from datetime import date, datetime
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import (
    ConceptSource,
    Derivation,
    FunctionType,
    Granularity,
    Modifier,
    Purpose,
    ShowCategory,
)
from trilogy.core.exceptions import MissingParameterException
from trilogy.core.internal import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.models.author import (
    AggregateWrapper,
    Comparison,
    Concept,
    ConceptRef,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    Metadata,
    Parenthetical,
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
    StructComponent,
    StructType,
    TraitDataType,
    TupleWrapper,
    arg_to_datatype,
    is_compatible_datatype,
)
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ConceptDerivationStatement,
    ShowStatement,
)
from trilogy.parsing.common import constant_to_concept
from trilogy.parsing.v2.concept_factory import arbitrary_to_concept_v2
from trilogy.parsing.v2.concept_syntax import (
    ConceptDeclarationSyntax,
    ConceptDerivationSyntax,
    ConceptPropertyDeclarationSyntax,
    ParameterDeclarationSyntax,
    PropertyIdentifierSyntax,
    PropertyWildcardSyntax,
)
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    apply_source_location,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.semantic_state import ConceptLookup
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind, SyntaxToken

CONSTANT_TYPES = (int, float, str, bool, ListWrapper, TupleWrapper, MapWrapper)


def metadata_from_meta(
    meta: Any | None,
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
    concepts: ConceptLookup | None = None,
) -> tuple[str, str, str, str | None]:
    parent = None
    if "." in name:
        if purpose == Purpose.PROPERTY:
            parent, name = name.rsplit(".", 1)
            if concepts is None:
                raise ValueError(
                    "parse_concept_reference requires a ConceptLookup when "
                    "resolving a PROPERTY parent reference"
                )
            parent_concept = concepts.require(parent)
            namespace = parent_concept.namespace or DEFAULT_NAMESPACE
            lookup = f"{namespace}.{name}"
        else:
            namespace, name = name.rsplit(".", 1)
            lookup = f"{namespace}.{name}"
    else:
        namespace = environment.namespace or DEFAULT_NAMESPACE
        lookup = name
    return lookup, namespace, name, parent


def parameter_default(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    return hydrate(node.children[0])


def parameter_declaration(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Concept:
    syntax = ParameterDeclarationSyntax.from_node(node)
    metadata = hydrate(syntax.metadata) if syntax.metadata else Metadata()
    default = hydrate(syntax.default) if syntax.default else None
    name = hydrate(syntax.name)
    datatype = hydrate(syntax.datatype)
    _, _, name, _ = parse_concept_reference(
        name, context.environment, concepts=context.concepts
    )
    raw = context.environment.parameters.get(name, default)
    if raw is None:
        raise MissingParameterException(
            f'This script requires parameter "{name}" to be set in environment.'
        )
    if datatype == DataType.INTEGER:
        parameter_value: Any = int(raw)
    elif datatype == DataType.FLOAT:
        parameter_value = float(raw)
    elif datatype == DataType.BOOL:
        parameter_value = bool(raw)
    elif datatype == DataType.STRING:
        parameter_value = str(raw)
    elif datatype == DataType.DATE:
        parameter_value = raw if isinstance(raw, date) else date.fromisoformat(raw)
    elif datatype == DataType.DATETIME:
        parameter_value = (
            raw if isinstance(raw, datetime) else datetime.fromisoformat(raw)
        )
    else:
        raise fail(node, f"Unsupported datatype {datatype} for parameter {name}.")
    return build_constant_derivation(
        meta=node.meta,
        name=name,
        constant=parameter_value,
        metadata=metadata,
        context=context,
    )


def concept_declaration(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptDeclarationStatement:
    syntax = ConceptDeclarationSyntax.from_node(node)
    metadata = hydrate(syntax.metadata) if syntax.metadata else Metadata()
    modifiers = [hydrate(syntax.nullable)] if syntax.nullable else []
    purpose = hydrate(syntax.purpose)
    name = hydrate(syntax.name)
    datatype = hydrate(syntax.datatype)
    _, namespace, name, _ = parse_concept_reference(
        name, context.environment, concepts=context.concepts
    )
    concept_value = Concept(
        name=name,
        datatype=datatype,
        purpose=purpose,
        metadata=metadata,
        namespace=namespace,
        modifiers=modifiers,
        derivation=Derivation.ROOT,
        granularity=Granularity.MULTI_ROW,
    )
    apply_source_location(concept_value, node.meta)
    context.add_top_level_concept(concept_value, meta=core_meta(node.meta))
    return ConceptDeclarationStatement(concept=concept_value)


def concept_property_declaration(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Concept:
    syntax = ConceptPropertyDeclarationSyntax.from_node(node)
    unique = syntax.unique is not None
    metadata = hydrate(syntax.metadata) if syntax.metadata else Metadata()
    modifiers = [hydrate(syntax.nullable)] if syntax.nullable else []
    declaration = hydrate(syntax.declaration)
    datatype = hydrate(syntax.datatype)
    if not isinstance(declaration, tuple):
        if "." not in declaration:
            raise fail(
                node,
                f"Property declaration {declaration} must be fully qualified with a parent key",
            )
        grain, name = declaration.rsplit(".", 1)
        parent = context.concepts.require(grain)
        parents = [parent]
        namespace = parent.namespace
    else:
        parents, name = declaration
        namespace = context.environment.namespace or DEFAULT_NAMESPACE
    grain_components = {x.address for x in parents}
    all_rows_addr = f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"
    is_abstract_grain = grain_components == {all_rows_addr}
    concept_value = Concept(
        name=name,
        datatype=datatype,
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
    context.add_property_concept(concept_value, meta=core_meta(node.meta))
    return concept_value


def concept_derivation(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptDerivationStatement:
    syntax = ConceptDerivationSyntax.from_node(node)
    metadata = hydrate(syntax.metadata) if syntax.metadata else None
    purpose = hydrate(syntax.purpose)
    raw_name = hydrate(syntax.name)
    if isinstance(raw_name, str):
        _, namespace, name, parent_concept = parse_concept_reference(
            raw_name,
            context.environment,
            purpose,
            concepts=context.concepts,
        )
        keys = (
            [context.concepts.require(parent_concept).address] if parent_concept else []
        )
    else:
        keys, name = raw_name
        keys = [x.address for x in keys]
        namespaces = {x.rsplit(".", 1)[0] for x in keys}
        namespace = (
            context.environment.namespace or DEFAULT_NAMESPACE
            if len(namespaces) != 1
            else namespaces.pop()
        )
    source_value = hydrate(syntax.source)
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
        concept_value = arbitrary_to_concept_v2(
            source_value,
            name=name,
            namespace=namespace,
            context=context,
            metadata=metadata,
        )
        if purpose == Purpose.KEY and concept_value.purpose != Purpose.KEY:
            concept_value.purpose = Purpose.KEY
        elif (
            purpose
            and purpose != Purpose.AUTO
            and concept_value.purpose != purpose
            and purpose != Purpose.CONSTANT
        ):
            raise fail(
                node,
                f'Concept {name} purpose {concept_value.purpose} does not match declared purpose {purpose}. Suggest defaulting to "auto"',
            )
        if purpose == Purpose.PROPERTY and keys:
            concept_value.keys = set(keys)
    elif isinstance(source_value, CONSTANT_TYPES):
        concept_value = constant_to_concept(
            source_value,
            name=name,
            namespace=namespace,
            metadata=metadata,
        )
    elif isinstance(source_value, ConceptRef):
        concept_value = arbitrary_to_concept_v2(
            context.function_factory.create_function(
                [source_value],
                FunctionType.ALIAS,
                meta=core_meta(node.meta),
            ),
            name=name,
            namespace=namespace,
            context=context,
            metadata=metadata,
        )
    else:
        snippet = ""
        if (
            node.meta
            and node.meta.start_pos is not None
            and node.meta.end_pos is not None
        ):
            snippet = context.source_text[node.meta.start_pos : node.meta.end_pos]
        raise fail(
            node,
            f"Received invalid type {type(source_value)} {source_value} as input to concept derivation: `{snippet}`",
        )
    apply_source_location(concept_value, node.meta)
    context.add_top_level_concept(concept_value, meta=core_meta(node.meta))
    return ConceptDerivationStatement(concept=concept_value)


def build_constant_derivation(
    meta: Any | None,
    name: str,
    constant: Any,
    metadata: Metadata | None,
    context: RuleContext,
) -> Concept:
    _, namespace, name, _ = parse_concept_reference(
        name, context.environment, concepts=context.concepts
    )
    concept_value = Concept(
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
    apply_source_location(concept_value, meta)
    context.add_top_level_concept(concept_value, meta=core_meta(meta))
    return concept_value


def show_category(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ShowCategory:
    return ShowCategory(hydrate(node.children[0]))


def show_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ShowStatement:
    return ShowStatement(content=hydrate(node.children[0]))


def concept_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptDeclarationStatement:
    declarations = node.child_nodes()
    if len(declarations) != 1:
        raise fail(
            node,
            f"Expected one child declaration under concept node, found {len(declarations)}",
        )
    output = hydrate(declarations[0])
    if isinstance(output, list):
        concept_value = output[0]
    elif isinstance(output, Concept):
        concept_value = output
    else:
        concept_value = output.concept
    apply_source_location(concept_value, node.meta)
    return ConceptDeclarationStatement(concept=concept_value)


def concept_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ConceptRef:
    address = hydrate(node.children[0])
    if "." not in address and context.environment.namespace == DEFAULT_NAMESPACE:
        address = f"{DEFAULT_NAMESPACE}.{address}"
    mapping = context.concepts.require(address)
    return ConceptRef(
        address=mapping.address,
        metadata=metadata_from_meta(node.meta),
        datatype=mapping.output_datatype,
    )


def data_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    resolved: Any = None
    traits: list[str] = []
    for child in node.children:
        if isinstance(child, SyntaxToken) and child.value == "::":
            continue
        if resolved is None:
            resolved = hydrate(child)
        else:
            traits.append(hydrate(child))
    base: Any
    if isinstance(
        resolved,
        (StructType, ArrayType, NumericType, MapType, EnumType),
    ):
        base = resolved
    else:
        base = DataType(str(resolved).lower())
    if traits:
        line = node.meta.line if node.meta else None
        for trait in traits:
            matched = context.types.get(trait)
            if matched is None:
                known = set(context.environment.data_types.keys())
                known.update(name for name, _ in context.semantic_state.pending_types())
                similar = difflib.get_close_matches(trait, list(known))
                hint = f" Did you mean: {', '.join(similar)}?" if similar else ""
                raise TypeError(
                    f"Invalid type (trait) {trait} for {base}, line {line}.{hint}"
                )
            if not is_compatible_datatype(matched.type, base):
                raise TypeError(
                    f"Invalid type (trait) {trait} for {base}, line {line}. "
                    f"Trait expects type {matched.type}, has {base}"
                )
        return TraitDataType(type=base, traits=traits)
    return base


def numeric_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> NumericType:
    args = hydrated_children(node, hydrate)
    return NumericType(precision=args[0], scale=args[1])


def map_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> MapType:
    args = hydrated_children(node, hydrate)
    key = args[0]
    value = args[1]
    if isinstance(key, str):
        key = context.concepts.require(key)
    if isinstance(value, str):
        value = context.concepts.require(value)
    return MapType(key_type=key, value_type=value)


def list_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ArrayType:
    args = hydrated_children(node, hydrate)
    content = args[0]
    if isinstance(content, str):
        content = context.concepts.require(content)
    return ArrayType(type=content)


def struct_component(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> StructComponent:
    args = hydrated_children(node, hydrate)
    modifiers = [a for a in args if isinstance(a, Modifier)]
    return StructComponent(name=str(args[0]), type=args[1], modifiers=modifiers)


def struct_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> StructType:
    args = hydrated_children(node, hydrate)
    final: list[Any] = []
    for arg in args:
        if isinstance(arg, StructComponent):
            final.append(arg)
        else:
            final.append(context.concepts.require(arg))
    return StructType(
        fields=final,
        fields_map={
            x.name: x for x in final if isinstance(x, (Concept, StructComponent))
        },
    )


def enum_type(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> EnumType:
    args = hydrated_children(node, hydrate)
    base_type = args[0]
    if not isinstance(base_type, DataType):
        raise fail(
            node, f"enum base type must be a primitive DataType, got {base_type}"
        )
    return EnumType(type=base_type, values=list(args[1:]))


def concept_nullable_modifier(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Modifier:
    return Modifier.NULLABLE


def metadata(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Metadata:
    values = hydrated_children(node, hydrate)
    pairs = {key: val for key, val in zip(values[::2], values[1::2])}
    return Metadata(**pairs)


def prop_ident(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> tuple[list[Concept], str]:
    syntax = PropertyIdentifierSyntax.from_node(node)
    grains = [hydrate(grain) for grain in syntax.grains]
    return [context.concepts.require(grain) for grain in grains], hydrate(syntax.name)


def prop_ident_wildcard(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> tuple[list[Concept], str]:
    syntax = PropertyWildcardSyntax.from_node(node)
    return [
        context.concepts.require(f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}")
    ], hydrate(syntax.name)


CONCEPT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CONCEPT: concept_statement,
    SyntaxNodeKind.PARAMETER_DEFAULT: parameter_default,
    SyntaxNodeKind.PARAMETER_DECLARATION: parameter_declaration,
    SyntaxNodeKind.CONCEPT_DECLARATION: concept_declaration,
    SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION: concept_property_declaration,
    SyntaxNodeKind.CONCEPT_DERIVATION: concept_derivation,
    SyntaxNodeKind.SHOW_CATEGORY: show_category,
    SyntaxNodeKind.SHOW_STATEMENT: show_statement,
    SyntaxNodeKind.CONCEPT_LITERAL: concept_lit,
    SyntaxNodeKind.DATA_TYPE: data_type,
    SyntaxNodeKind.NUMERIC_TYPE: numeric_type,
    SyntaxNodeKind.MAP_TYPE: map_type,
    SyntaxNodeKind.LIST_TYPE: list_type,
    SyntaxNodeKind.STRUCT_TYPE: struct_type,
    SyntaxNodeKind.STRUCT_COMPONENT: struct_component,
    SyntaxNodeKind.ENUM_TYPE: enum_type,
    SyntaxNodeKind.CONCEPT_NULLABLE_MODIFIER: concept_nullable_modifier,
    SyntaxNodeKind.METADATA: metadata,
    SyntaxNodeKind.PROPERTY_IDENTIFIER: prop_ident,
    SyntaxNodeKind.PROPERTY_IDENTIFIER_WILDCARD: prop_ident_wildcard,
}
