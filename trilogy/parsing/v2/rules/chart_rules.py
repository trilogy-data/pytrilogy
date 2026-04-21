from __future__ import annotations

from typing import Any

from trilogy.core.enums import ChartPlaceKind, ChartType, ConceptSource
from trilogy.core.models.author import ConceptRef
from trilogy.core.statements.author import (
    CHART_ROLES,
    ChartLayer,
    ChartLayerBinding,
    ChartPlacement,
    ChartStatement,
    ConceptTransform,
    SelectItem,
    SelectStatement,
)
from trilogy.parsing.v2.concept_factory import (
    arbitrary_to_concept_v2,
    unwrap_transformation_v2,
)
from trilogy.parsing.v2.rules.concept_rules import (
    metadata_from_meta,
    parse_concept_reference,
)
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
)
from trilogy.parsing.v2.syntax import (
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxTokenKind,
)

_BOOL_MAP = {"hide_legend": "hide_legend", "show_title": "show_title"}
_SCALE_MAP = {"scale_x": "scale_x", "scale_y": "scale_y"}
_ROLE_SET = frozenset(CHART_ROLES)


def chart_layer_binding(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ChartLayerBinding:
    identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
    if not identifiers:
        raise fail(node, "Chart layer binding missing role name")
    role_token = identifiers[0]
    role = role_token.value
    if role not in _ROLE_SET:
        raise fail(
            role_token,
            f"Unknown chart role '{role}'. Expected one of: {', '.join(CHART_ROLES)}.",
        )
    expr_nodes = node.child_nodes()
    if not expr_nodes:
        raise fail(node, f"Chart role '{role}' is missing a binding expression")
    expr = hydrate(expr_nodes[0])
    alias = identifiers[1].value if len(identifiers) > 1 else None
    return ChartLayerBinding(role=role, expr=expr, alias=alias)


def chart_layer_body(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[ChartLayerBinding]:
    bindings: list[ChartLayerBinding] = []
    seen_roles: set[str] = set()
    for child in node.child_nodes(SyntaxNodeKind.CHART_LAYER_BINDING):
        binding = hydrate(child)
        if not isinstance(binding, ChartLayerBinding):
            raise fail(child, "Chart layer binding failed to hydrate")
        if binding.role in seen_roles:
            raise fail(
                child,
                f"Chart role '{binding.role}' may only be assigned once per layer.",
            )
        seen_roles.add(binding.role)
        bindings.append(binding)
    if not bindings:
        raise fail(node, "Chart layer must declare at least one binding")
    return bindings


def _binding_to_select_item(
    binding: ChartLayerBinding,
    binding_node: SyntaxNode,
    context: RuleContext,
) -> SelectItem:
    if binding.alias is not None:
        transformation = unwrap_transformation_v2(binding.expr, context)
        _, namespace, name, _ = parse_concept_reference(
            binding.alias, context.environment
        )
        concept = arbitrary_to_concept_v2(
            transformation,
            context=context,
            namespace=namespace,
            name=name,
            metadata=metadata_from_meta(
                binding_node.meta, concept_source=ConceptSource.SELECT
            ),
        )
        context.add_select_concept(concept, meta=core_meta(binding_node.meta))
        return SelectItem(
            content=ConceptTransform(function=transformation, output=concept)
        )
    if isinstance(binding.expr, ConceptRef):
        return SelectItem(content=binding.expr)
    raise fail(
        binding_node,
        f"Chart binding for role '{binding.role}' uses a computed expression"
        " and must declare an alias: `<role> <- <expr> as <name>`.",
    )


def chart_layer(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ChartLayer:
    layer_type_tokens = node.child_tokens(SyntaxTokenKind.CHART_TYPE)
    if not layer_type_tokens:
        raise fail(node, "Chart layer missing type")
    layer_type = ChartType(hydrate(layer_type_tokens[0]))
    body_node = node.first_child_node(SyntaxNodeKind.CHART_LAYER_BODY)
    binding_nodes = body_node.child_nodes(SyntaxNodeKind.CHART_LAYER_BINDING)
    explicit_select_node = node.optional_node(SyntaxNodeKind.SELECT_STATEMENT)
    select: SelectStatement
    if explicit_select_node is not None:
        hydrated = hydrate(explicit_select_node)
        if not isinstance(hydrated, SelectStatement):
            raise fail(explicit_select_node, "Chart layer select failed to hydrate")
        bindings = hydrate(body_node)
        if not isinstance(bindings, list):
            raise fail(body_node, "Chart layer body failed to hydrate")
        for binding, binding_node in zip(bindings, binding_nodes):
            if binding.alias is not None or not isinstance(binding.expr, ConceptRef):
                raise fail(
                    binding_node,
                    f"Chart role '{binding.role}' must be a direct concept"
                    " reference when `from select ...` is provided; put"
                    " transformations inside the select.",
                )
        select = hydrated
    else:
        bindings = hydrate(body_node)
        if not isinstance(bindings, list):
            raise fail(body_node, "Chart layer body failed to hydrate")
        select_items = [
            _binding_to_select_item(binding, binding_node, context)
            for binding, binding_node in zip(bindings, binding_nodes)
        ]
        select = SelectStatement(
            selection=select_items,
            meta=metadata_from_meta(node.meta),
        )
    return ChartLayer(layer_type=layer_type, bindings=bindings, select=select)


def chart_place(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ChartPlacement:
    kind_tokens = node.child_tokens(SyntaxTokenKind.CHART_PLACE_TYPE)
    if not kind_tokens:
        raise fail(node, "Chart place missing type")
    kind = ChartPlaceKind(hydrate(kind_tokens[0]))
    literal_node = node.first_child_node(SyntaxNodeKind.LITERAL)
    value = hydrate(literal_node)
    label_tokens = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
    label = label_tokens[0].value if label_tokens else None
    return ChartPlacement(kind=kind, value=value, label=label)


def chart_bool_setting(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, Any]:
    tokens = node.child_tokens()
    return {_BOOL_MAP[tokens[0].value.lower()]: True}


def chart_scale_setting(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, Any]:
    tokens = node.child_tokens()
    return {_SCALE_MAP[tokens[0].value.lower()]: tokens[1].value.lower()}


def chart_component(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    return hydrate(node.first_child_node())


def chart_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ChartStatement:
    layers: list[ChartLayer] = []
    placements: list[ChartPlacement] = []
    scalar_settings: dict[str, Any] = {}
    for child in node.child_nodes(SyntaxNodeKind.CHART_COMPONENT):
        hydrated = hydrate(child)
        if isinstance(hydrated, ChartLayer):
            layers.append(hydrated)
        elif isinstance(hydrated, ChartPlacement):
            placements.append(hydrated)
        elif isinstance(hydrated, dict):
            scalar_settings.update(hydrated)
    if not layers:
        raise fail(node, "Chart statement must declare at least one layer")
    return ChartStatement(
        layers=layers,
        placements=placements,
        meta=metadata_from_meta(node.meta),
        **scalar_settings,
    )


CHART_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CHART_STATEMENT: chart_statement,
    SyntaxNodeKind.CHART_COMPONENT: chart_component,
    SyntaxNodeKind.CHART_LAYER: chart_layer,
    SyntaxNodeKind.CHART_LAYER_BODY: chart_layer_body,
    SyntaxNodeKind.CHART_LAYER_BINDING: chart_layer_binding,
    SyntaxNodeKind.CHART_PLACE: chart_place,
    SyntaxNodeKind.CHART_BOOL_SETTING: chart_bool_setting,
    SyntaxNodeKind.CHART_SCALE_SETTING: chart_scale_setting,
}
