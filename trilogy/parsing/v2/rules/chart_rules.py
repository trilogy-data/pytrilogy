from __future__ import annotations

from typing import Any

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig, ChartStatement, SelectStatement
from trilogy.parsing.v2.rules.concept_rules import metadata_from_meta
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    fail,
)
from trilogy.parsing.v2.syntax import (
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxTokenKind,
)

_FIELD_MAP = {
    "x_axis": "x_fields",
    "y_axis": "y_fields",
    "color": "color_field",
    "size": "size_field",
    "group": "group_field",
    "trellis": "trellis_field",
    "trellis_row": "trellis_row_field",
    "geo": "geo_field",
    "annotation": "annotation_field",
}
_BOOL_MAP = {"hide_legend": "hide_legend", "show_title": "show_title"}
_SCALE_MAP = {"scale_x": "scale_x", "scale_y": "scale_y"}


def chart_field_setting(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, Any]:
    tokens = node.child_tokens()
    field_name = tokens[0].value.lower()
    idents = [t.value for t in tokens[1:]]
    key = _FIELD_MAP[field_name]
    if key in ("x_fields", "y_fields"):
        return {key: idents}
    return {key: idents[0] if idents else None}


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


def chart_setting(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, Any]:
    child = node.children[0]
    return hydrate(child)


def chart_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ChartStatement:
    chart_type: ChartType | None = None
    settings: dict[str, Any] = {"x_fields": [], "y_fields": []}
    select: SelectStatement | None = None
    setting_kinds = {
        SyntaxNodeKind.CHART_SETTING,
        SyntaxNodeKind.CHART_FIELD_SETTING,
        SyntaxNodeKind.CHART_BOOL_SETTING,
        SyntaxNodeKind.CHART_SCALE_SETTING,
    }
    for token in node.child_tokens(SyntaxTokenKind.CHART_TYPE):
        chart_type = ChartType(hydrate(token))
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            select = hydrate(child)
            continue
        if child.kind in setting_kinds:
            piece = hydrate(child)
            for k, v in piece.items():
                if k in ("x_fields", "y_fields"):
                    settings[k].extend(v)
                else:
                    settings[k] = v
    if chart_type is None or select is None:
        raise fail(node, "Malformed chart statement: missing chart type or select")
    return ChartStatement(
        config=ChartConfig(chart_type=chart_type, **settings),
        select=select,
        meta=metadata_from_meta(node.meta),
    )


CHART_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CHART_STATEMENT: chart_statement,
    SyntaxNodeKind.CHART_SETTING: chart_setting,
    SyntaxNodeKind.CHART_FIELD_SETTING: chart_field_setting,
    SyntaxNodeKind.CHART_BOOL_SETTING: chart_bool_setting,
    SyntaxNodeKind.CHART_SCALE_SETTING: chart_scale_setting,
}
