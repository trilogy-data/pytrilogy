from __future__ import annotations

from trilogy.core.enums import (
    CreateMode,
    IOType,
    PublishAction,
    ValidationScope,
)
from trilogy.core.statements.author import (
    ChartStatement,
    CopyStatement,
    CreateStatement,
    MockStatement,
    PublishStatement,
    SelectStatement,
    ValidateStatement,
)
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
    SyntaxToken,
    SyntaxTokenKind,
)


def _parse_validate_scope(token: SyntaxToken) -> ValidationScope:
    base = token.value.lower()
    if not base.endswith("s"):
        base += "s"
    return ValidationScope(base)


def _parse_create_modifier(node: SyntaxNode) -> CreateMode:
    # create_modifier_clause wraps a single CREATE_IF_NOT_EXISTS or
    # CREATE_OR_REPLACE anonymous token.
    for token in node.child_tokens():
        value = token.value.lower().strip()
        if "replace" in value:
            return CreateMode.CREATE_OR_REPLACE
        if "not" in value and "exists" in value:
            return CreateMode.CREATE_IF_NOT_EXISTS
    return CreateMode.CREATE


def create_modifier_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CreateMode:
    return _parse_create_modifier(node)


def publish_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> PublishStatement:
    targets: list[str] = []
    scope = ValidationScope.DATASOURCES
    action = PublishAction.PUBLISH
    for token in node.child_tokens():
        if token.kind == SyntaxTokenKind.PUBLISH_ACTION:
            action = PublishAction(token.value.lower())
        elif token.kind == SyntaxTokenKind.VALIDATE_SCOPE:
            scope = _parse_validate_scope(token)
            if scope != ValidationScope.DATASOURCES:
                raise fail(
                    node,
                    f"Publishing is only supported for Datasources, got {scope}",
                )
        elif token.kind == SyntaxTokenKind.IDENTIFIER:
            targets.append(token.value)
    return PublishStatement(scope=scope, targets=targets, action=action)


def create_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CreateStatement:
    targets: list[str] = []
    scope = ValidationScope.DATASOURCES
    mode = CreateMode.CREATE
    for token in node.child_tokens():
        if token.kind == SyntaxTokenKind.VALIDATE_SCOPE:
            scope = _parse_validate_scope(token)
            if scope != ValidationScope.DATASOURCES:
                raise fail(
                    node,
                    f"Creating is only supported for Datasources, got {scope}",
                )
        elif token.kind == SyntaxTokenKind.IDENTIFIER:
            targets.append(token.value)
    modifier = node.optional_node(SyntaxNodeKind.CREATE_MODIFIER_CLAUSE)
    if modifier is not None:
        mode = _parse_create_modifier(modifier)
    return CreateStatement(scope=scope, targets=targets, create_mode=mode)


def validate_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ValidateStatement:
    scope: ValidationScope | None = None
    targets: list[str] = []
    for child in node.child_tokens():
        if child.kind == SyntaxTokenKind.VALIDATE_SCOPE:
            scope = _parse_validate_scope(child)
        elif child.kind == SyntaxTokenKind.IDENTIFIER:
            targets.append(child.value)
    if scope is None:
        return ValidateStatement(scope=ValidationScope.ALL, targets=None)
    return ValidateStatement(scope=scope, targets=targets or None)


def mock_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> MockStatement:
    scope = ValidationScope.DATASOURCES
    targets: list[str] = []
    for child in node.child_tokens():
        if child.kind == SyntaxTokenKind.VALIDATE_SCOPE:
            scope = _parse_validate_scope(child)
        elif child.kind == SyntaxTokenKind.IDENTIFIER:
            targets.append(child.value)
    return MockStatement(scope=scope, targets=targets)


def copy_option(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> tuple[str, object]:
    identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
    if not identifiers:
        raise fail(node, "Copy option missing name")
    key = identifiers[0].value
    literal_nodes = node.child_nodes()
    if not literal_nodes:
        raise fail(node, f"Copy option '{key}' missing value")
    return key, hydrate(literal_nodes[0])


def copy_options(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, object]:
    result: dict[str, object] = {}
    for child in node.child_nodes(SyntaxNodeKind.COPY_OPTION):
        hydrated = hydrate(child)
        if not isinstance(hydrated, tuple):
            raise fail(child, "Copy option failed to hydrate")
        key, value = hydrated
        if key in result:
            raise fail(child, f"Duplicate copy option '{key}'")
        result[key] = value
    return result


def copy_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CopyStatement:
    target_type: IOType | None = None
    target: str | None = None
    source: SelectStatement | ChartStatement | None = None
    options: dict[str, object] = {}
    file_path_kinds = (SyntaxTokenKind.FILE_PATH, SyntaxTokenKind.F_FILE_PATH)
    for token in node.child_tokens():
        if token.kind == SyntaxTokenKind.COPY_TYPE and target_type is None:
            target_type = IOType(hydrate(token))
        elif token.kind in file_path_kinds and target is None:
            target = str(hydrate(token))
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.STRING_LITERAL and target is None:
            target = str(hydrate(child))
        elif child.kind == SyntaxNodeKind.COPY_OPTIONS:
            hydrated = hydrate(child)
            if not isinstance(hydrated, dict):
                raise fail(child, "Copy options failed to hydrate")
            options = hydrated
        elif child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            source = hydrate(child)
        elif child.kind == SyntaxNodeKind.CHART_STATEMENT:
            source = hydrate(child)
    if target_type is None or target is None or source is None:
        raise fail(node, "Malformed copy statement: missing type/target/source")
    if isinstance(source, ChartStatement) and not target_type.is_chart_format:
        raise fail(
            node,
            f"Copy source 'chart' requires a chart format (png/svg/html/pdf), got {target_type.value}",
        )
    if not isinstance(source, ChartStatement) and target_type.is_chart_format:
        raise fail(
            node,
            f"Copy format {target_type.value} requires a chart source, got a select",
        )
    return CopyStatement(
        target=target,
        target_type=target_type,
        select=source,
        options=options,
        meta=metadata_from_meta(node.meta),
    )


OPERATIONAL_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CREATE_MODIFIER_CLAUSE: create_modifier_clause,
    SyntaxNodeKind.CREATE_STATEMENT: create_statement,
    SyntaxNodeKind.VALIDATE_STATEMENT: validate_statement,
    SyntaxNodeKind.MOCK_STATEMENT: mock_statement,
    SyntaxNodeKind.PUBLISH_STATEMENT: publish_statement,
    SyntaxNodeKind.COPY_STATEMENT: copy_statement,
    SyntaxNodeKind.COPY_OPTION: copy_option,
    SyntaxNodeKind.COPY_OPTIONS: copy_options,
}
