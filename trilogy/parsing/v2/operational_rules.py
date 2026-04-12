from __future__ import annotations

from trilogy.core.enums import (
    CreateMode,
    IOType,
    PublishAction,
    ValidationScope,
)
from trilogy.core.statements.author import (
    CopyStatement,
    CreateStatement,
    MockStatement,
    PublishStatement,
    SelectStatement,
    ValidateStatement,
)
from trilogy.parsing.v2.concept_rules import metadata_from_meta
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
    for child in node.children:
        if isinstance(child, SyntaxToken):
            value = child.value.lower().strip()
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
    for child in node.children:
        if isinstance(child, SyntaxToken):
            if child.kind == SyntaxTokenKind.PUBLISH_ACTION:
                action = PublishAction(child.value.lower())
            elif child.kind == SyntaxTokenKind.VALIDATE_SCOPE:
                scope = _parse_validate_scope(child)
                if scope != ValidationScope.DATASOURCES:
                    raise fail(
                        node,
                        f"Publishing is only supported for Datasources, got {scope}",
                    )
            elif child.kind == SyntaxTokenKind.IDENTIFIER:
                targets.append(child.value)
    return PublishStatement(scope=scope, targets=targets, action=action)


def create_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CreateStatement:
    targets: list[str] = []
    scope = ValidationScope.DATASOURCES
    mode = CreateMode.CREATE
    for child in node.children:
        if isinstance(child, SyntaxToken):
            if child.kind == SyntaxTokenKind.VALIDATE_SCOPE:
                scope = _parse_validate_scope(child)
                if scope != ValidationScope.DATASOURCES:
                    raise fail(
                        node,
                        f"Creating is only supported for Datasources, got {scope}",
                    )
            elif child.kind == SyntaxTokenKind.IDENTIFIER:
                targets.append(child.value)
        elif (
            isinstance(child, SyntaxNode)
            and child.kind == SyntaxNodeKind.CREATE_MODIFIER_CLAUSE
        ):
            mode = _parse_create_modifier(child)
    return CreateStatement(scope=scope, targets=targets, create_mode=mode)


def validate_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ValidateStatement:
    scope: ValidationScope | None = None
    targets: list[str] = []
    for child in node.children:
        if isinstance(child, SyntaxToken):
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
    for child in node.children:
        if isinstance(child, SyntaxToken):
            if child.kind == SyntaxTokenKind.VALIDATE_SCOPE:
                scope = _parse_validate_scope(child)
            elif child.kind == SyntaxTokenKind.IDENTIFIER:
                targets.append(child.value)
    return MockStatement(scope=scope, targets=targets)


def copy_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CopyStatement:
    target_type: IOType | None = None
    target: str | None = None
    select: SelectStatement | None = None
    for child in node.children:
        if isinstance(child, SyntaxToken):
            if (
                child.kind == SyntaxTokenKind.COPY_TYPE
                and target_type is None
            ):
                target_type = IOType(hydrate(child))
            elif child.kind in (
                SyntaxTokenKind.FILE_PATH,
                SyntaxTokenKind.F_FILE_PATH,
            ) and target is None:
                target = str(hydrate(child))
        elif isinstance(child, SyntaxNode):
            if child.kind == SyntaxNodeKind.STRING_LITERAL and target is None:
                target = str(hydrate(child))
            elif child.kind == SyntaxNodeKind.SELECT_STATEMENT:
                select = hydrate(child)
    if target_type is None or target is None or select is None:
        raise fail(node, "Malformed copy statement: missing type/target/select")
    return CopyStatement(
        target=target,
        target_type=target_type,
        select=select,
        meta=metadata_from_meta(node.meta),
    )


OPERATIONAL_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CREATE_MODIFIER_CLAUSE: create_modifier_clause,
    SyntaxNodeKind.CREATE_STATEMENT: create_statement,
    SyntaxNodeKind.VALIDATE_STATEMENT: validate_statement,
    SyntaxNodeKind.MOCK_STATEMENT: mock_statement,
    SyntaxNodeKind.PUBLISH_STATEMENT: publish_statement,
    SyntaxNodeKind.COPY_STATEMENT: copy_statement,
}
