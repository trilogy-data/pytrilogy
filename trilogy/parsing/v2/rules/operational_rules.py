from __future__ import annotations

import re
from collections.abc import Sequence

from trilogy.core.enums import (
    CreateMode,
    IOType,
    PublishAction,
    QueryComparison,
    ValidationScope,
)
from trilogy.core.statements.author import (
    CallStatement,
    ChartStatement,
    CopyStatement,
    CreateStatement,
    MockStatement,
    NaturalSelectStatement,
    PublishStatement,
    SelectStatement,
    ValidateNaturalStatement,
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
    populate = False
    for token in node.child_tokens():
        if token.kind == SyntaxTokenKind.VALIDATE_SCOPE:
            scope = _parse_validate_scope(token)
            if scope != ValidationScope.DATASOURCES:
                raise fail(
                    node,
                    f"Creating is only supported for Datasources, got {scope}",
                )
        elif token.name == "CREATE_WITH_DATA":
            populate = True
        elif token.kind == SyntaxTokenKind.IDENTIFIER:
            targets.append(token.value)
    modifier = node.optional_node(SyntaxNodeKind.CREATE_MODIFIER_CLAUSE)
    if modifier is not None:
        mode = _parse_create_modifier(modifier)
    if populate and mode == CreateMode.CREATE_IF_NOT_EXISTS:
        # The load would run against a table that may already hold rows, so it
        # would double it. There is no "populate only if I just created it"
        # without probing the warehouse at parse time.
        raise fail(
            node,
            "`with data` cannot be combined with `if not exists` — the table may "
            "already hold rows. Use `create or replace ... with data` to rebuild "
            "it, or `append` to add to it.",
        )
    return CreateStatement(
        scope=scope, targets=targets, create_mode=mode, populate=populate
    )


def natural_select_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> NaturalSelectStatement:
    question: str | None = None
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.STRING_LITERAL:
            question = str(hydrate(child))
    if question is None:
        raise fail(node, "Natural select is missing its question string")
    return NaturalSelectStatement(question=question)


def validate_query_option(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> tuple[str, object]:
    identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
    if not identifiers:
        raise fail(node, "Validation option missing name")
    key = identifiers[0].value
    if len(identifiers) > 1:
        # bare-word value, e.g. `comparison = tolerant`
        return key, identifiers[1].value
    literal_nodes = node.child_nodes()
    if not literal_nodes:
        raise fail(node, f"Validation option '{key}' missing value")
    return key, hydrate(literal_nodes[0])


def validate_query_config(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> dict[str, object]:
    result: dict[str, object] = {}
    for child in node.child_nodes(SyntaxNodeKind.VALIDATE_QUERY_OPTION):
        hydrated = hydrate(child)
        if not isinstance(hydrated, tuple):
            raise fail(child, "Validation option failed to hydrate")
        key, value = hydrated
        if key in result:
            raise fail(child, f"Duplicate validation option '{key}'")
        result[key] = value
    return result


def _coerce_int(node: SyntaxNode, key: str, value: object, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise fail(node, f"Validation option '{key}' must be an integer")
    if value < minimum:
        raise fail(node, f"Validation option '{key}' must be >= {minimum}")
    return value


def _validate_natural_statement(
    node: SyntaxNode,
    natural: SyntaxNode,
    hydrate: HydrateFunction,
) -> ValidateNaturalStatement:
    query = hydrate(natural)
    if not isinstance(query, NaturalSelectStatement):
        raise fail(natural, "Natural select failed to hydrate")
    select_node = node.optional_node(SyntaxNodeKind.SELECT_STATEMENT)
    if select_node is None:
        raise fail(node, "validate ... matches is missing its expected select")
    expected = hydrate(select_node)
    if not isinstance(expected, SelectStatement):
        raise fail(select_node, "Expected select failed to hydrate")
    identifiers = node.child_tokens(SyntaxTokenKind.IDENTIFIER)
    name = identifiers[0].value if identifiers else None
    statement = ValidateNaturalStatement(query=query, expected=expected, name=name)
    config_node = node.optional_node(SyntaxNodeKind.VALIDATE_QUERY_CONFIG)
    if config_node is None:
        return statement
    config = hydrate(config_node)
    if not isinstance(config, dict):
        raise fail(config_node, "Validation config failed to hydrate")
    for key, value in config.items():
        if key == "repetitions":
            statement.repetitions = _coerce_int(config_node, key, value, minimum=1)
        elif key == "target":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise fail(config_node, "Validation option 'target' must be a number")
            if not 0.0 < float(value) <= 1.0:
                raise fail(
                    config_node,
                    "Validation option 'target' must be in (0.0, 1.0]",
                )
            statement.target = float(value)
        elif key == "comparison":
            if not isinstance(value, str):
                raise fail(
                    config_node,
                    "Validation option 'comparison' must be one of "
                    "tolerant, exact, ordered",
                )
            try:
                statement.comparison = QueryComparison(value)
            except ValueError:
                raise fail(
                    config_node,
                    f"Unknown comparison '{value}'; must be one of "
                    "tolerant, exact, ordered",
                ) from None
        elif key == "tags":
            # array literals hydrate to ListWrapper (a UserList), not list
            if not isinstance(value, Sequence) or isinstance(value, str):
                raise fail(
                    config_node, "Validation option 'tags' must be a list of strings"
                )
            tags = list(value)
            if not all(isinstance(t, str) for t in tags):
                raise fail(
                    config_node, "Validation option 'tags' must be a list of strings"
                )
            statement.tags = tags
        elif key == "timeout":
            statement.timeout = _coerce_int(config_node, key, value, minimum=1)
        else:
            raise fail(
                config_node,
                f"Unknown validation option '{key}'; supported: repetitions, "
                "target, comparison, tags, timeout",
            )
    return statement


def validate_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ValidateStatement | ValidateNaturalStatement:
    natural = node.optional_node(SyntaxNodeKind.NATURAL_SELECT_STATEMENT)
    if natural is not None:
        return _validate_natural_statement(node, natural, hydrate)
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
        elif (
            child.kind == SyntaxNodeKind.SELECT_STATEMENT
            or child.kind == SyntaxNodeKind.CHART_STATEMENT
        ):
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


# A call arg renders as `--<name> <value>`; names must be flag-safe.
_CALL_ARG_NAME = re.compile(r"[a-zA-Z][a-zA-Z0-9_]*")


def call_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> CallStatement:
    target: str | None = None
    select: SelectStatement | None = None
    file_path_kinds = (SyntaxTokenKind.FILE_PATH, SyntaxTokenKind.F_FILE_PATH)
    for token in node.child_tokens():
        if token.kind in file_path_kinds and target is None:
            target = str(hydrate(token))
    for child in node.child_nodes():
        if child.kind == SyntaxNodeKind.STRING_LITERAL and target is None:
            target = str(hydrate(child))
        elif child.kind == SyntaxNodeKind.SELECT_STATEMENT:
            hydrated = hydrate(child)
            if not isinstance(hydrated, SelectStatement):
                raise fail(child, "Call statement select failed to hydrate")
            select = hydrated
    if target is None:
        raise fail(node, "Malformed call statement: missing script target")
    if select is not None:
        seen: set[str] = set()
        for ref in select.output_components:
            if ref.address in select.hidden_components:
                continue
            name = ref.address.rsplit(".", 1)[-1]
            if not _CALL_ARG_NAME.fullmatch(name):
                raise fail(
                    node,
                    f"Call statement output '{name}' is not a valid argument name; "
                    "alias it to match [a-zA-Z][a-zA-Z0-9_]*.",
                )
            if name in seen:
                raise fail(
                    node,
                    f"Call statement has two outputs that both map to --{name}; "
                    "alias one to a distinct name.",
                )
            seen.add(name)
    return CallStatement(
        target=target, select=select, meta=metadata_from_meta(node.meta)
    )


OPERATIONAL_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.CREATE_MODIFIER_CLAUSE: create_modifier_clause,
    SyntaxNodeKind.CREATE_STATEMENT: create_statement,
    SyntaxNodeKind.VALIDATE_STATEMENT: validate_statement,
    SyntaxNodeKind.NATURAL_SELECT_STATEMENT: natural_select_statement,
    SyntaxNodeKind.VALIDATE_QUERY_OPTION: validate_query_option,
    SyntaxNodeKind.VALIDATE_QUERY_CONFIG: validate_query_config,
    SyntaxNodeKind.MOCK_STATEMENT: mock_statement,
    SyntaxNodeKind.PUBLISH_STATEMENT: publish_statement,
    SyntaxNodeKind.COPY_STATEMENT: copy_statement,
    SyntaxNodeKind.COPY_OPTION: copy_option,
    SyntaxNodeKind.COPY_OPTIONS: copy_options,
    SyntaxNodeKind.CALL_STATEMENT: call_statement,
}
