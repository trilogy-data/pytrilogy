from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.concept_rules import parse_concept_reference
from trilogy.parsing.v2.concept_syntax import (
    ConceptDeclarationSyntax,
    ConceptDerivationSyntax,
    ConceptPropertyDeclarationSyntax,
    ConstantDerivationSyntax,
    PropertyIdentifierSyntax,
    PropertyWildcardSyntax,
)
from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.syntax import (
    SyntaxElement,
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
)

if TYPE_CHECKING:
    from trilogy.parsing.v2.statement_plans import ConceptStatementPlan

_CONCEPT_INNER_KINDS = {
    SyntaxNodeKind.CONCEPT_DECLARATION,
    SyntaxNodeKind.CONCEPT_DERIVATION,
    SyntaxNodeKind.CONSTANT_DERIVATION,
    SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION,
    SyntaxNodeKind.PARAMETER_DECLARATION,
    SyntaxNodeKind.PROPERTIES_DECLARATION,
}


def find_concept_literals(element: SyntaxElement) -> list[SyntaxNode]:
    """Walk a syntax tree and return all CONCEPT_LITERAL nodes."""
    result: list[SyntaxNode] = []
    stack: list[SyntaxElement] = [element]
    while stack:
        node = stack.pop()
        if isinstance(node, SyntaxNode):
            if node.kind == SyntaxNodeKind.CONCEPT_LITERAL:
                result.append(node)
            else:
                stack.extend(node.children)
    return result


def extract_concept_name_from_literal(node: SyntaxNode, namespace: str) -> str:
    """Extract the fully-qualified concept address from a CONCEPT_LITERAL node."""
    if not node.children or not isinstance(node.children[0], SyntaxToken):
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Concept literal requires a leading identifier token", node
            )
        )
    name = node.children[0].value
    if "." not in name and namespace == DEFAULT_NAMESPACE:
        name = f"{DEFAULT_NAMESPACE}.{name}"
    return name


def find_select_transform_targets(element: SyntaxElement) -> list[str]:
    """Find all concept names created by `expr -> name` in SELECT projections."""
    result: list[str] = []
    stack: list[SyntaxElement] = [element]
    while stack:
        node = stack.pop()
        if isinstance(node, SyntaxNode):
            if node.kind == SyntaxNodeKind.SELECT_TRANSFORM:
                for child in reversed(node.children):
                    if (
                        isinstance(child, SyntaxToken)
                        and child.kind == SyntaxTokenKind.IDENTIFIER
                    ):
                        result.append(child.value)
                        break
            else:
                stack.extend(node.children)
    return result


def collect_inline_concept_addresses(
    element: SyntaxElement, namespace: str
) -> list[str]:
    """Extract addresses of concepts created inline via `-> name` in SELECT statements."""
    names = find_select_transform_targets(element)
    return [_make_address(n, namespace) for n in names]


def _get_concept_inner_node(block: SyntaxNode) -> SyntaxNode:
    """Get the inner concept node (declaration/derivation/etc) from a BLOCK > CONCEPT."""
    if not block.children:
        raise HydrationError(
            HydrationDiagnostic.from_syntax("Concept block is empty", block)
        )
    statement = block.children[0]
    if (
        not isinstance(statement, SyntaxNode)
        or statement.kind != SyntaxNodeKind.CONCEPT
    ):
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Expected CONCEPT node inside concept block", block
            )
        )
    nodes = [
        c
        for c in statement.children
        if isinstance(c, SyntaxNode) and c.kind in _CONCEPT_INNER_KINDS
    ]
    if len(nodes) != 1:
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                f"Concept block expects a single inner declaration, found {len(nodes)}",
                statement,
            )
        )
    return nodes[0]


def _make_address(name: str, namespace: str) -> str:
    return f"{namespace}.{name}"


def collect_concept_address(block: SyntaxNode, environment: Environment) -> str | None:
    """Extract the concept address from a block without modifying the environment.

    Returns the concept address, or None for parameter/properties declarations.
    """
    inner = _get_concept_inner_node(block)
    kind = inner.kind

    if kind == SyntaxNodeKind.CONCEPT_DECLARATION:
        decl_syntax = ConceptDeclarationSyntax.from_node(inner)
        _, namespace, name, _ = parse_concept_reference(
            decl_syntax.name.value, environment
        )
        return _make_address(name, namespace)

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        derivation_syntax = ConceptDerivationSyntax.from_node(inner)
        raw_name = derivation_syntax.name
        if isinstance(raw_name, SyntaxToken):
            name_value = raw_name.value
            if (
                derivation_syntax.purpose.value.lower() == "property"
                and "." in name_value
            ):
                parent, short_name = name_value.rsplit(".", 1)
                if "." in parent:
                    namespace = parent.rsplit(".", 1)[0]
                else:
                    namespace = environment.namespace or DEFAULT_NAMESPACE
                return _make_address(short_name, namespace)
            _, namespace, name_str, _ = parse_concept_reference(name_value, environment)
            return _make_address(name_str, namespace)
        if (
            isinstance(raw_name, SyntaxNode)
            and raw_name.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER
        ):
            property_id = PropertyIdentifierSyntax.from_node(raw_name)
            namespace = environment.namespace or DEFAULT_NAMESPACE
            return _make_address(property_id.name.value, namespace)
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Concept derivation name must be an identifier or property identifier",
                raw_name,
            )
        )

    if kind == SyntaxNodeKind.CONSTANT_DERIVATION:
        const_syntax = ConstantDerivationSyntax.from_node(inner)
        _, namespace, name_str, _ = parse_concept_reference(
            const_syntax.name.value, environment
        )
        return _make_address(name_str, namespace)

    if kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        property_syntax = ConceptPropertyDeclarationSyntax.from_node(inner)
        decl = property_syntax.declaration
        namespace = environment.namespace or DEFAULT_NAMESPACE
        if (
            isinstance(decl, SyntaxNode)
            and decl.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER
        ):
            property_id = PropertyIdentifierSyntax.from_node(decl)
            return _make_address(property_id.name.value, namespace)
        if (
            isinstance(decl, SyntaxNode)
            and decl.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER_WILDCARD
        ):
            wildcard = PropertyWildcardSyntax.from_node(decl)
            return _make_address(wildcard.name.value, namespace)
        if isinstance(decl, SyntaxToken):
            raw = decl.value
            short = raw.rsplit(".", 1)[-1] if "." in raw else raw
            return _make_address(short, namespace)
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Property declaration target must be a property identifier or token",
                decl,
            )
        )

    return None


def collect_properties_addresses(
    block: SyntaxNode, environment: Environment
) -> list[str]:
    """Extract all concept addresses from a PROPERTIES_DECLARATION block."""
    inner = _get_concept_inner_node(block)
    if inner.kind != SyntaxNodeKind.PROPERTIES_DECLARATION:
        return []
    namespace = environment.namespace or DEFAULT_NAMESPACE
    result: list[str] = []
    for child in inner.children:
        if (
            isinstance(child, SyntaxNode)
            and child.kind == SyntaxNodeKind.INLINE_PROPERTY_LIST
        ):
            for prop in child.children:
                if (
                    isinstance(prop, SyntaxNode)
                    and prop.kind == SyntaxNodeKind.INLINE_PROPERTY
                ):
                    for token in prop.children:
                        if (
                            isinstance(token, SyntaxToken)
                            and token.kind == SyntaxTokenKind.IDENTIFIER
                        ):
                            result.append(_make_address(token.value, namespace))
                            break
    return result


def extract_dependencies(block: SyntaxNode, environment: Environment) -> list[str]:
    """Find all concept addresses referenced in a concept block's source expression."""
    inner = _get_concept_inner_node(block)
    kind = inner.kind
    namespace = environment.namespace or DEFAULT_NAMESPACE

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        syntax = ConceptDerivationSyntax.from_node(inner)
        literals = find_concept_literals(syntax.source)
        if isinstance(syntax.name, SyntaxNode):
            literals.extend(find_concept_literals(syntax.name))
    elif kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        syntax_prop = ConceptPropertyDeclarationSyntax.from_node(inner)
        literals = find_concept_literals(syntax_prop.declaration)
    elif kind == SyntaxNodeKind.CONSTANT_DERIVATION:
        syntax_const = ConstantDerivationSyntax.from_node(inner)
        literals = find_concept_literals(syntax_const.source)
    elif kind == SyntaxNodeKind.PROPERTIES_DECLARATION:
        deps: list[str] = []
        for child in inner.children:
            if (
                isinstance(child, SyntaxNode)
                and child.kind == SyntaxNodeKind.PROP_IDENT_LIST
            ):
                for token in child.children:
                    if (
                        isinstance(token, SyntaxToken)
                        and token.kind == SyntaxTokenKind.IDENTIFIER
                    ):
                        name = token.value
                        if "." not in name:
                            name = f"{namespace}.{name}"
                        deps.append(name)
        return deps
    else:
        return []

    return [extract_concept_name_from_literal(lit, namespace) for lit in literals]


def topological_sort_plans(
    concept_plans: list["ConceptStatementPlan"],
    environment: Environment,
) -> list["ConceptStatementPlan"]:
    """Sort concept plans so dependencies are hydrated first."""
    if not concept_plans:
        return []

    addr_to_plan: dict[str, "ConceptStatementPlan"] = {}
    for plan in concept_plans:
        for addr in plan.provided_addresses:
            addr_to_plan[addr] = plan

    plan_ids = {id(p): p for p in concept_plans}

    dep_graph: dict[int, set[int]] = {id(p): set() for p in concept_plans}
    for plan in concept_plans:
        for dep_addr in plan.dependencies:
            dep_plan = addr_to_plan.get(dep_addr)
            if dep_plan is not None and id(dep_plan) != id(plan):
                dep_graph[id(plan)].add(id(dep_plan))

    forward: dict[int, list[int]] = defaultdict(list)
    in_deg: dict[int, int] = {}
    for pid, deps in dep_graph.items():
        in_deg[pid] = len(deps)
        for dep_pid in deps:
            forward[dep_pid].append(pid)

    queue: deque[int] = deque(pid for pid, deg in in_deg.items() if deg == 0)

    ordered: list["ConceptStatementPlan"] = []
    while queue:
        pid = queue.popleft()
        ordered.append(plan_ids[pid])
        for dependent in forward.get(pid, []):
            in_deg[dependent] -= 1
            if in_deg[dependent] == 0:
                queue.append(dependent)

    seen = set(id(p) for p in ordered)
    for plan in concept_plans:
        if id(plan) not in seen:
            ordered.append(plan)

    return ordered
