from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.concept_syntax import (
    ConceptDeclarationSyntax,
    ConceptDerivationSyntax,
    ConceptPropertyDeclarationSyntax,
    ConstantDerivationSyntax,
    PropertyIdentifierSyntax,
    PropertyWildcardSyntax,
)
from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.rules.concept_rules import parse_concept_reference
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


def find_join_clause_literals(element: SyntaxElement) -> list[SyntaxNode]:
    """Return CONCEPT_LITERAL nodes that appear inside JOIN_CLAUSE subtrees.

    A scoped self-join's join condition (`cur.dow = nxt.dow`) references the
    other source's columns; those are not projected outputs but still surface as
    rowset-namespace forward refs, so callers compare them against the non-join
    literals to isolate pure join-key leaks."""
    result: list[SyntaxNode] = []
    stack: list[tuple[SyntaxElement, bool]] = [(element, False)]
    while stack:
        node, in_join = stack.pop()
        if not isinstance(node, SyntaxNode):
            continue
        if node.kind == SyntaxNodeKind.CONCEPT_LITERAL:
            if in_join:
                result.append(node)
            continue
        child_in_join = in_join or node.kind == SyntaxNodeKind.JOIN_CLAUSE
        for child in node.children:
            stack.append((child, child_in_join))
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


def find_tvf_output_names(element: SyntaxElement) -> list[str]:
    """Find the output names declared by a TVF signature `-> (name, ...)`.

    Each ``tvf_output_item`` leads with its name IDENTIFIER (grammar:
    ``select_hide_modifier? ~ IDENTIFIER ~ ...``). Unlike arm columns these
    names are renames that don't appear as concept literals, so they must be
    collected here for forward references to a `with X as union(...)` output.
    """
    result: list[str] = []
    stack: list[SyntaxElement] = [element]
    while stack:
        node = stack.pop()
        if isinstance(node, SyntaxNode):
            if node.kind == SyntaxNodeKind.TVF_OUTPUT_ITEM:
                for child in node.children:
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


@dataclass(frozen=True)
class ConceptAddress:
    """Where a concept block's address comes from.

    Most forms know their namespace outright. The property forms written as
    ``<parent_key_path>.<name>`` inherit their PARENT KEY's namespace
    (``sold_date.id.year`` -> ``sold_date.year``), so their address is not
    knowable until the parent's is — those carry ``parent_path`` instead and are
    resolved in parent-before-child order by
    :meth:`NativeHydrator._resolve_concept_addresses`.
    """

    name: str
    namespace: str | None = None
    parent_path: str | None = None

    def resolve(self, namespace: str) -> str:
        return _make_address(self.name, self.namespace or namespace)


def _property_declaration(path: str, environment: Environment) -> ConceptAddress:
    parent, _, name = path.rpartition(".")
    if not parent:
        return ConceptAddress(
            name=path, namespace=environment.namespace or DEFAULT_NAMESPACE
        )
    return ConceptAddress(name=name, parent_path=parent)


def parent_namespace(
    parent_path: str, environment: Environment, resolved: set[str]
) -> str | None:
    """Namespace of an already-resolved parent key, or None if not yet known.

    Reads the parent's real namespace from concepts resolved earlier in this
    pass or already committed by an import — the same source
    ``concept_property_declaration`` reads at hydration.
    """
    for candidate in (parent_path, f"{DEFAULT_NAMESPACE}.{parent_path}"):
        if candidate in resolved:
            return candidate.rpartition(".")[0] or DEFAULT_NAMESPACE
        existing = environment.concepts.data.get(candidate)
        if existing is not None:
            return existing.namespace or DEFAULT_NAMESPACE
    return None


def collect_concept_address(
    block: SyntaxNode, environment: Environment
) -> ConceptAddress | None:
    """Extract a concept block's address without modifying the environment.

    Returns None for parameter/properties declarations, which provide their
    addresses through :func:`collect_properties_addresses` instead.
    """
    inner = _get_concept_inner_node(block)
    kind = inner.kind

    if kind == SyntaxNodeKind.CONCEPT_DECLARATION:
        decl_syntax = ConceptDeclarationSyntax.from_node(inner)
        _, namespace, name, _ = parse_concept_reference(
            decl_syntax.name.value, environment
        )
        return ConceptAddress(name=name, namespace=namespace)

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        derivation_syntax = ConceptDerivationSyntax.from_node(inner)
        raw_name = derivation_syntax.name
        if isinstance(raw_name, SyntaxToken):
            name_value = raw_name.value
            if (
                derivation_syntax.purpose.value.lower() == "property"
                and "." in name_value
            ):
                return _property_declaration(name_value, environment)
            _, namespace, name_str, _ = parse_concept_reference(name_value, environment)
            return ConceptAddress(name=name_str, namespace=namespace)
        if (
            isinstance(raw_name, SyntaxNode)
            and raw_name.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER
        ):
            property_id = PropertyIdentifierSyntax.from_node(raw_name)
            namespace = environment.namespace or DEFAULT_NAMESPACE
            return ConceptAddress(name=property_id.name.value, namespace=namespace)
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
        return ConceptAddress(name=name_str, namespace=namespace)

    if kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        property_syntax = ConceptPropertyDeclarationSyntax.from_node(inner)
        decl = property_syntax.declaration
        namespace = environment.namespace or DEFAULT_NAMESPACE
        if (
            isinstance(decl, SyntaxNode)
            and decl.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER
        ):
            property_id = PropertyIdentifierSyntax.from_node(decl)
            return ConceptAddress(name=property_id.name.value, namespace=namespace)
        if (
            isinstance(decl, SyntaxNode)
            and decl.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER_WILDCARD
        ):
            wildcard = PropertyWildcardSyntax.from_node(decl)
            return ConceptAddress(name=wildcard.name.value, namespace=namespace)
        if isinstance(decl, SyntaxToken):
            return _property_declaration(decl.value, environment)
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
    for child in inner.child_nodes(SyntaxNodeKind.INLINE_PROPERTY_LIST):
        for prop in child.child_nodes(SyntaxNodeKind.INLINE_PROPERTY):
            for token in prop.child_tokens(SyntaxTokenKind.IDENTIFIER):
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
    concept_plans: list[ConceptStatementPlan],
    environment: Environment,
) -> list[ConceptStatementPlan]:
    """Sort concept plans so dependencies are hydrated first."""
    if not concept_plans:
        return []

    addr_to_plan: dict[str, ConceptStatementPlan] = {}
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

    ordered: list[ConceptStatementPlan] = []
    while queue:
        pid = queue.popleft()
        ordered.append(plan_ids[pid])
        for dependent in forward.get(pid, []):
            in_deg[dependent] -= 1
            if in_deg[dependent] == 0:
                queue.append(dependent)

    seen = {id(p) for p in ordered}
    for plan in concept_plans:
        if id(plan) not in seen:
            ordered.append(plan)

    return ordered
