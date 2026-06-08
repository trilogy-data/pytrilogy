from __future__ import annotations

from trilogy.core.enums import Ordering
from trilogy.core.models.author import (
    Metadata,
    OrderBy,
    OrderItem,
    UndefinedConcept,
    UndefinedConceptFull,
)
from trilogy.core.statements.author import Limit
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import (
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
)


def order_by(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> OrderBy:
    args = hydrated_children(node, hydrate)
    return OrderBy(items=args[0])


def _order_identifier_expr(context: RuleContext, token: SyntaxToken):
    """Resolve an ORDER BY identifier to a reference, deferring unresolved names
    to an ``UndefinedConcept`` placeholder (carrying the token position) so
    ``finalize_select_statement`` can report every undefined order term at once
    instead of this rule raising on the first one."""
    address = token.value.strip()
    resolved = context.concepts.get(address)
    if resolved is not None and not isinstance(
        resolved, (UndefinedConcept, UndefinedConceptFull)
    ):
        return resolved.reference
    return UndefinedConcept(
        address=address,
        metadata=Metadata(line_number=token.line, column=token.column),
    )


def order_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[OrderItem]:
    # Children alternate (order term, ordering); a term is either an
    # ORDER_IDENTIFIER token or a general expr node.
    elems = node.children
    items: list[OrderItem] = []
    for i in range(0, len(elems), 2):
        term = elems[i]
        order = hydrate(elems[i + 1])
        if (
            isinstance(term, SyntaxToken)
            and term.kind == SyntaxTokenKind.ORDER_IDENTIFIER
        ):
            expr = _order_identifier_expr(context, term)
        else:
            hv = hydrate(term)
            expr = context.concepts.reference(hv) if isinstance(hv, str) else hv
        items.append(OrderItem(expr=expr, order=order))
    return items


def ordering(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Ordering:
    args = hydrated_children(node, hydrate)
    base = "asc"
    null_sort: str | None = None
    if args:
        first = str(args[0]).lower()
        if first in {"asc", "desc"}:
            base = first
            if len(args) > 1:
                null_sort = str(args[-1]).lower()
        else:
            null_sort = first
    if null_sort:
        return Ordering(" ".join([base, "nulls", null_sort]))
    return Ordering(base)


def limit_clause(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Limit:
    args = hydrated_children(node, hydrate)
    return Limit(count=int(str(args[0])))


ORDER_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.ORDER_BY: order_by,
    SyntaxNodeKind.ORDER_LIST: order_list,
    SyntaxNodeKind.ORDERING: ordering,
    SyntaxNodeKind.LIMIT: limit_clause,
}
