from __future__ import annotations

from trilogy.core.enums import Ordering
from trilogy.core.models.author import OrderBy, OrderItem
from trilogy.core.statements.author import Limit
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def order_by(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> OrderBy:
    args = hydrated_children(node, hydrate)
    return OrderBy(items=args[0])


def order_list(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[OrderItem]:
    args = hydrated_children(node, hydrate)
    return [
        OrderItem(
            expr=(context.concepts.reference(str(x)) if isinstance(x, str) else x),
            order=y,
        )
        for x, y in zip(args[::2], args[1::2])
    ]


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
