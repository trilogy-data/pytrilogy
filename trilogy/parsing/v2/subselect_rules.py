from __future__ import annotations

from typing import Any

from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def expr_tuple(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.models.core import DataType, TupleWrapper, arg_to_datatype

    args = hydrated_children(node, hydrate)
    datatypes = set(arg_to_datatype(x) for x in args)
    if len(datatypes) != 1:
        raise fail(node, "Tuple must have same type for all elements")
    dtype = datatypes.pop()
    if not isinstance(dtype, DataType):
        raise fail(node, f"Tuple element type {dtype} is not a base DataType")
    return TupleWrapper(val=tuple(args), type=dtype)


def subselect_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.models.author import (
        AggregateWrapper,
        Concept,
        FilterItem,
        Function,
        Parenthetical,
        SubselectComparison,
        WindowItem,
    )
    from trilogy.core.models.core import ListWrapper, TupleWrapper
    from trilogy.parsing.common import arbitrary_to_concept

    args = hydrated_children(node, hydrate)
    left = args[0]
    operator = args[1]
    right = args[2]
    while isinstance(right, Parenthetical) and isinstance(
        right.content,
        (
            Concept,
            Function,
            FilterItem,
            WindowItem,
            AggregateWrapper,
            ListWrapper,
            TupleWrapper,
        ),
    ):
        right = right.content
    if isinstance(right, (Function, FilterItem, WindowItem, AggregateWrapper)):
        right_concept = arbitrary_to_concept(right, environment=context.environment)
        context.add_virtual_concept(right_concept, meta=core_meta(node.meta))
        right = right_concept.reference
    return SubselectComparison(left=left, right=right, operator=operator)


SUBSELECT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.EXPR_TUPLE: expr_tuple,
    SyntaxNodeKind.SUBSELECT_COMPARISON: subselect_comparison,
}
