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
    from trilogy.core.models.core import (
        DataType,
        TupleWrapper,
        arg_to_datatype,
        reduce_tuple_element_datatypes,
    )

    # A merged element type only makes sense for a value-list tuple; a composite
    # (row) membership tuple compares position-wise and may legitimately mix
    # types, so defer validation to SubselectComparison (per-position for row
    # tuples, per-element-vs-left for scalar value lists).
    args = hydrated_children(node, hydrate)
    datatypes = [arg_to_datatype(x) for x in args]
    try:
        dtype, nullable = reduce_tuple_element_datatypes(datatypes)
    except ValueError:
        dtype = DataType.UNKNOWN
        nullable = any(d == DataType.NULL for d in datatypes)
    return TupleWrapper(val=tuple(args), type=dtype, nullable=nullable)


def subselect_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    from trilogy.core.enums import FunctionType
    from trilogy.core.exceptions import InvalidSyntaxException
    from trilogy.core.models.author import (
        AggregateWrapper,
        Concept,
        FilterItem,
        Function,
        Parenthetical,
        SubqueryItem,
        SubselectComparison,
        WindowItem,
    )
    from trilogy.core.models.core import ListWrapper, TupleWrapper
    from trilogy.parsing.common import (
        resolve_subquery_membership,
        rewrite_composite_membership,
    )
    from trilogy.parsing.v2.concept_factory import arbitrary_to_concept_v2

    # The final child is the RHS set/value; a multi-output `(select ...)` there
    # is a row-tuple membership set (admissible only in this position).
    children = list(node.children)
    args = []
    for idx, child in enumerate(children):
        if idx == len(children) - 1:
            with context.semantic_state.membership_subquery_scope():
                args.append(hydrate(child))
        else:
            args.append(hydrate(child))
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
    if isinstance(right, SubqueryItem):
        try:
            left, right = resolve_subquery_membership(left, right)
        except InvalidSyntaxException as e:
            raise fail(node, str(e)) from e
        return SubselectComparison(left=left, right=right, operator=operator)
    left, right = rewrite_composite_membership(left, right, operator)
    # a ROW_TUPLE operand is a composite-membership row constructor, not a
    # function to be lifted into its own concept
    if isinstance(right, (Function, FilterItem, WindowItem, AggregateWrapper)) and not (
        isinstance(right, Function) and right.operator == FunctionType.ROW_TUPLE
    ):
        right_concept = arbitrary_to_concept_v2(right, context=context)
        context.add_virtual_concept(right_concept, meta=core_meta(node.meta))
        right = right_concept.reference
    return SubselectComparison(left=left, right=right, operator=operator)


SUBSELECT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.EXPR_TUPLE: expr_tuple,
    SyntaxNodeKind.SUBSELECT_COMPARISON: subselect_comparison,
}
