from __future__ import annotations

from typing import Any

from trilogy.constants import NULL_VALUE
from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.models.author import (
    Between,
    Comparison,
    Parenthetical,
    SubselectComparison,
)
from trilogy.core.models.core import (
    ListWrapper,
    MapWrapper,
    TupleWrapper,
    dict_to_map_wrapper,
    list_to_wrapper,
    tuple_to_wrapper,
)
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import (
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
)

_LIKE_OPERATORS = {
    "like": ComparisonOperator.LIKE,
    "ilike": ComparisonOperator.ILIKE,
    "not like": ComparisonOperator.NOT_LIKE,
    "not ilike": ComparisonOperator.NOT_ILIKE,
}


def int_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> int:
    return int("".join(str(hydrate(child)) for child in node.children))


def float_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> float:
    return float(hydrate(node.children[0]))


def bool_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> bool:
    return str(hydrate(node.children[0])).capitalize() == "True"


def null_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    return NULL_VALUE


def string_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> str:
    values = hydrated_children(node, hydrate)
    return values[0] if values else ""


def array_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ListWrapper:
    return list_to_wrapper(hydrated_children(node, hydrate))


def tuple_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> TupleWrapper:
    return tuple_to_wrapper(hydrated_children(node, hydrate))


def map_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> MapWrapper:
    values = hydrated_children(node, hydrate)
    return dict_to_map_wrapper(dict(zip(values[::2], values[1::2])))


def struct_lit(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    args = hydrated_children(node, hydrate)
    return context.function_factory.create_function(
        args, operator=FunctionType.STRUCT, meta=core_meta(node.meta)
    )


def literal(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    return hydrate(node.children[0])


def product_operator(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    values = hydrated_children(node, hydrate)
    if len(values) == 1:
        return values[0]
    result = values[0]
    for i in range(1, len(values), 2):
        op = str(values[i])
        right = values[i + 1]
        operator = {
            "*": FunctionType.MULTIPLY,
            "**": FunctionType.POWER,
            "/": FunctionType.DIVIDE,
            "%": FunctionType.MOD,
        }.get(op)
        if operator is None:
            raise fail(node, f"Unknown operator: {op}")
        result = context.function_factory.create_function(
            [result, right], operator=operator
        )
    return result


def sum_operator(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    children = node.children
    # A leading UNARY_MINUS is prefix negation on the first product_chain
    # (`-sum(x)`, `-col`). It binds only the first operand so precedence stays
    # `(-a) + b`. A negative numeric literal (`-1`) never reaches here — its
    # sign is part of the literal token.
    negate_leading = bool(
        children
        and isinstance(children[0], SyntaxToken)
        and children[0].kind == SyntaxTokenKind.UNARY_MINUS
    )
    if negate_leading:
        children = children[1:]
    values = [hydrate(child) for child in children]
    if len(values) == 1 and not negate_leading:
        return values[0]
    result = values[0]
    if negate_leading:
        result = context.function_factory.create_function(
            [result, -1], operator=FunctionType.MULTIPLY, meta=core_meta(node.meta)
        )
    for i in range(1, len(values), 2):
        op = " ".join(str(values[i]).lower().split())
        right = values[i + 1]
        # LIKE / ILIKE (and their negations) are binary boolean predicates —
        # emit them as ``Comparison`` so they slot into the proof and pushdown
        # machinery the same way other null-propagating comparisons do.
        like_op = _LIKE_OPERATORS.get(op)
        if like_op is not None:
            result = Comparison(left=result, right=right, operator=like_op)
            continue
        operator = {
            "+": FunctionType.ADD,
            "-": FunctionType.SUBTRACT,
            "||": FunctionType.CONCAT_STRICT,
        }.get(op)
        if operator is None:
            raise fail(node, f"Unknown operator: {op}")
        result = context.function_factory.create_function(
            [result, right],
            operator=operator,
            meta=core_meta(node.meta),
        )
    return result


def comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Any:
    children = list(node.children)
    if len(children) == 1:
        return hydrate(children[0])
    left = hydrate(children[0])
    operator = hydrate(children[1])
    membership = operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN)
    if membership:
        # A multi-output `(select ...)` on the RHS is a row-tuple membership set;
        # admit it here (scalar_subquery rejects it in every other position).
        with context.semantic_state.membership_subquery_scope():
            right = hydrate(children[2])
    else:
        right = hydrate(children[2])
    if membership:
        from trilogy.core.exceptions import InvalidSyntaxException
        from trilogy.core.models.author import SubqueryItem
        from trilogy.parsing.common import (
            resolve_subquery_membership,
            rewrite_composite_membership,
        )

        if isinstance(right, SubqueryItem):
            try:
                left, right = resolve_subquery_membership(left, right)
            except InvalidSyntaxException as e:
                raise fail(node, str(e)) from e
            return SubselectComparison(left=left, right=right, operator=operator)
        left, right = rewrite_composite_membership(left, right, operator)
        return SubselectComparison(left=left, right=right, operator=operator)
    return Comparison(left=left, right=right, operator=operator)


def between_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Between:
    values = hydrated_children(node, hydrate)
    left, low, high = values[0], values[1], values[2]
    return Between(left=left, low=low, high=high)


def parenthetical(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Parenthetical:
    return Parenthetical(content=hydrate(node.children[0]))


EXPRESSION_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.INT_LITERAL: int_lit,
    SyntaxNodeKind.FLOAT_LITERAL: float_lit,
    SyntaxNodeKind.BOOL_LITERAL: bool_lit,
    SyntaxNodeKind.NULL_LITERAL: null_lit,
    SyntaxNodeKind.STRING_LITERAL: string_lit,
    SyntaxNodeKind.ARRAY_LITERAL: array_lit,
    SyntaxNodeKind.TUPLE_LITERAL: tuple_lit,
    SyntaxNodeKind.MAP_LITERAL: map_lit,
    SyntaxNodeKind.STRUCT_LITERAL: struct_lit,
    SyntaxNodeKind.LITERAL: literal,
    SyntaxNodeKind.PRODUCT_OPERATOR: product_operator,
    SyntaxNodeKind.SUM_OPERATOR: sum_operator,
    SyntaxNodeKind.COMPARISON: comparison,
    SyntaxNodeKind.BETWEEN_COMPARISON: between_comparison,
    SyntaxNodeKind.PARENTHETICAL: parenthetical,
}
