from __future__ import annotations

from typing import Any

from trilogy.constants import NULL_VALUE
from trilogy.core.enums import BooleanOperator, ComparisonOperator, FunctionType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    Comparison,
    ConceptRef,
    Conditional,
    Parenthetical,
    SubselectComparison,
)
from trilogy.core.models.core import (
    EnumType,
    ListWrapper,
    MapWrapper,
    TupleWrapper,
    dict_to_map_wrapper,
    list_to_wrapper,
    tuple_to_wrapper,
)
from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind


def hydrated_children(
    node: SyntaxNode,
    hydrate: HydrateFunction,
) -> list[Any]:
    return [hydrate(child) for child in node.children]


def fail(node: SyntaxNode, message: str) -> HydrationError:
    return HydrationError(HydrationDiagnostic.from_syntax(message, node))


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
    values = hydrated_children(node, hydrate)
    if len(values) == 1:
        return values[0]
    result = values[0]
    for i in range(1, len(values), 2):
        op = str(values[i]).lower()
        right = values[i + 1]
        operator = {
            "+": FunctionType.ADD,
            "-": FunctionType.SUBTRACT,
            "||": FunctionType.CONCAT,
            "like": FunctionType.LIKE,
            "ilike": FunctionType.ILIKE,
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
    values = hydrated_children(node, hydrate)
    if len(values) == 1:
        return values[0]
    left = values[0]
    operator = values[1]
    right = values[2]
    if operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
        return SubselectComparison(left=left, right=right, operator=operator)
    for concept_side, value_side in ((left, right), (right, left)):
        if isinstance(concept_side, ConceptRef) and isinstance(
            concept_side.datatype,
            EnumType,
        ):
            if (
                isinstance(value_side, (str, int))
                and value_side not in concept_side.datatype.values
            ):
                raise InvalidSyntaxException(
                    f"Value {value_side!r} is not a valid member of enum {concept_side.datatype} for '{concept_side.address}'"
                )
    return Comparison(left=left, right=right, operator=operator)


def between_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Conditional:
    values = hydrated_children(node, hydrate)
    return Conditional(
        left=Comparison(
            left=values[0], right=values[1], operator=ComparisonOperator.GTE
        ),
        right=Comparison(
            left=values[0], right=values[2], operator=ComparisonOperator.LTE
        ),
        operator=BooleanOperator.AND,
    )


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
    SyntaxNodeKind.LITERAL: literal,
    SyntaxNodeKind.PRODUCT_OPERATOR: product_operator,
    SyntaxNodeKind.SUM_OPERATOR: sum_operator,
    SyntaxNodeKind.COMPARISON: comparison,
    SyntaxNodeKind.BETWEEN_COMPARISON: between_comparison,
    SyntaxNodeKind.PARENTHETICAL: parenthetical,
}
