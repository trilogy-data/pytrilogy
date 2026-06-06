from __future__ import annotations

import operator as _operator
from typing import Any, Callable

from trilogy.constants import NULL_VALUE
from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.exceptions import InvalidComparison
from trilogy.core.models.author import (
    Between,
    Comparison,
    ConceptRef,
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
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    core_meta,
    fail,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind

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
    values = hydrated_children(node, hydrate)
    if len(values) == 1:
        return values[0]
    result = values[0]
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
            "||": FunctionType.CONCAT,
        }.get(op)
        if operator is None:
            raise fail(node, f"Unknown operator: {op}")
        result = context.function_factory.create_function(
            [result, right],
            operator=operator,
            meta=core_meta(node.meta),
        )
    return result


# Ordered/equality operators that treat an enum as its underlying scalar; each
# maps to the python function used to evaluate a literal against the domain.
# Keyed by the operator's symbol since ComparisonOperator is unhashable.
_ENUM_COMPARE_FUNCS: dict[str, Callable[[Any, Any], bool]] = {
    ComparisonOperator.EQ.value: _operator.eq,
    ComparisonOperator.NE.value: _operator.ne,
    ComparisonOperator.GT.value: _operator.gt,
    ComparisonOperator.GTE.value: _operator.ge,
    ComparisonOperator.LT.value: _operator.lt,
    ComparisonOperator.LTE.value: _operator.le,
}


def _enum_concept(side: Any) -> EnumType | None:
    if isinstance(side, ConceptRef) and isinstance(side.datatype, EnumType):
        return side.datatype
    return None


def _enum_constant_result(
    values: list[Any], predicate: Callable[[Any], Any]
) -> bool | None:
    """Classify a predicate over the enum domain: True if it holds for every member
    (tautology), False if for none (unsatisfiable), None if it discriminates."""
    try:
        results = [bool(predicate(v)) for v in values]
    except TypeError:
        # Incomparable literal vs domain types; leave it to normal type checking.
        return None
    if all(results):
        return True
    if not any(results):
        return False
    return None


def _literal_members(side: Any) -> list[Any] | None:
    if isinstance(side, (TupleWrapper, ListWrapper, tuple, list)):
        members = list(side)
        if members and all(isinstance(m, (str, int, float)) for m in members):
            return members
    return None


# Operators where a tautologically-true result is still a meaningful query: `=`
# and `in` express positive selection of valid members (e.g. picking one source of
# a single-member-enum union), so only flag them when they are *unsatisfiable*.
_TAUTOLOGY_OK = (ComparisonOperator.EQ.value, ComparisonOperator.IN.value)


def _enum_result_is_invalid(operator_value: str, result: bool | None) -> bool:
    if result is None:
        return False
    if result is False:
        return True  # unsatisfiable — can never match
    return operator_value not in _TAUTOLOGY_OK  # tautology — redundant noise


def _raise_enum_comparison(
    address: str, values: list[Any], subject: str, result: bool
) -> None:
    allowed = ", ".join(repr(v) for v in values)
    state = "True" if result else "False"
    raise InvalidComparison(
        f"{subject} is not valid for enum field '{address}'. This enum contains "
        f"only these values: {allowed}. This comparison will always be {state} "
        f"and should be removed."
    )


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
        _validate_enum_membership(left, right, operator)
        return SubselectComparison(left=left, right=right, operator=operator)
    _validate_enum_comparison(left, operator, right)
    return Comparison(left=left, right=right, operator=operator)


def _validate_enum_comparison(left: Any, operator: Any, right: Any) -> None:
    op_value = getattr(operator, "value", operator)
    func = _ENUM_COMPARE_FUNCS.get(op_value)
    if func is None:
        return
    for concept_side, value_side, concept_on_left in (
        (left, right, True),
        (right, left, False),
    ):
        enum = _enum_concept(concept_side)
        if enum is None or not isinstance(value_side, (str, int, float)):
            continue
        result = _enum_constant_result(
            enum.values,
            lambda v: func(v, value_side) if concept_on_left else func(value_side, v),
        )
        if _enum_result_is_invalid(op_value, result):
            _raise_enum_comparison(
                concept_side.address, enum.values, f"Value {value_side!r}", result
            )


def _validate_enum_membership(left: Any, right: Any, operator: Any) -> None:
    enum = _enum_concept(left)
    members = _literal_members(right)
    if enum is None or members is None:
        return
    if operator == ComparisonOperator.IN:
        result = _enum_constant_result(enum.values, lambda v: v in members)
    else:
        result = _enum_constant_result(enum.values, lambda v: v not in members)
    op_value = getattr(operator, "value", operator)
    if _enum_result_is_invalid(op_value, result):
        _raise_enum_comparison(left.address, enum.values, f"Set {members!r}", result)


def between_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Between:
    values = hydrated_children(node, hydrate)
    left, low, high = values[0], values[1], values[2]
    enum = _enum_concept(left)
    if (
        enum is not None
        and isinstance(low, (str, int, float))
        and isinstance(high, (str, int, float))
    ):
        result = _enum_constant_result(enum.values, lambda v: low <= v <= high)
        if result is not None:
            _raise_enum_comparison(
                left.address, enum.values, f"Range {low!r} to {high!r}", result
            )
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
