from __future__ import annotations

import operator as _operator
import re
from typing import Any, Callable, Literal

from trilogy.constants import NULL_VALUE
from trilogy.core.enums import ComparisonOperator, FunctionType, Modifier
from trilogy.core.exceptions import InvalidComparison
from trilogy.core.models.author import (
    Between,
    Comparison,
    ConceptRef,
    Expr,
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
            validate_enum_like(result, like_op, right, context)
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


# Literal types that can be compared against an enum's underlying scalar domain.
EnumLiteral = int | str | float

# How a constant enum comparison fails the meaningfulness contract.
EnumViolation = Literal["unsatisfiable", "tautology", "nullable_tautology"]

# Ordered/equality operators that treat an enum as its underlying scalar; each
# maps to the python function used to evaluate a literal against the domain.
# Keyed by the operator's symbol since ComparisonOperator is unhashable.
_ENUM_COMPARE_FUNCS: dict[str, Callable[..., bool]] = {
    ComparisonOperator.EQ.value: _operator.eq,
    ComparisonOperator.NE.value: _operator.ne,
    ComparisonOperator.GT.value: _operator.gt,
    ComparisonOperator.GTE.value: _operator.ge,
    ComparisonOperator.LT.value: _operator.lt,
    ComparisonOperator.LTE.value: _operator.le,
}


def _enum_field(
    side: Expr, context: RuleContext
) -> tuple[ConceptRef, EnumType, bool] | None:
    """If ``side`` references an enum concept, return it with its enum type and
    declared nullability (NULL is part of the satisfiability domain when nullable)."""
    if isinstance(side, ConceptRef) and isinstance(side.datatype, EnumType):
        concept = context.concepts.get(side.address)
        nullable = concept is not None and Modifier.NULLABLE in concept.modifiers
        return side, side.datatype, nullable
    return None


def _enum_constant_result(
    values: list[object], predicate: Callable[[object], bool]
) -> bool | None:
    """Classify a predicate over the (non-null) enum members: True if it holds for
    every member, False if for none, None if it discriminates between members."""
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


def _literal_members(side: Expr) -> list[EnumLiteral] | None:
    if isinstance(side, (TupleWrapper, ListWrapper, tuple, list)):
        members = [m for m in side if isinstance(m, (int, str, float))]
        if members and len(members) == len(side):
            return members
    return None


def _enum_violation(result: bool | None, nullable: bool) -> EnumViolation | None:
    """Map a constant result over the members to the kind of meaningless comparison,
    or None if it discriminates. A comparison matching every member only narrows away
    NULLs when the field is nullable, so it is equivalent to ``is not null`` there."""
    if result is None:
        return None
    if result is False:
        return "unsatisfiable"  # no member matches, and NULL never matches either
    return "nullable_tautology" if nullable else "tautology"


def _raise_enum_comparison(
    field: ConceptRef, enum: EnumType, comparison: str, violation: EnumViolation
) -> None:
    allowed = ", ".join(repr(v) for v in enum.values)
    address = field.address
    if violation == "unsatisfiable":
        body = (
            f"can never match enum field '{address}', which contains only these "
            f"values: {allowed}. It is always false and should be removed."
        )
    elif violation == "tautology":
        body = (
            f"matches every value of enum field '{address}', which contains only "
            f"these values: {allowed}. It is always true and should be removed."
        )
    else:
        body = (
            f"matches every value of nullable enum field '{address}', which contains "
            f"only these values: {allowed}. It only excludes nulls; simplify it to "
            f"'{address} is not null'."
        )
    raise InvalidComparison(f"Comparison `{comparison}` {body}")


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
        _validate_enum_membership(left, right, operator, context)
        return SubselectComparison(left=left, right=right, operator=operator)
    _validate_enum_comparison(left, operator, right, context)
    return Comparison(left=left, right=right, operator=operator)


def _validate_enum_comparison(
    left: Expr, operator: ComparisonOperator, right: Expr, context: RuleContext
) -> None:
    func = _ENUM_COMPARE_FUNCS.get(operator.value)
    if func is None:
        return
    for concept_side, value_side, concept_on_left in (
        (left, right, True),
        (right, left, False),
    ):
        field = _enum_field(concept_side, context)
        if field is None or not isinstance(value_side, (int, str, float)):
            continue
        ref, enum, nullable = field
        result = _enum_constant_result(
            enum.values,
            lambda v: func(v, value_side) if concept_on_left else func(value_side, v),
        )
        violation = _enum_violation(result, nullable)
        if violation is not None:
            if concept_on_left:
                rendered = f"{ref.address} {operator.value} {value_side!r}"
            else:
                rendered = f"{value_side!r} {operator.value} {ref.address}"
            _raise_enum_comparison(ref, enum, rendered, violation)


def _validate_enum_membership(
    left: Expr, right: Expr, operator: ComparisonOperator, context: RuleContext
) -> None:
    field = _enum_field(left, context)
    members = _literal_members(right)
    if field is None or members is None:
        return
    ref, enum, nullable = field
    if operator == ComparisonOperator.IN:
        result = _enum_constant_result(enum.values, lambda v: v in members)
    else:
        result = _enum_constant_result(enum.values, lambda v: v not in members)
    violation = _enum_violation(result, nullable)
    if violation is not None:
        listed = ", ".join(repr(m) for m in members)
        rendered = f"{ref.address} {operator.value} ({listed})"
        _raise_enum_comparison(ref, enum, rendered, violation)


_LIKE_NEGATED = (ComparisonOperator.NOT_LIKE, ComparisonOperator.NOT_ILIKE)
_LIKE_INSENSITIVE = (ComparisonOperator.ILIKE, ComparisonOperator.NOT_ILIKE)


def _like_pattern_to_regex(pattern: str, ignorecase: bool) -> re.Pattern[str]:
    body = "".join(
        ".*" if ch == "%" else "." if ch == "_" else re.escape(ch) for ch in pattern
    )
    flags = re.DOTALL | (re.IGNORECASE if ignorecase else 0)
    return re.compile(f"^{body}$", flags)


def validate_enum_like(
    left: Expr, operator: ComparisonOperator, right: Expr, context: RuleContext
) -> None:
    """Flag LIKE/ILIKE (and negations) whose pattern matches every / no member of a
    string enum. Public so both the infix and ``like()`` call sites can reuse it."""
    field = _enum_field(left, context)
    if field is None or not isinstance(right, str):
        return
    ref, enum, nullable = field
    if not all(isinstance(v, str) for v in enum.values):
        return
    regex = _like_pattern_to_regex(right, operator in _LIKE_INSENSITIVE)
    negate = operator in _LIKE_NEGATED
    result = _enum_constant_result(
        enum.values,
        lambda v: isinstance(v, str)
        and ((regex.match(v) is None) if negate else (regex.match(v) is not None)),
    )
    violation = _enum_violation(result, nullable)
    if violation is not None:
        rendered = f"{ref.address} {operator.value} {right!r}"
        _raise_enum_comparison(ref, enum, rendered, violation)


def between_comparison(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> Between:
    values = hydrated_children(node, hydrate)
    left, low, high = values[0], values[1], values[2]
    field = _enum_field(left, context)
    if (
        field is not None
        and isinstance(low, (int, str, float))
        and isinstance(high, (int, str, float))
    ):
        ref, enum, nullable = field
        le = _ENUM_COMPARE_FUNCS[ComparisonOperator.LTE.value]
        result = _enum_constant_result(
            enum.values, lambda v: le(low, v) and le(v, high)
        )
        violation = _enum_violation(result, nullable)
        if violation is not None:
            rendered = f"{ref.address} between {low!r} and {high!r}"
            _raise_enum_comparison(ref, enum, rendered, violation)
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
