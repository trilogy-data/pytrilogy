from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from typing import Any, TypeVar

from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    DatePart,
    FunctionClass,
    FunctionType,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildFilterItem,
    BuildFunction,
    BuildParenthetical,
    BuildSubselectComparison,
    BuildWhereClause,
    BuildWindowItem,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    EnumType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    TraitDataType,
    TupleWrapper,
)

AGGREGATE_TYPES = (BuildAggregateWrapper,)
SUBSELECT_TYPES = (BuildSubselectComparison,)
COMPARISON_TYPES = (BuildComparison,)
FUNCTION_TYPES = (BuildFunction,)
PARENTHETICAL_TYPES = (BuildParenthetical,)
CONDITIONAL_TYPES = (BuildConditional,)
CONCEPT_TYPES = (BuildConcept,)
WINDOW_TYPES = (BuildWindowItem,)

CONDITION_TYPES = (
    BuildSubselectComparison,
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
)

_T = TypeVar("_T", int, float, date, datetime)

TARGET_TYPES = (int, date, float, datetime, bool, str)
REDUCABLE_TYPES = (int, float, date, bool, datetime, str, BuildFunction)


def _is_tautology(node: BuildComparison) -> bool:
    """True for comparisons that always evaluate to true (e.g. True IS True)."""
    return (
        node.left == node.right
        and isinstance(node.left, bool)
        and node.operator in (ComparisonOperator.EQ, ComparisonOperator.IS)
    )


def _unwrap_condition_boolean_wrapper(
    conditional: BuildComparison,
) -> BuildComparison | BuildConditional | BuildParenthetical | BuildSubselectComparison:
    """Collapse redundant wrappers like ``(<condition>) = True``.

    The parser can wrap parenthesized boolean expressions in an equality-to-True
    comparison when materializing a WHERE clause. Downstream routing logic treats
    conditions atomically, so normalize that form back to the original condition.
    """
    if conditional.operator not in (ComparisonOperator.EQ, ComparisonOperator.IS):
        return conditional

    if conditional.right is True and isinstance(conditional.left, CONDITION_TYPES):
        return flatten_conditions(conditional.left)
    if conditional.left is True and isinstance(conditional.right, CONDITION_TYPES):
        return flatten_conditions(conditional.right)
    return conditional


def flatten_conditions(
    conditional: BuildComparison | BuildConditional | BuildParenthetical,
) -> BuildComparison | BuildConditional | BuildParenthetical:
    """Simplify a condition tree.

    - Unwraps parentheticals around leaf comparisons/subselects (not around
      conditionals, where parens may enforce OR-vs-AND precedence).
    - Drops tautologies (e.g. True IS True) from AND chains.
    """
    if isinstance(conditional, BuildParenthetical) and isinstance(
        conditional.content,
        (BuildComparison, BuildSubselectComparison, BuildParenthetical),
    ):
        return flatten_conditions(conditional.content)
    if isinstance(conditional, BuildComparison):
        unwrapped = _unwrap_condition_boolean_wrapper(conditional)
        if unwrapped is not conditional:
            return unwrapped
    if isinstance(conditional, BuildConditional):
        left = conditional.left
        right = conditional.right
        new_left = (
            flatten_conditions(left) if isinstance(left, CONDITION_TYPES) else left
        )
        new_right = (
            flatten_conditions(right) if isinstance(right, CONDITION_TYPES) else right
        )
        # Drop tautologies from AND chains
        if conditional.operator == BooleanOperator.AND:
            left_taut = isinstance(new_left, BuildComparison) and _is_tautology(
                new_left
            )
            right_taut = isinstance(new_right, BuildComparison) and _is_tautology(
                new_right
            )
            if left_taut and right_taut:
                assert isinstance(new_left, BuildComparison)
                return new_left  # both trivial, return either
            if left_taut:
                return new_right if isinstance(new_right, CONDITION_TYPES) else conditional  # type: ignore
            if right_taut:
                return new_left if isinstance(new_left, CONDITION_TYPES) else conditional  # type: ignore
        if new_left is not left or new_right is not right:
            return BuildConditional(
                left=new_left, right=new_right, operator=conditional.operator
            )
    return conditional


def is_scalar_condition(
    element: (
        int
        | str
        | float
        | date
        | datetime
        | list[Any]
        | BuildConcept
        | BuildWindowItem
        | BuildFilterItem
        | BuildConditional
        | BuildComparison
        | BuildParenthetical
        | BuildFunction
        | BuildAggregateWrapper
        | BuildCaseWhen
        | BuildCaseElse
        | MagicConstants
        | TraitDataType
        | DataType
        | MapWrapper[Any, Any]
        | ArrayType
        | MapType
        | NumericType
        | DatePart
        | ListWrapper[Any]
        | TupleWrapper[Any]
    ),
    materialized: set[str] | None = None,
) -> bool:
    if isinstance(element, PARENTHETICAL_TYPES):
        return is_scalar_condition(element.content, materialized)
    elif isinstance(element, SUBSELECT_TYPES):
        return True
    elif isinstance(element, COMPARISON_TYPES):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, FUNCTION_TYPES):
        if element.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return False
        return all(is_scalar_condition(x, materialized) for x in element.arguments)
    elif isinstance(element, CONCEPT_TYPES):
        if materialized and element.address in materialized:
            return True
        if element.lineage and isinstance(element.lineage, AGGREGATE_TYPES):
            return is_scalar_condition(element.lineage, materialized)
        if element.lineage and isinstance(element.lineage, FUNCTION_TYPES):
            return is_scalar_condition(element.lineage, materialized)
        return True
    elif isinstance(element, AGGREGATE_TYPES):
        return is_scalar_condition(element.function, materialized)
    elif isinstance(element, CONDITIONAL_TYPES):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, (BuildCaseWhen,)):
        return is_scalar_condition(
            element.comparison, materialized
        ) and is_scalar_condition(element.expr, materialized)
    elif isinstance(element, (BuildCaseElse,)):
        return is_scalar_condition(element.expr, materialized)
    elif isinstance(element, MagicConstants):
        return True
    return True


def reduce_expression(
    var: BuildConcept, group_tuple: list[tuple[ComparisonOperator, Any]]
) -> bool:
    if isinstance(var.datatype, EnumType):
        covered = {
            str(v)
            for op, v in group_tuple
            if op in (ComparisonOperator.EQ, ComparisonOperator.IS)
        }
        return covered >= {str(v) for v in var.datatype.values}

    lower_check: Any
    upper_check: Any
    if var.datatype in (DataType.INTEGER, DataType.FLOAT):
        lower_check = float("-inf")
        upper_check = float("inf")
    elif var.datatype == DataType.DATE:
        lower_check = date.min
        upper_check = date.max
    elif var.datatype == DataType.DATETIME:
        lower_check = datetime.min
        upper_check = datetime.max
    elif var.datatype == DataType.BOOL:
        lower_check = False
        upper_check = True
    else:
        return False

    ranges: list[tuple[Any, Any]] = []
    for op, value in group_tuple:
        increment: int | timedelta | float
        if isinstance(value, date):
            increment = timedelta(days=1)
        elif isinstance(value, datetime):
            increment = timedelta(seconds=1)
        elif isinstance(value, int):
            increment = 1
        elif isinstance(value, float):
            increment = sys.float_info.epsilon
        else:
            return False

        if op == ">":
            ranges.append((value + increment, upper_check))  # type: ignore[operator]
        elif op == ">=":
            ranges.append((value, upper_check))
        elif op == "<":
            ranges.append((lower_check, value - increment))  # type: ignore[operator]
        elif op == "<=":
            ranges.append((lower_check, value))
        elif op in ("=", ComparisonOperator.IS):
            ranges.append((value, value))
        elif op == ComparisonOperator.NE:
            pass
        else:
            return False
    return is_fully_covered(lower_check, upper_check, ranges, increment)  # type: ignore[arg-type]


def boolean_fully_covered(
    start: bool,
    end: bool,
    ranges: list[tuple[bool, bool]],
) -> bool:
    return any(r_start is True and r_end is True for r_start, r_end in ranges) and any(
        r_start is False and r_end is False for r_start, r_end in ranges
    )


def is_fully_covered(
    start: _T,
    end: _T,
    ranges: list[tuple[_T, _T]],
    increment: int | timedelta | float,
) -> bool:
    if isinstance(start, bool) and isinstance(end, bool):
        bool_ranges = [(bool(r_start), bool(r_end)) for r_start, r_end in ranges]
        return boolean_fully_covered(start, end, bool_ranges)
    ranges.sort()
    current_end = start
    for r_start, r_end in ranges:
        if (r_start - current_end) > increment:  # type: ignore[operator]
            return False
        current_end = max(current_end, r_end)
    return current_end >= end


def simplify_conditions(
    conditions: list[BuildComparison | BuildConditional | BuildParenthetical],
) -> bool:
    # Key by address string — concept objects from different datasources may not
    # hash/compare identically even when they represent the same concept.
    grouped: dict[str, tuple[BuildConcept, list[tuple[ComparisonOperator, Any]]]] = {}
    for condition in conditions:
        if not isinstance(condition, BuildComparison):
            return False
        if isinstance(condition.left, BuildConcept) and isinstance(
            condition.right, REDUCABLE_TYPES
        ):
            concept, raw_comparison = condition.left, condition.right
        elif isinstance(condition.right, BuildConcept) and isinstance(
            condition.left, REDUCABLE_TYPES
        ):
            concept, raw_comparison = condition.right, condition.left
        else:
            return False

        if isinstance(raw_comparison, BuildFunction):
            if raw_comparison.operator != FunctionType.CONSTANT:
                return False
            first_arg = raw_comparison.arguments[0]
            if not isinstance(first_arg, TARGET_TYPES):
                return False
            comparison = first_arg
        else:
            if not isinstance(raw_comparison, TARGET_TYPES):
                return False
            comparison = raw_comparison

        entry = grouped.setdefault(concept.canonical_address, (concept, []))
        entry[1].append((condition.operator, comparison))

    return all(reduce_expression(var, group) for var, group in grouped.values())


def decompose_condition(
    conditional: BuildConditional | BuildComparison | BuildParenthetical,
) -> list[
    BuildSubselectComparison | BuildComparison | BuildConditional | BuildParenthetical
]:
    chunks: list[
        BuildSubselectComparison
        | BuildComparison
        | BuildConditional
        | BuildParenthetical
    ] = []
    if not isinstance(conditional, BuildConditional):
        return [conditional]
    if conditional.operator == BooleanOperator.AND:
        if not (
            isinstance(conditional.left, CONDITION_TYPES)
            and isinstance(
                conditional.right,
                CONDITION_TYPES,
            )
        ):
            chunks.append(conditional)
        else:
            for val in [conditional.left, conditional.right]:
                if isinstance(val, BuildConditional):
                    chunks.extend(decompose_condition(val))
                else:
                    chunks.append(val)
    else:
        chunks.append(conditional)
    return chunks


def condition_implies_with_extras(
    query: BuildComparison | BuildConditional | BuildParenthetical,
    required: BuildComparison | BuildConditional | BuildParenthetical,
) -> tuple[bool, list[BuildComparison | BuildConditional | BuildParenthetical]]:
    """If required is a subset of query, returns (True, atoms in query not in required).
    Otherwise returns (False, [])."""
    query_atoms = decompose_condition(query)
    required_atoms = decompose_condition(required)
    if not all(atom in query_atoms for atom in required_atoms):
        return False, []
    return True, [atom for atom in query_atoms if atom not in required_atoms]


def condition_implies(
    query: BuildComparison | BuildConditional | BuildParenthetical,
    required: BuildComparison | BuildConditional | BuildParenthetical,
) -> bool:
    """True if every AND-atom of required appears in query's AND-atoms (query is a superset)."""
    implied, _ = condition_implies_with_extras(query, required)
    return implied


def drop_covered_conditions(
    conditions: list[BuildComparison | BuildConditional | BuildParenthetical],
) -> list[BuildComparison | BuildConditional | BuildParenthetical]:
    """Remove conditions that are made redundant by a more general one in the same list.

    A condition C is dropped if another condition D exists (D != C) where
    condition_implies(C, D) — meaning D is more general (fewer atoms) and
    covers everything C covers.
    """
    result = []
    for i, c in enumerate(conditions):
        dominated = any(
            j != i and condition_implies(c, d)
            for j, d in enumerate(conditions)
            if j < i or c != d  # keep first occurrence of duplicates, drop the rest
        )
        if not dominated:
            result.append(c)
    return result


def _build_eq_map(
    atoms: list[BuildComparison | BuildConditional | BuildParenthetical],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for atom in atoms:
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator not in (ComparisonOperator.EQ, ComparisonOperator.IS):
            continue
        if isinstance(atom.left, BuildConcept) and not isinstance(
            atom.right, BuildConcept
        ):
            result[atom.left.address] = atom.right
        elif isinstance(atom.right, BuildConcept) and not isinstance(
            atom.left, BuildConcept
        ):
            result[atom.right.address] = atom.left
    return result


def conditions_mutually_exclusive(
    a: BuildComparison | BuildConditional | BuildParenthetical,
    b: BuildComparison | BuildConditional | BuildParenthetical,
) -> bool:
    """True if a and b cannot both be satisfied: same concept has conflicting EQ values in each."""
    a_eq = _build_eq_map(decompose_condition(a))
    b_eq = _build_eq_map(decompose_condition(b))
    return any(addr in b_eq and b_eq[addr] != val for addr, val in a_eq.items())


def _build_from_atoms(
    atoms: list[BuildComparison | BuildConditional | BuildParenthetical],
) -> BuildComparison | BuildConditional | BuildParenthetical | None:
    if not atoms:
        return None
    result: BuildComparison | BuildConditional | BuildParenthetical = atoms[0]
    for atom in atoms[1:]:
        result = result + atom  # type: ignore[operator]
    return result


def merge_conditions_and_dedup(
    conditions: BuildComparison | BuildConditional | BuildParenthetical,
    preexisting: BuildComparison | BuildConditional | BuildParenthetical,
) -> BuildComparison | BuildConditional | BuildParenthetical:
    """AND-merge two conditions, deduplicating atoms from `conditions` already in `preexisting`.

    Returns `preexisting` unchanged when every atom of `conditions` is already present,
    preserving object identity so equality checks in validate_stack remain intact.
    """
    preexisting_atoms = decompose_condition(preexisting)
    new_atoms = [
        a for a in decompose_condition(conditions) if a not in preexisting_atoms
    ]
    if not new_atoms:
        return preexisting
    return _build_from_atoms(preexisting_atoms + new_atoms)  # type: ignore[return-value]


def strip_condition_atoms(
    query: BuildComparison | BuildConditional | BuildParenthetical,
    to_strip: BuildComparison | BuildConditional | BuildParenthetical,
) -> BuildComparison | BuildConditional | BuildParenthetical | None:
    """Remove atoms present in to_strip from query's AND-tree. Returns None if all atoms removed."""
    strip_atoms = decompose_condition(to_strip)
    return _build_from_atoms(
        [a for a in decompose_condition(query) if a not in strip_atoms]
    )


def merge_conditions(
    conditions: list[BuildComparison | BuildConditional | BuildParenthetical],
) -> BuildComparison | BuildConditional | BuildParenthetical | None:
    """Merge a list of OR'd conditions into a minimal equivalent.

    Keeps atoms common to all conditions, then drops per-concept varying atoms
    that are provably complete (cover all enum values or the full numeric/date/bool
    range via simplify_conditions). Returns None if the merged result is unconditional.
    If varying atoms cannot be proven complete, returns the first condition unchanged.

    Example: [city='USBOS' AND status='a', city='USBOS' AND status='b'] where
    status has only values {'a','b'} → returns city='USBOS'.
    """
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]

    per_atoms = [decompose_condition(c) for c in conditions]
    common = [a for a in per_atoms[0] if all(a in atoms for atoms in per_atoms[1:])]
    all_varying = [a for atoms in per_atoms for a in atoms if a not in common]

    if not all_varying:
        return _build_from_atoms(common)

    if not simplify_conditions(all_varying):
        return conditions[0]

    return _build_from_atoms(common)


def filter_union_children(
    non_partial_map: dict[str, BuildWhereClause | None],
    query_condition: BuildComparison | BuildConditional | BuildParenthetical,
) -> dict[str, BuildComparison | BuildConditional | BuildParenthetical | None]:
    """Filter union datasource children based on query conditions.

    Takes a mapping of child ID → non_partial_for clause and the query condition.
    Returns a mapping of kept child IDs → injected condition (None = no extra filter needed).

    Children whose non_partial_for is mutually exclusive with the query are dropped.
    Children whose non_partial_for is implied by the query have redundant atoms stripped.
    Falls back to all children (with full condition) if filtering would drop everything.
    """
    kept: dict[str, BuildComparison | BuildConditional | BuildParenthetical | None] = {}
    for child_id, non_partial_for in non_partial_map.items():
        if not non_partial_for:
            kept[child_id] = query_condition
            continue
        npf = non_partial_for.conditional
        if conditions_mutually_exclusive(query_condition, npf):
            continue
        if condition_implies(query_condition, npf):
            kept[child_id] = strip_condition_atoms(query_condition, npf)
        else:
            kept[child_id] = query_condition
    if not kept:
        return {child_id: query_condition for child_id in non_partial_map}
    return kept
