from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildBetween,
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
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

if TYPE_CHECKING:
    from trilogy.core.models.build_environment import BuildEnvironment

AGGREGATE_TYPES = (BuildAggregateWrapper,)
SUBSELECT_TYPES = (BuildSubselectComparison,)
COMPARISON_TYPES = (BuildComparison,)
FUNCTION_TYPES = (BuildFunction,)
PARENTHETICAL_TYPES = (BuildParenthetical,)
CONDITIONAL_TYPES = (BuildConditional,)
CONCEPT_TYPES = (BuildConcept,)
BETWEEN_TYPES = (BuildBetween,)
WINDOW_TYPES = (BuildWindowItem,)

CONDITION_TYPES = (
    BuildSubselectComparison,
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
    BuildBetween,
)

# Operators whose result is NULL (and therefore not TRUE) when either operand is
# NULL — when one of these atoms appears in an AND chain, both sides' concept
# references must be non-null in surviving rows. (Tuple, not set, since
# ComparisonOperator overrides __eq__ without __hash__.)
NULL_PROPAGATING_OPS: tuple[ComparisonOperator, ...] = (
    ComparisonOperator.EQ,
    ComparisonOperator.NE,
    ComparisonOperator.LT,
    ComparisonOperator.GT,
    ComparisonOperator.LTE,
    ComparisonOperator.GTE,
    ComparisonOperator.LIKE,
    ComparisonOperator.ILIKE,
    ComparisonOperator.NOT_LIKE,
    ComparisonOperator.NOT_ILIKE,
    ComparisonOperator.IN,
    ComparisonOperator.NOT_IN,
    ComparisonOperator.CONTAINS,
)

# Functions whose result can be non-null even when an argument is NULL — we
# can't recurse through them to claim the args are non-null.
NULL_OPAQUE_FUNCTIONS: tuple[FunctionType, ...] = (
    FunctionType.COALESCE,
    FunctionType.NULLIF,
    FunctionType.CASE,
    FunctionType.COUNT,  # COUNT(NULL) = 0, not NULL
    FunctionType.COUNT_DISTINCT,
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
) -> BoolExpr | BuildSubselectComparison:
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
    conditional: BoolExpr,
) -> BoolExpr:
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
        | BoolExpr
        | BuildFunction
        | BuildAggregateWrapper
        | BuildCaseWhen
        | BuildCaseElse
        | BuildSubselectComparison
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
        # A membership is placed by its left operand (the set is sourced
        # separately); an aggregate left (`grouping(a) in (0,1)`) must reach HAVING.
        return is_scalar_condition(element.left, materialized)
    elif isinstance(element, COMPARISON_TYPES):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, BETWEEN_TYPES):
        return (
            is_scalar_condition(element.left, materialized)
            and is_scalar_condition(element.low, materialized)
            and is_scalar_condition(element.high, materialized)
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


def gather_windows(
    element: Any, materialized: set[str] | None = None
) -> list[BuildWindowItem]:
    """Every window function (``rank/lag/... over (…)``) that must be *emitted*
    by ``element`` — i.e. appears in the tree and isn't already a materialized
    column. A materialized window concept is just a plain column reference
    (computed by a parent CTE); an inline one must lower to QUALIFY since SQL
    forbids windows in WHERE/HAVING. Windows nested inside arithmetic/case wrappers
    (e.g. ``sum(x) / lead(sum(x), N) over (...)``) are reached too."""
    if isinstance(element, WINDOW_TYPES):
        return [element]
    elif isinstance(element, PARENTHETICAL_TYPES):
        return gather_windows(element.content, materialized)
    elif isinstance(element, COMPARISON_TYPES):
        return gather_windows(element.left, materialized) + gather_windows(
            element.right, materialized
        )
    elif isinstance(element, BETWEEN_TYPES):
        return (
            gather_windows(element.left, materialized)
            + gather_windows(element.low, materialized)
            + gather_windows(element.high, materialized)
        )
    elif isinstance(element, CONDITIONAL_TYPES):
        return gather_windows(element.left, materialized) + gather_windows(
            element.right, materialized
        )
    elif isinstance(element, FUNCTION_TYPES):
        return [w for x in element.arguments for w in gather_windows(x, materialized)]
    elif isinstance(element, AGGREGATE_TYPES):
        return gather_windows(element.function, materialized)
    elif isinstance(element, (BuildCaseWhen,)):
        return gather_windows(element.comparison, materialized) + gather_windows(
            element.expr, materialized
        )
    elif isinstance(element, (BuildCaseElse,)):
        return gather_windows(element.expr, materialized)
    elif isinstance(element, CONCEPT_TYPES):
        if materialized and element.address in materialized:
            return []
        if element.lineage is not None:
            return gather_windows(element.lineage, materialized)
        return []
    return []


def contains_window(element: Any, materialized: set[str] | None = None) -> bool:
    """True when ``element`` must emit a window function. See ``gather_windows``."""
    return bool(gather_windows(element, materialized))


def reduce_expression(
    datatype: Any, group_tuple: list[tuple[ComparisonOperator, Any]]
) -> bool:
    """True when ``group_tuple`` — ``(operator, value)`` atoms against a single
    concept of type ``datatype`` — covers that type's entire domain."""
    if isinstance(datatype, EnumType):
        covered = {
            str(v)
            for op, v in group_tuple
            if op in (ComparisonOperator.EQ, ComparisonOperator.IS)
        }
        return covered >= {str(v) for v in datatype.values}

    lower_check: Any
    upper_check: Any
    if datatype in (DataType.INTEGER, DataType.FLOAT):
        lower_check = float("-inf")
        upper_check = float("inf")
    elif datatype == DataType.DATE:
        lower_check = date.min
        upper_check = date.max
    elif datatype == DataType.DATETIME:
        lower_check = datetime.min
        upper_check = datetime.max
    elif datatype == DataType.BOOL:
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


def conditions_cover_domain(
    grouped: dict[str, tuple[Any, list[tuple[ComparisonOperator, Any]]]],
) -> bool:
    """True when every concept's ``(operator, value)`` atoms span its whole
    domain — i.e. the disjunction of all the atoms is a tautology.

    ``grouped`` maps a concept identity to ``(datatype, atoms)``. Empty input
    proves nothing, so returns False.
    """
    if not grouped:
        return False
    return all(reduce_expression(dt, atoms) for dt, atoms in grouped.values())


def simplify_conditions(
    conditions: list[BoolExpr],
) -> bool:
    # Key by address string — concept objects from different datasources may not
    # hash/compare identically even when they represent the same concept.
    grouped: dict[str, tuple[Any, list[tuple[ComparisonOperator, Any]]]] = {}
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

        entry = grouped.setdefault(concept.canonical_address, (concept.datatype, []))
        entry[1].append((condition.operator, comparison))

    return conditions_cover_domain(grouped)


def decompose_condition(
    conditional: BoolExpr,
) -> list[BuildSubselectComparison | BoolExpr]:
    chunks: list[BuildSubselectComparison | BoolExpr] = []
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
    query: BoolExpr,
    required: BoolExpr,
) -> tuple[bool, list[BoolExpr]]:
    """If required is a subset of query, returns (True, atoms in query not in required).
    Otherwise returns (False, [])."""
    query_atoms = decompose_condition(query)
    required_atoms = decompose_condition(required)
    if not all(atom in query_atoms for atom in required_atoms):
        return False, []
    return True, [atom for atom in query_atoms if atom not in required_atoms]


def condition_implies(
    query: BoolExpr,
    required: BoolExpr,
) -> bool:
    """True if every AND-atom of required appears in query's AND-atoms (query is a superset)."""
    implied, _ = condition_implies_with_extras(query, required)
    return implied


def drop_covered_conditions(
    conditions: list[BoolExpr],
) -> list[BoolExpr]:
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


# Only genuine literals have an enumerable allowed-value set. Anything else
# (a concept, or an expression node like BuildFunction) is not reasoned over.
_LITERAL_TYPES = (str, bytes, bool, int, float, Decimal, date, timedelta, Enum)


def _literal_values(value: object) -> set[object] | None:
    if isinstance(value, (TupleWrapper, ListWrapper, tuple, list, set)):
        if all(isinstance(v, _LITERAL_TYPES) for v in value):
            return set(value)
        return None
    if isinstance(value, _LITERAL_TYPES):
        return {value}
    return None


def _build_allowed_map(
    atoms: list[BoolExpr],
) -> dict[str, set[object]]:
    result: dict[str, set[object]] = {}
    for atom in atoms:
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator not in (
            ComparisonOperator.EQ,
            ComparisonOperator.IS,
            ComparisonOperator.IN,
        ):
            continue
        if isinstance(atom.left, BuildConcept) and not isinstance(
            atom.right, BuildConcept
        ):
            values = _literal_values(atom.right)
            if values is not None:
                result[atom.left.address] = values
        elif isinstance(atom.right, BuildConcept) and not isinstance(
            atom.left, BuildConcept
        ):
            values = _literal_values(atom.left)
            if values is not None:
                result[atom.right.address] = values
    return result


def conditions_mutually_exclusive(
    a: BoolExpr,
    b: BoolExpr,
) -> bool:
    """True if a and b cannot both be satisfied by allowed values for the same concept."""
    a_allowed = _build_allowed_map(decompose_condition(a))
    b_allowed = _build_allowed_map(decompose_condition(b))
    return any(
        addr in b_allowed and values.isdisjoint(b_allowed[addr])
        for addr, values in a_allowed.items()
    )


def condition_value_implies(
    constraint: BoolExpr,
    candidate: BoolExpr,
) -> bool:
    """True if ``constraint`` value-wise guarantees ``candidate`` is true.

    Every per-column allowed value-set from ``constraint`` must be a subset
    of ``candidate``'s allowed set for the same column, AND every atom of
    ``candidate`` must be a literal EQ/IS/IN against a concept (so its allowed
    set is computable). For mixed/unsupported operators we return False —
    we can't prove implication without value semantics.

    Use case: a partial datasource bounded ``complete where channel='CATALOG'``
    value-implies a pushed predicate ``channel in ('WEB','CATALOG')`` — the
    pushed atom is tautological on that branch and adding it just bloats the
    rendered SQL.
    """
    cand_atoms = decompose_condition(candidate)
    if not cand_atoms:
        return False
    cand_allowed = _build_allowed_map(cand_atoms)
    for atom in cand_atoms:
        if not isinstance(atom, BuildComparison):
            return False
        left = atom.left
        right = atom.right
        if isinstance(left, BuildConcept) and not isinstance(right, BuildConcept):
            target_addr = left.address
        elif isinstance(right, BuildConcept) and not isinstance(left, BuildConcept):
            target_addr = right.address
        else:
            return False
        if target_addr not in cand_allowed:
            return False
    constraint_allowed = _build_allowed_map(decompose_condition(constraint))
    for addr, cand_values in cand_allowed.items():
        constraint_values = constraint_allowed.get(addr)
        if constraint_values is None:
            return False
        if not constraint_values.issubset(cand_values):
            return False
    return True


def condition_required_addresses(
    condition: BoolExpr,
) -> set[str]:
    """Canonical addresses of the non-constant row concepts a condition references."""
    return {
        c.canonical_address
        for c in condition.row_arguments
        if c.derivation != Derivation.CONSTANT
    }


def combine_condition_atoms(
    atoms: list[BoolExpr],
) -> BoolExpr | None:
    """AND-combine atoms into a single left-associative condition (None if empty).

    Builds ``BuildConditional`` directly rather than via ``+`` so a bare boolean
    ``BuildConcept`` atom (a valid predicate, but not a ``BoolExpr``) combines
    cleanly instead of tripping ``BoolExpr.__add__``.
    """
    if not atoms:
        return None
    result: BoolExpr = atoms[0]
    for atom in atoms[1:]:
        result = BuildConditional(left=result, operator=BooleanOperator.AND, right=atom)
    return result


def merge_conditions_and_dedup(
    conditions: BoolExpr,
    preexisting: BoolExpr,
) -> BoolExpr:
    """AND-merge two conditions, deduplicating atoms from `conditions` already in `preexisting`.

    Returns `preexisting` unchanged when every atom of `conditions` is already present,
    preserving object identity so equality checks in validate_stack remain intact.

    Atoms within `conditions` are deduped against each other too, so a
    pre-doubled input (`H AND H`) collapses to a single copy.
    """
    preexisting_atoms = decompose_condition(preexisting)
    seen = list(preexisting_atoms)
    new_atoms: list[BoolExpr] = []
    for a in decompose_condition(conditions):
        if a not in seen:
            seen.append(a)
            new_atoms.append(a)
    if not new_atoms:
        return preexisting
    return combine_condition_atoms(preexisting_atoms + new_atoms)  # type: ignore[return-value]


def strip_condition_atoms(
    query: BoolExpr,
    to_strip: BoolExpr,
) -> BoolExpr | None:
    """Remove atoms present in to_strip from query's AND-tree. Returns None if all atoms removed."""
    strip_atoms = decompose_condition(to_strip)
    return combine_condition_atoms(
        [a for a in decompose_condition(query) if a not in strip_atoms]
    )


def merge_conditions(
    conditions: list[BoolExpr],
) -> BoolExpr | None:
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
        return combine_condition_atoms(common)

    if not simplify_conditions(all_varying):
        return conditions[0]

    return combine_condition_atoms(common)


def preserved_non_partial_conditions(
    conditions: "BuildWhereClause", environment: "BuildEnvironment"
) -> "BuildWhereClause | None":
    """Return the subset of `conditions`' atoms owned by a non-partial datasource.

    When the full conditions imply some datasource's `non_partial_for`, those
    atoms are guaranteed by an exact-match source and must stay as WHERE filters
    (rather than being flattened into projections) so partial-datasource
    resolution works downstream.
    """
    atoms = decompose_condition(conditions.conditional)
    atom_str_map = {str(a): a for a in atoms}
    preserved: list[BoolExpr] = []
    seen: set[str] = set()
    for ds in environment.datasources.values():
        if not isinstance(ds, BuildDatasource) or not ds.non_partial_for:
            continue
        if not condition_implies(
            conditions.conditional, ds.non_partial_for.conditional
        ):
            continue
        for np_atom in decompose_condition(ds.non_partial_for.conditional):
            key = str(np_atom)
            if key in atom_str_map and key not in seen:
                preserved.append(atom_str_map[key])
                seen.add(key)
    cond = combine_condition_atoms(preserved)
    if cond is None:
        return None
    return BuildWhereClause(conditional=cond)


def filter_union_children(
    non_partial_map: dict[str, BuildWhereClause | None],
    query_condition: BoolExpr,
) -> dict[str, BoolExpr | None]:
    """Filter union datasource children based on query conditions.

    Takes a mapping of child ID → non_partial_for clause and the query condition.
    Returns a mapping of kept child IDs → injected condition (None = no extra filter needed).

    Children whose non_partial_for is mutually exclusive with the query are dropped.
    Children whose non_partial_for is implied by the query have redundant atoms stripped.
    Falls back to all children (with full condition) if filtering would drop everything.
    """
    kept: dict[
        str,
        BoolExpr | None,
    ] = {}
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


def is_null_literal(value: object) -> bool:
    return value is None or value is MagicConstants.NULL


def _not_null_concept(
    atom: BuildSubselectComparison | BoolExpr,
) -> BuildConcept | None:
    """If ``atom`` is a plain ``X IS NOT NULL`` (either operand order), return X."""
    if isinstance(atom, BuildSubselectComparison) or not isinstance(
        atom, BuildComparison
    ):
        return None
    if atom.operator != ComparisonOperator.IS_NOT:
        return None
    if isinstance(atom.left, BuildConcept) and is_null_literal(atom.right):
        return atom.left
    if isinstance(atom.right, BuildConcept) and is_null_literal(atom.left):
        return atom.right
    return None


def concepts_implied_non_null(value: object) -> set[str]:
    """Concepts whose individual non-nullness is implied when ``value`` evaluates non-null.

    Stops at null-opaque functions (``COALESCE`` et al.) — those can produce a
    non-null result even when an argument is NULL, so descending past them
    would over-claim.
    """
    if isinstance(value, BuildConcept):
        return {value.address}
    if isinstance(value, BuildParenthetical):
        return concepts_implied_non_null(value.content)
    if isinstance(value, BuildAggregateWrapper):
        return concepts_implied_non_null(value.function)
    if isinstance(value, BuildFunction):
        if value.operator in NULL_OPAQUE_FUNCTIONS:
            return set()
        addresses: set[str] = set()
        for arg in value.arguments:
            addresses |= concepts_implied_non_null(arg)
        return addresses
    return set()


def _non_null_or_disjuncts(
    atom: BoolExpr,
) -> list[BoolExpr]:
    if isinstance(atom, BuildParenthetical):
        return _non_null_or_disjuncts(atom.content)  # type: ignore[arg-type]
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.OR:
        return _non_null_or_disjuncts(atom.left) + _non_null_or_disjuncts(  # type: ignore[arg-type]
            atom.right  # type: ignore[arg-type]
        )
    return [atom]


def _flip_op(op: ComparisonOperator) -> ComparisonOperator | None:
    """Mirror an asymmetric comparator so ``X <op> Y`` becomes ``Y <op'> X``.
    Symmetric comparators (``=``/``!=``) mirror to themselves. Returns
    ``None`` for operators we don't flip (``IN``, ``LIKE``, etc.)."""
    if op == ComparisonOperator.LT:
        return ComparisonOperator.GT
    if op == ComparisonOperator.GT:
        return ComparisonOperator.LT
    if op == ComparisonOperator.LTE:
        return ComparisonOperator.GTE
    if op == ComparisonOperator.GTE:
        return ComparisonOperator.LTE
    if op == ComparisonOperator.EQ:
        return ComparisonOperator.EQ
    if op == ComparisonOperator.NE:
        return ComparisonOperator.NE
    return None


def _literal_value(value: object) -> object | None:
    """Return the Python literal carried by ``value``, unwrapping parens.

    ``None`` is returned for anything that isn't a concrete literal (concepts,
    functions, the NULL sentinel, etc.) — callers use that as "can't fold"."""
    while isinstance(value, BuildParenthetical):
        value = value.content  # type: ignore[assignment]
    if is_null_literal(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str, date, datetime)):
        return value
    if isinstance(value, (ListWrapper, TupleWrapper)):
        members: list[object] = []
        for item in value:
            literal = _literal_value(item)
            if literal is None:
                return None
            members.append(literal)
        return tuple(members)
    return None


def _eval_literal_comparison(
    left: object, op: ComparisonOperator, right: object
) -> bool | None:
    """Statically evaluate a literal ``<op>`` comparison, or ``None`` if the
    operator isn't one we fold or the types don't compare cleanly."""
    try:
        if op == ComparisonOperator.EQ or op == ComparisonOperator.IS:
            return left == right
        if op == ComparisonOperator.NE:
            return left != right
        if op == ComparisonOperator.LT:
            return left < right  # type: ignore[operator]
        if op == ComparisonOperator.GT:
            return left > right  # type: ignore[operator]
        if op == ComparisonOperator.LTE:
            return left <= right  # type: ignore[operator]
        if op == ComparisonOperator.GTE:
            return left >= right  # type: ignore[operator]
        if op == ComparisonOperator.IN:
            if isinstance(right, tuple):
                return left in right
            return None
        if op == ComparisonOperator.NOT_IN:
            if isinstance(right, tuple):
                return left not in right
            return None
    except TypeError:
        return None
    return None


def _coalesce_primary_proves_non_null(
    maybe_coalesce: object, op: ComparisonOperator, rhs: object
) -> set[str]:
    """``coalesce(PRIMARY, default1, default2, ...) <op> rhs``: when every
    default is a literal that statically *fails* ``<op> rhs`` (and ``rhs`` is
    itself a literal), the surviving rows can't have come from a default
    branch — so the coalesce had to fall on ``PRIMARY``, proving PRIMARY's
    concepts non-null.

    Returns the implied non-null set, or empty when the pattern doesn't apply
    (LHS isn't a coalesce, any default is a non-literal, any default survives
    the comparison, or the comparator isn't statically foldable)."""
    expr = maybe_coalesce
    while isinstance(expr, BuildParenthetical):
        expr = expr.content  # type: ignore[assignment]
    if not isinstance(expr, BuildFunction):
        return set()
    if expr.operator != FunctionType.COALESCE:
        return set()
    if len(expr.arguments) < 2:
        return set()
    rhs_value = _literal_value(rhs)
    if rhs_value is None:
        return set()
    for default in expr.arguments[1:]:
        default_value = _literal_value(default)
        if default_value is None:
            return set()
        outcome = _eval_literal_comparison(default_value, op, rhs_value)
        if outcome is None or outcome is True:
            return set()
    return concepts_implied_non_null(expr.arguments[0])


def comparison_proves_non_null(
    atom: BuildComparison,
) -> set[str]:
    """Concept addresses a single comparison forces non-null in surviving rows.

    Shared leaf logic for both null-proof walkers (this module's
    ``condition_proves_non_null`` and ``join_upgrade._proves_non_null``); the
    walkers differ only in their handling of nested conditionals/``BETWEEN``.
    """
    left, right, op = atom.left, atom.right, atom.operator
    if op == ComparisonOperator.IS_NOT:
        # `<expr> IS NOT NULL` — every concept inside the expression is non-null.
        if is_null_literal(right):
            return concepts_implied_non_null(left)
        if is_null_literal(left):
            return concepts_implied_non_null(right)
        return set()
    if op == ComparisonOperator.IS:
        # `<expr> IS NULL` wants NULLs and proves nothing; `X IS True`/`IS False`
        # (any non-null literal) forces the operands non-null, like an equality.
        if is_null_literal(right) or is_null_literal(left):
            return set()
        return concepts_implied_non_null(left) | concepts_implied_non_null(right)
    if op in NULL_PROPAGATING_OPS:
        proofs = concepts_implied_non_null(left) | concepts_implied_non_null(right)
        # Peer through a ``coalesce(PRIMARY, defaults...)`` wrapper when every
        # default statically fails the comparison — surviving rows can only
        # come from PRIMARY, so PRIMARY's concepts are non-null. The renderer
        # wraps ``count(...)`` aggregates in ``coalesce(..., 0)`` to satisfy
        # the "count-of-empty is 0, not NULL" convention; predicates like
        # ``> 0`` over the wrapped column otherwise become opaque to us.
        proofs |= _coalesce_primary_proves_non_null(left, op, right)
        flipped = _flip_op(op)
        if flipped is not None:
            proofs |= _coalesce_primary_proves_non_null(right, flipped, left)
        return proofs
    return set()


def _atom_proves_non_null(
    atom: BoolExpr,
) -> set[str]:
    if isinstance(atom, BuildParenthetical):
        return _atom_proves_non_null(atom.content)  # type: ignore[arg-type]
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.OR:
        # A surviving row satisfies at least one disjunct but we don't know
        # which — only concepts non-null under *every* disjunct are proven.
        sets = [condition_proves_non_null(d) for d in _non_null_or_disjuncts(atom)]
        return set.intersection(*sets) if sets else set()
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.AND:
        # ``decompose_condition`` returns the whole AND as one chunk when a
        # child isn't in ``CONDITION_TYPES`` (e.g. a ``raw(...)`` predicate
        # arrives as a bare ``BuildFunction``). Walk both sides ourselves so
        # ordinary Comparison proofs sitting next to the opaque child still
        # contribute.
        return _atom_proves_non_null(atom.left) | _atom_proves_non_null(atom.right)  # type: ignore[arg-type]
    if not isinstance(atom, BuildComparison):
        return set()
    return comparison_proves_non_null(atom)


def condition_proves_non_null(
    condition: BoolExpr,
) -> set[str]:
    """Concept addresses a *fully applied* condition forces non-null.

    Unlike ``non_null_proofs`` (logical/merge stage — deliberately ignores
    ``IS NOT NULL`` because a merged join key may materialize as
    ``COALESCE(left, right)``), this honors ``IS NOT NULL`` too. Safe only for
    a caller asking about a single datasource's own columns under that scan's
    own applied WHERE, where no cross-source COALESCE of the key exists.
    """
    return {
        addr
        for atom in decompose_condition(condition)
        for addr in _atom_proves_non_null(atom)
    }
