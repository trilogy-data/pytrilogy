from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

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
    BuildConceptArgs,
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
    ValidatedType,
    ValueRange,
)
from trilogy.core.models.datasource import RawColumnExpr
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE

ProofSource = CTE | UnionCTE | BuildDatasource | QueryDatasource

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
# NULL: in an AND chain, both sides' concept references must be non-null in
# surviving rows. (Tuple, not set, since ComparisonOperator overrides __eq__
# without __hash__.)
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

# Functions whose result can be non-null even when an argument is NULL, so
# recursing through them cannot claim the args are non-null.
NULL_OPAQUE_FUNCTIONS: tuple[FunctionType, ...] = (
    FunctionType.COALESCE,
    FunctionType.NULLIF,
    FunctionType.CASE,
    FunctionType.COUNT,  # COUNT(NULL) = 0, not NULL
    FunctionType.COUNT_DISTINCT,
    # skip NULL arguments on DuckDB and Postgres: greatest(NULL, 0) = 0
    FunctionType.GREATEST,
    FunctionType.LEAST,
    # a non-null array proves SOME element was non-null, not the row's own value
    FunctionType.ARRAY_AGG,
)

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
        str
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
        # Recurse into the lineage so an aggregate wrapped behind a concept
        # name (directly, in an expression, or in a boolean) is seen and
        # routed to HAVING rather than inlined in WHERE.
        if element.lineage and isinstance(
            element.lineage,
            AGGREGATE_TYPES + FUNCTION_TYPES + CONDITION_TYPES,
        ):
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
    """Every window function that ``element`` must emit: it appears in the tree
    and is not already a materialized column. A materialized window concept is
    a plain column reference; an inline one must lower to QUALIFY since SQL
    forbids windows in WHERE/HAVING. Windows nested inside arithmetic/case
    wrappers are reached too."""
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


# Discrete domains step to a successor; every other ordered domain is dense,
# so no epsilon can stand in for "the next value" (`formal/Trilogy/Sourcing.lean`).
_DISCRETE = (DataType.INTEGER, DataType.BIGINT, DataType.DATE)
_ORDERED = _DISCRETE + (
    DataType.FLOAT,
    DataType.DOUBLE,
    DataType.NUMBER,
    DataType.NUMERIC,
    DataType.DATETIME,
    DataType.TIMESTAMP,
)
_ORDERED_LITERALS = (int, float, Decimal, date)

_ORDER_OPERATORS = (
    ComparisonOperator.LT,
    ComparisonOperator.LTE,
    ComparisonOperator.GT,
    ComparisonOperator.GTE,
)
_CELL_OPERATORS = (
    ComparisonOperator.EQ,
    ComparisonOperator.IS,
    ComparisonOperator.NE,
    ComparisonOperator.IS_NOT,
    *_ORDER_OPERATORS,
)


# A domain cell: a literal, an open interval between adjacent literals (None
# is unbounded), or the NULL member. Every comparison against the literals is
# constant on a cell, so evaluating each atom once per cell decides it exactly.
_Cell = tuple[Any, ...]
_NULL_CELL: _Cell = ("null",)


def _gap_nonempty(lo: Any, hi: Any, discrete: bool) -> bool:
    if lo is None or hi is None:
        return True
    if not discrete:
        return lo < hi
    if isinstance(lo, date):
        return lo + timedelta(days=1) < hi
    return math.floor(lo) + 1 < hi


def _domain_cells(base: Any, literals: list[Any]) -> list[_Cell] | None:
    if base == DataType.BOOL:
        return [("point", False), ("point", True)]
    if base not in _ORDERED:
        return None
    if any(
        isinstance(v, bool) or not isinstance(v, _ORDERED_LITERALS) for v in literals
    ):
        return None
    try:
        points = sorted(set(literals))
    except TypeError:
        return None
    discrete = base in _DISCRETE
    bounds: list[Any] = [None, *points, None]
    cells: list[_Cell] = []
    for idx in range(len(bounds) - 1):
        lo, hi = bounds[idx], bounds[idx + 1]
        if _gap_nonempty(lo, hi, discrete):
            cells.append(("gap", lo, hi))
        if hi is not None:
            cells.append(("point", hi))
    return cells


def _compare(op: ComparisonOperator, left: Any, right: Any) -> bool:
    if op in (ComparisonOperator.EQ, ComparisonOperator.IS):
        return left == right
    if op in (ComparisonOperator.NE, ComparisonOperator.IS_NOT):
        return left != right
    if op == ComparisonOperator.LT:
        return left < right
    if op == ComparisonOperator.LTE:
        return left <= right
    if op == ComparisonOperator.GT:
        return left > right
    return left >= right


def _atom_holds(op: ComparisonOperator, value: Any, cell: _Cell) -> bool | None:
    """Kleene-TRUE of ``x <op> value`` on ``cell``; None for an operator the
    cell model does not reason about."""
    if value is None:
        if op == ComparisonOperator.IS:
            return cell == _NULL_CELL
        if op == ComparisonOperator.IS_NOT:
            return cell != _NULL_CELL
        return None
    if op not in _CELL_OPERATORS:
        return None
    if cell == _NULL_CELL:
        return False
    if cell[0] == "point":
        return _compare(op, cell[1], value)
    # An open gap holds no literal, so it lies wholly below or above `value`.
    lo, hi = cell[1], cell[2]
    if op in (ComparisonOperator.LT, ComparisonOperator.LTE):
        return hi is not None and hi <= value
    if op in (ComparisonOperator.GT, ComparisonOperator.GTE):
        return lo is not None and lo >= value
    return op in (ComparisonOperator.NE, ComparisonOperator.IS_NOT)


def _in_declared_ranges(ranges: tuple[ValueRange, ...], cell: _Cell) -> bool:
    if cell[0] == "point":
        return any(r.contains(cell[1]) for r in ranges)
    lo, hi = cell[1], cell[2]
    return any(
        (r.min is None or (lo is not None and lo >= r.min))
        and (r.max is None or (hi is not None and hi <= r.max))
        for r in ranges
    )


def domain_cover(
    datatype: Any,
    group_tuple: list[tuple[ComparisonOperator, Any]],
    nullable: bool = False,
) -> list[list[bool]] | None:
    """Which of ``group_tuple``'s atoms hold on each cell of ``datatype``'s
    domain (plus NULL when ``nullable``), one row per cell; None when the atoms
    or the type are outside what the cell model can decide."""
    if isinstance(datatype, EnumType):
        # An enum's order is not its values' string order: only equality.
        if any(op in _ORDER_OPERATORS for op, _ in group_tuple):
            return None
        cells: list[_Cell] = [("point", str(v)) for v in datatype.values]
        atoms = [(op, None if v is None else str(v)) for op, v in group_tuple]
    elif isinstance(datatype, DataType) and datatype == DataType.STRING:
        # Unbounded, and SQL collation is not python's string order: equality
        # only, each literal a point and every other string one cell.
        if any(op in _ORDER_OPERATORS for op, _ in group_tuple):
            return None
        strings = {v for _, v in group_tuple if v is not None}
        if not all(isinstance(v, str) for v in strings):
            return None
        cells = [("point", v) for v in sorted(strings)] + [("gap", None, None)]
        atoms = list(group_tuple)
    else:
        base = datatype
        ranges: tuple[ValueRange, ...] = ()
        if isinstance(datatype, ValidatedType):
            if datatype.pattern is not None or not datatype.ranges:
                return None
            base, ranges = datatype.data_type, datatype.ranges
        literals = [v for _, v in group_tuple if v is not None]
        literals += [b for r in ranges for b in (r.min, r.max) if b is not None]
        domain = _domain_cells(base, literals)
        if domain is None:
            return None
        cells = [c for c in domain if not ranges or _in_declared_ranges(ranges, c)]
        atoms = list(group_tuple)
    if nullable:
        cells.append(_NULL_CELL)
    matrix: list[list[bool]] = []
    for cell in cells:
        row: list[bool] = []
        for op, value in atoms:
            holds = _atom_holds(op, value, cell)
            if holds is None:
                return None
            row.append(holds)
        matrix.append(row)
    return matrix


def domain_hits(
    datatype: Any,
    group_tuple: list[tuple[ComparisonOperator, Any]],
    nullable: bool = False,
) -> list[int] | None:
    """How many of ``group_tuple``'s atoms hold on each cell of the domain."""
    matrix = domain_cover(datatype, group_tuple, nullable)
    return None if matrix is None else [sum(row) for row in matrix]


# An exact-cover search enumerates products of duplicate arms; a family's
# best cover is among the first few, so the search stops here.
MAX_EXACT_COVERS = 64


def exact_covers(matrix: list[list[bool]]) -> list[tuple[int, ...]]:
    """Every set of columns (atoms) that holds exactly once on each row
    (cell): the sub-families that partition the domain."""
    found: list[tuple[int, ...]] = []
    _search_covers(matrix, frozenset(range(len(matrix))), (), found)
    return found


def _search_covers(
    matrix: list[list[bool]],
    open_rows: frozenset[int],
    chosen: tuple[int, ...],
    found: list[tuple[int, ...]],
) -> None:
    if len(found) >= MAX_EXACT_COVERS:
        return
    if not open_rows:
        found.append(chosen)
        return
    # Branch on the lowest open cell: exactly one column of any cover claims
    # it, so each cover is found once.
    row = min(open_rows)
    for col, holds in enumerate(matrix[row]):
        if not holds:
            continue
        claimed = frozenset(i for i, r in enumerate(matrix) if r[col])
        if claimed <= open_rows:
            _search_covers(matrix, open_rows - claimed, chosen + (col,), found)


def reduce_expression(
    datatype: Any,
    group_tuple: list[tuple[ComparisonOperator, Any]],
    nullable: bool = False,
) -> bool:
    """True when the disjunction of ``group_tuple`` (``(operator, value)``
    atoms against one concept of type ``datatype``) holds on its whole domain."""
    hits = domain_hits(datatype, group_tuple, nullable)
    return hits is not None and all(hits)


def partitions_domain(
    datatype: Any,
    group_tuple: list[tuple[ComparisonOperator, Any]],
    nullable: bool = False,
) -> bool:
    """True when exactly one atom holds on every value of the domain: the
    atoms' sources union, with no gap and no overlap, into the whole."""
    hits = domain_hits(datatype, group_tuple, nullable)
    return hits is not None and all(h == 1 for h in hits)


# Discriminator address (or canonical address) -> enum values the statement's
# row gate rules out. A per-statement view of the domain: a partition family
# is complete once it covers the values that can still occur.
ExcludedEnumValues = Mapping[str, frozenset[str]]

# The NULL member of a nullable discriminator, listed among a gate's excluded
# values when the gate proves the discriminator non-null.
NULL_MEMBER = "<null>"

_FLIPPED = (
    (ComparisonOperator.LT, ComparisonOperator.GT),
    (ComparisonOperator.LTE, ComparisonOperator.GTE),
    (ComparisonOperator.GT, ComparisonOperator.LT),
    (ComparisonOperator.GTE, ComparisonOperator.LTE),
)


def _flipped(operator: ComparisonOperator) -> ComparisonOperator:
    """``operator`` with its operands swapped: ``10 > x`` is ``x < 10``."""
    return next((b for a, b in _FLIPPED if a == operator), operator)


def gate_allowed_values(conditional: BoolExpr) -> dict[str, set[object]]:
    """Address -> literal values a row gate's EQ/IS/IN atoms allow."""
    return _build_allowed_map(decompose_condition(conditional))


def effective_enum_domain(
    datatype: Any,
    concept: BuildConcept,
    excluded: ExcludedEnumValues | None,
) -> Any:
    """``datatype`` with the values the statement rules out for ``concept``
    removed, so a domain proof runs over what can still occur."""
    if not excluded or not isinstance(datatype, EnumType):
        return datatype
    gone = excluded.get(concept.address) or excluded.get(concept.canonical_address)
    if not gone:
        return datatype
    return EnumType(
        type=datatype.type, values=[v for v in datatype.values if str(v) not in gone]
    )


def conditions_cover_domain(
    grouped: dict[str, tuple[Any, list[tuple[ComparisonOperator, Any]]]],
) -> bool:
    """True when every concept's ``(operator, value)`` atoms span its whole
    domain, i.e. the disjunction of all the atoms is a tautology.

    ``grouped`` maps a concept identity to ``(datatype, atoms)``. Empty input
    proves nothing, so returns False.
    """
    if not grouped:
        return False
    return all(reduce_expression(dt, atoms) for dt, atoms in grouped.values())


def _grouped_atoms(
    conditions: list[BoolExpr],
    excluded: ExcludedEnumValues | None,
) -> dict[str, tuple[Any, bool, list[tuple[ComparisonOperator, Any]]]] | None:
    """Concept -> ``(domain, nullable, atoms)`` for a list of single-atom
    conditions, each atom normalized to ``concept <op> literal`` (a NULL
    literal is None); None when any condition is not such an atom."""
    # Key by address string: concept objects from different datasources may not
    # hash/compare identically even when they represent the same concept.
    grouped: dict[str, tuple[Any, bool, list[tuple[ComparisonOperator, Any]]]] = {}
    for condition in conditions:
        if not isinstance(condition, BuildComparison):
            return None
        operator = condition.operator
        raw_comparison: Any
        if isinstance(condition.left, BuildConcept) and not isinstance(
            condition.right, BuildConcept
        ):
            concept, raw_comparison = condition.left, condition.right
        elif isinstance(condition.right, BuildConcept):
            concept, raw_comparison = condition.right, condition.left
            operator = _flipped(operator)
        else:
            return None

        comparison: Any
        if raw_comparison is MagicConstants.NULL:
            comparison = None
        elif isinstance(raw_comparison, BuildFunction):
            if raw_comparison.operator != FunctionType.CONSTANT:
                return None
            comparison = raw_comparison.arguments[0]
        else:
            comparison = raw_comparison
        if comparison is not None and not isinstance(comparison, TARGET_TYPES):
            return None

        gone = (excluded or {}).get(concept.address) or (excluded or {}).get(
            concept.canonical_address, frozenset()
        )
        entry = grouped.setdefault(
            concept.canonical_address,
            (
                effective_enum_domain(concept.datatype, concept, excluded),
                concept.is_nullable and NULL_MEMBER not in gone,
                [],
            ),
        )
        entry[2].append((operator, comparison))
    return grouped


def simplify_conditions(
    conditions: list[BoolExpr],
    excluded: ExcludedEnumValues | None = None,
) -> bool:
    """True when the disjunction of ``conditions`` holds on every row."""
    grouped = _grouped_atoms(conditions, excluded)
    if not grouped:
        return False
    return all(
        reduce_expression(dt, atoms, nullable)
        for dt, nullable, atoms in grouped.values()
    )


def conditions_partition_domain(
    conditions: list[BoolExpr],
    excluded: ExcludedEnumValues | None = None,
) -> bool:
    """True when exactly one of ``conditions`` holds on every row, so sources
    each complete where one of them holds union, with no row missing or
    doubled, into a complete source (`Sourcing.union_perm_of_partition`)."""
    grouped = _grouped_atoms(conditions, excluded)
    return (
        grouped is not None
        and len(grouped) == 1
        and all(
            partitions_domain(dt, atoms, nullable)
            for dt, nullable, atoms in grouped.values()
        )
    )


def conditions_partition_covers(
    conditions: list[BoolExpr],
    excluded: ExcludedEnumValues | None = None,
) -> list[tuple[int, ...]]:
    """Index sets of ``conditions`` that each partition the domain. A family
    with a redundant arm (a materialized copy of one partition) has no
    partition as a whole, but every cover that picks one copy is one."""
    grouped = _grouped_atoms(conditions, excluded)
    if grouped is None or len(grouped) != 1:
        return []
    ((datatype, nullable, atoms),) = grouped.values()
    matrix = domain_cover(datatype, atoms, nullable)
    return exact_covers(matrix) if matrix else []


def references_any_concept(element: Any, addresses: set[str]) -> bool:
    """True when the condition directly references any of the given addresses."""
    if not addresses:
        return False
    if isinstance(element, BuildConcept):
        return element.address in addresses
    if isinstance(element, BuildConceptArgs):
        return any(arg.address in addresses for arg in element.concept_arguments)
    return False


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
    set is computable). Mixed/unsupported operators return False.

    A partial datasource's ``complete where k = 'a'`` value-implies a pushed
    predicate ``k in ('a', 'b')``, which is tautological on that branch.
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


def and_optional(existing: BoolExpr | None, new: BoolExpr) -> BoolExpr:
    """AND `new` onto a condition that may not be there yet. The two-operand
    case of `combine_condition_atoms`, typed so the result is never None."""
    if existing is None:
        return new
    return BuildConditional(left=existing, operator=BooleanOperator.AND, right=new)


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


def combine_where_clauses(
    clauses: list[BuildWhereClause],
) -> BuildWhereClause | None:
    """AND-combine whole where clauses into one (None if empty)."""
    combined = combine_condition_atoms([clause.conditional for clause in clauses])
    return BuildWhereClause(conditional=combined) if combined is not None else None


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
    excluded: ExcludedEnumValues | None = None,
) -> BoolExpr | None:
    """Merge a list of OR'd conditions into a minimal equivalent.

    Keeps atoms common to all conditions, then drops per-concept varying atoms
    that are provably complete (cover all enum values or the full
    numeric/date/bool range via simplify_conditions). Returns None if the merged
    result is unconditional. If varying atoms cannot be proven complete, returns
    the first condition unchanged.

    Example: [k='x' AND status='a', k='x' AND status='b'] where status has only
    values {'a','b'} returns k='x'.
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

    if not simplify_conditions(all_varying, excluded):
        return conditions[0]

    return combine_condition_atoms(common)


def filter_union_children(
    non_partial_map: dict[str, BuildWhereClause | None],
    query_condition: BoolExpr,
) -> dict[str, BoolExpr | None]:
    """Filter union datasource children based on query conditions.

    Takes a mapping of child ID to non_partial_for clause and the query condition.
    Returns a mapping of kept child IDs to injected condition (None = no extra
    filter needed).

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

    Stops at null-opaque functions (``COALESCE`` et al.): those can produce a
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
    functions, the NULL sentinel, etc.); callers use that as "can't fold"."""
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
    default is a literal that statically fails ``<op> rhs`` (and ``rhs`` is
    itself a literal), the surviving rows can't have come from a default
    branch, so the coalesce fell on ``PRIMARY``, proving PRIMARY's concepts
    non-null.

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


def _forced_true_proofs(value: object) -> set[str]:
    """Non-null proofs carried by ``<value> = True`` / ``IS True``.

    Forcing ``value`` TRUE forces the boolean expression it names to be
    satisfied, so everything THAT condition proves non-null holds too, both
    for an inline condition and for a concept whose lineage is one (a flag
    defined as ``x is not null`` proves ``x`` non-null when forced TRUE)."""
    while isinstance(value, BuildParenthetical):
        value = value.content  # type: ignore[assignment]
    if isinstance(value, BuildConcept) and isinstance(value.lineage, CONDITION_TYPES):
        return condition_proves_non_null(value.lineage)
    if isinstance(value, CONDITION_TYPES):
        return condition_proves_non_null(value)
    return set()


def comparison_proves_non_null(
    atom: BuildComparison,
) -> set[str]:
    """Concept addresses a single comparison forces non-null in surviving rows.

    Shared leaf logic for both null-proof walkers (this module's
    ``condition_proves_non_null`` and ``join_upgrade._proves_non_null``); the
    walkers differ only in their handling of nested conditionals/``BETWEEN``.
    """
    left, right, op = atom.left, atom.right, atom.operator
    if (
        op in (ComparisonOperator.IN, ComparisonOperator.NOT_IN)
        and isinstance(left, BuildFunction)
        and left.operator == FunctionType.ROW_TUPLE
    ):
        # row-tuple membership renders as a null-safe existence match, so a
        # NULL component still matches a NULL member
        return set()
    if op == ComparisonOperator.IS_NOT:
        # `<expr> IS NOT NULL`: every concept inside the expression is non-null.
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
        proofs = concepts_implied_non_null(left) | concepts_implied_non_null(right)
        if right is True:
            proofs |= _forced_true_proofs(left)
        if left is True:
            proofs |= _forced_true_proofs(right)
        return proofs
    if op in NULL_PROPAGATING_OPS:
        proofs = concepts_implied_non_null(left) | concepts_implied_non_null(right)
        if op == ComparisonOperator.EQ:
            if right is True:
                proofs |= _forced_true_proofs(left)
            if left is True:
                proofs |= _forced_true_proofs(right)
        # Peer through a ``coalesce(PRIMARY, defaults...)`` wrapper when every
        # default statically fails the comparison. The renderer wraps
        # ``count(...)`` in ``coalesce(..., 0)``, so predicates like ``> 0``
        # over the wrapped column would otherwise be opaque.
        proofs |= _coalesce_primary_proves_non_null(left, op, right)
        flipped = _flip_op(op)
        if flipped is not None:
            proofs |= _coalesce_primary_proves_non_null(right, flipped, left)
        return proofs
    return set()


def _atom_proves_non_null(atom: BoolExpr, between: bool) -> set[str]:
    if isinstance(atom, BuildParenthetical):
        return _atom_proves_non_null(atom.content, between)  # type: ignore[arg-type]
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.OR:
        # A surviving row satisfies at least one unknown disjunct; only concepts
        # non-null under every disjunct are proven.
        sets = [_proves_non_null(d, between) for d in _non_null_or_disjuncts(atom)]
        return set.intersection(*sets) if sets else set()
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.AND:
        # ``decompose_condition`` returns the whole AND as one chunk when a
        # child isn't in ``CONDITION_TYPES`` (a bare ``BuildFunction``); walk
        # both sides so proofs beside the opaque child still contribute.
        return _atom_proves_non_null(
            atom.left, between  # type: ignore[arg-type]
        ) | _atom_proves_non_null(
            atom.right, between  # type: ignore[arg-type]
        )
    if between and isinstance(atom, BuildBetween):
        # all three operands must be non-null for the row to survive
        return (
            concepts_implied_non_null(atom.left)
            | concepts_implied_non_null(atom.low)
            | concepts_implied_non_null(atom.high)
        )
    if not isinstance(atom, BuildComparison):
        return set()
    return comparison_proves_non_null(atom)


def _proves_non_null(condition: BoolExpr, between: bool) -> set[str]:
    return {
        addr
        for atom in decompose_condition(condition)
        for addr in _atom_proves_non_null(atom, between)
    }


def condition_proves_non_null(
    condition: BoolExpr,
) -> set[str]:
    """Concept addresses a *fully applied* condition forces non-null.

    Unlike ``non_null_proofs`` (merge stage; ignores ``IS NOT NULL`` because a
    merged join key may materialize as ``COALESCE(left, right)``), this honors
    ``IS NOT NULL`` too. Safe only for a node's own output columns under the
    WHERE that node itself applies, rendered through the same source map, so
    the condition reads each column exactly as the output does.
    """
    return _proves_non_null(condition, between=False)


def drop_proven_non_null(
    concepts: list[BuildConcept], condition: BoolExpr | None
) -> list[BuildConcept]:
    """``concepts`` minus those ``condition`` forces non-null."""
    if not condition or not concepts:
        return concepts
    proven = condition_proves_non_null(condition)
    return [c for c in concepts if c.address not in proven]


def gather_non_null_proofs(cond: BoolExpr) -> set[str]:
    """Concept addresses a condition forces non-null in surviving rows, in
    the form join narrowing consumes: ``IS NOT NULL``, ``BETWEEN`` and
    OR-of-ANDs all count. Sound for a join whose sides are materialized
    relations (a CTE, or one datasource of a merge), never for a merged key
    that will render as a cross-source COALESCE."""
    return _proves_non_null(cond, between=True)


def gather_or_groups(cond: BoolExpr) -> list[list[set[str]]]:
    """Per-OR-atom disjunct proof sets, for side-level (not concept-level)
    proofs. ``(a.x = 1 OR a.y = 2)`` proves no single concept non-null, but
    every disjunct forces the ``a`` side present, enough to drop ``a``'s
    unmatched rows. Only kept when every disjunct proves something."""
    groups: list[list[set[str]]] = []
    for atom in decompose_condition(cond):
        disjuncts = _non_null_or_disjuncts(atom)
        if len(disjuncts) < 2:
            continue
        sets = [gather_non_null_proofs(d) for d in disjuncts]
        if all(sets):
            groups.append(sets)
    return groups


def partial_addresses(source: ProofSource) -> set[str]:
    """Addresses a source covers only PARTIALLY: it does not hold the full
    set of that concept's values.

    A filter on a partial concept cannot promote an OUTER join to a stricter
    one: the other side's unmatched rows are meaningful (their value simply
    is not in this partial source), and the predicate renders from the
    complete side anyway, so requiring the value non-null does not reject the
    NULL-padded rows. ``customer LEFT JOIN orders`` (orders partial for
    ``customer.id``: promoting would drop no-order customers) versus
    ``customer LEFT JOIN customer_address`` (the address PK is complete, so a
    non-null filter legitimately forces INNER)."""
    if isinstance(source, BuildDatasource):
        return set()
    return {c.address for c in source.partial_concepts}


def _scan_opaque_bindings(
    ds: ProofSource | None, out: set[str], seen: set[int]
) -> None:
    if ds is None or id(ds) in seen:
        return
    seen.add(id(ds))
    if isinstance(ds, BuildDatasource):
        for col in ds.columns:
            alias = col.alias
            if isinstance(alias, RawColumnExpr) or (
                isinstance(alias, BuildFunction)
                and not concepts_implied_non_null(alias)
            ):
                out.add(col.concept.address)
    elif isinstance(ds, QueryDatasource):
        for child in ds.datasources:
            _scan_opaque_bindings(child, out, seen)
        _scan_opaque_bindings(ds.base_datasource, out, seen)
    else:
        _scan_opaque_bindings(ds.source, out, seen)


def opaque_binding_addresses(source: ProofSource | None) -> set[str]:
    """Addresses whose datasource column binding can be non-null even on a
    join's NULL-padded (unmatched) rows.

    A plain column is NULL-padded when its row is unmatched, so a non-null
    proof on it genuinely forces that side present. A ``RawColumnExpr``
    (opaque SQL such as ``CASE WHEN <rightcol> IS NOT NULL THEN True ELSE
    False END``) or a null-opaque function can yield a non-null value for an
    unmatched row, so proving such a concept non-null proves nothing about
    the join and must never force a stricter type: a ``flag = false``
    predicate over such a CASE is satisfied by exactly the unmatched rows."""
    out: set[str] = set()
    _scan_opaque_bindings(source, out, set())
    return out
