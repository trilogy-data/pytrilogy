import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from itertools import product
from typing import List, Tuple, TypeVar

from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildParenthetical,
)
from trilogy.core.models.core import DataType, EnumType
from trilogy.core.models.datasource import Address

# Define a generic type that ensures start and end are the same type
T = TypeVar("T", int, float, date, datetime)


def reduce_expression(
    var: BuildConcept, group_tuple: list[tuple[ComparisonOperator, T]]
) -> bool:
    # Track ranges
    lower_check: T
    upper_check: T

    # if var.datatype in (DataType.FLOAT,):
    #     lower_check = float("-inf")  # type: ignore
    #     upper_check = float("inf")  # type: ignore
    if var.datatype == DataType.INTEGER:
        lower_check = float("-inf")  # type: ignore
        upper_check = float("inf")  # type: ignore
    elif var.datatype == DataType.DATE:
        lower_check = date.min  # type: ignore
        upper_check = date.max  # type: ignore

    elif var.datatype == DataType.DATETIME:
        lower_check = datetime.min  # type: ignore
        upper_check = datetime.max  # type: ignore
    elif var.datatype == DataType.BOOL:
        lower_check = False  # type: ignore
        upper_check = True  # type: ignore
    elif var.datatype == DataType.FLOAT:
        lower_check = float("-inf")  # type: ignore
        upper_check = float("inf")  # type: ignore
    else:
        return False

    ranges: list[Tuple[T, T]] = []
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

        if op == ">":
            ranges.append(
                (
                    value + increment,
                    upper_check,
                )
            )
        elif op == ">=":
            ranges.append(
                (
                    value,
                    upper_check,
                )
            )
        elif op == "<":
            ranges.append(
                (
                    lower_check,
                    value - increment,
                )
            )
        elif op == "<=":
            ranges.append(
                (
                    lower_check,
                    value,
                )
            )
        elif op == "=":
            ranges.append(
                (
                    value,
                    value,
                )
            )
        elif op == ComparisonOperator.IS:
            ranges.append(
                (
                    value,
                    value,
                )
            )
        elif op == ComparisonOperator.NE:
            pass
        else:
            return False
    return is_fully_covered(lower_check, upper_check, ranges, increment)


TARGET_TYPES = (
    int,
    date,
    float,
    datetime,
    bool,
)
REDUCABLE_TYPES = (int, float, date, bool, datetime, BuildFunction)


def simplify_conditions(
    conditions: list[BuildComparison | BuildConditional | BuildParenthetical],
) -> bool:
    # Group conditions by variable
    grouped: dict[
        BuildConcept, list[tuple[ComparisonOperator, datetime | int | date | float]]
    ] = defaultdict(list)
    for condition in conditions:
        if not isinstance(condition, BuildComparison):
            return False
        left_is_concept = False
        left_is_reducable = False
        right_is_concept = False
        right_is_reducable = False
        if isinstance(condition.left, BuildConcept):
            left_is_concept = True
        elif isinstance(condition.left, REDUCABLE_TYPES):
            left_is_reducable = True

        if isinstance(condition.right, BuildConcept):
            right_is_concept = True
        elif isinstance(condition.right, REDUCABLE_TYPES):
            right_is_reducable = True

        if not (
            (left_is_concept and right_is_reducable)
            or (right_is_concept and left_is_reducable)
        ):
            return False
        if left_is_concept:
            concept = condition.left
            raw_comparison = condition.right
        else:
            concept = condition.right
            raw_comparison = condition.left

        if isinstance(raw_comparison, BuildFunction):
            if not raw_comparison.operator == FunctionType.CONSTANT:
                return False
            first_arg = raw_comparison.arguments[0]
            if not isinstance(first_arg, TARGET_TYPES):
                return False
            comparison = first_arg
        else:
            if not isinstance(raw_comparison, TARGET_TYPES):
                return False
            comparison = raw_comparison

        if not isinstance(comparison, REDUCABLE_TYPES):
            return False

        var: BuildConcept = concept  # type: ignore
        op = condition.operator
        grouped[var].append((op, comparison))

    simplified = []
    for var, group_tuple in grouped.items():
        simplified.append(reduce_expression(var, group_tuple))  # type: ignore

    # Final simplification
    return True if all(isinstance(s, bool) and s for s in simplified) else False


def boolean_fully_covered(
    start: bool,
    end: bool,
    ranges: List[Tuple[bool, bool]],
):
    all = []
    for r_start, r_end in ranges:
        if r_start is True and r_end is True:
            all.append(True)
        elif r_start is False and r_end is False:
            all.append(False)
    return set(all) == {False, True}


def is_fully_covered(
    start: T,
    end: T,
    ranges: List[Tuple[T, T]],
    increment: int | timedelta | float,
):
    """
    Check if the list of range pairs fully covers the set [start, end].

    Parameters:
    - start (int or float): The starting value of the set to cover.
    - end (int or float): The ending value of the set to cover.
    - ranges (list of tuples): List of range pairs [(start1, end1), (start2, end2), ...].

    Returns:
    - bool: True if the ranges fully cover [start, end], False otherwise.
    """
    if isinstance(start, bool) and isinstance(end, bool):
        # convert each element of each tuple to a boolean
        bool_ranges = [(bool(r_start), bool(r_end)) for r_start, r_end in ranges]

        return boolean_fully_covered(start, end, bool_ranges)
    # Sort ranges by their start values (and by end values for ties)
    ranges.sort()

    # Check for gaps
    current_end = start
    for r_start, r_end in ranges:
        # If there's a gap between the current range and the previous coverage
        if (r_start - current_end) > increment:  # type: ignore
            return False
        # Extend the current coverage
        current_end = max(current_end, r_end)

    # If the loop ends and we haven't reached the end, return False
    return current_end >= end


def _enum_fully_covered(
    conditions: list[BuildComparison | BuildConditional | BuildParenthetical],
    enum_type: EnumType,
) -> bool:
    """Check if equality conditions cover all values of an enum."""
    covered: set = set()
    for condition in conditions:
        if not isinstance(condition, BuildComparison):
            return False
        if condition.operator not in (ComparisonOperator.EQ, ComparisonOperator.IS):
            return False
        if isinstance(condition.left, BuildConcept) and not isinstance(
            condition.right, BuildConcept
        ):
            covered.add(condition.right)
        elif isinstance(condition.right, BuildConcept) and not isinstance(
            condition.left, BuildConcept
        ):
            covered.add(condition.left)
        else:
            return False
    return covered >= set(enum_type.values)


def _datasource_score(ds: BuildDatasource) -> int:
    """1 if table-addressed (materialized), 0 if script/query."""
    if isinstance(ds.address, Address) and (ds.address.is_query or ds.address.is_file):
        return 0
    return 1


def _extract_enum_value(
    conditional: BuildComparison | BuildConditional | BuildParenthetical,
) -> object | None:
    """Extract the literal value from a single equality comparison, or None."""
    if not isinstance(conditional, BuildComparison):
        return None
    if conditional.operator not in (ComparisonOperator.EQ, ComparisonOperator.IS):
        return None
    if isinstance(conditional.left, BuildConcept) and not isinstance(
        conditional.right, BuildConcept
    ):
        return conditional.right
    if isinstance(conditional.right, BuildConcept) and not isinstance(
        conditional.left, BuildConcept
    ):
        return conditional.left
    return None


def _best_enum_union(
    dses: list[BuildDatasource],
    enum_type: EnumType,
    merge_key: BuildConcept,
) -> list[BuildDatasource] | None:
    """Find the best minimal covering combination for an enum-partitioned key.

    Groups by covered enum value, then picks the highest-scoring combination
    (one source per value) that has field overlap beyond the merge key itself.
    Materialized table sources score higher than script/query sources.
    """
    by_value: dict[object, list[BuildDatasource]] = defaultdict(list)
    for ds in dses:
        if not ds.non_partial_for:
            continue
        val = _extract_enum_value(ds.non_partial_for.conditional)
        if val is None:
            continue
        by_value[val].append(ds)

    # All enum values must have at least one candidate source
    if set(str(v) for v in by_value.keys()) < set(enum_type.values):
        return None

    values = list(by_value.keys())
    merge_key_addr = {merge_key.address}

    best: list[BuildDatasource] | None = None
    best_score = -1

    for combo in product(*[by_value[v] for v in values]):
        combo_list = list(combo)

        # Require at least one shared concept beyond the merge key
        overlap = set(c.address for c in combo_list[0].output_concepts)
        for ds in combo_list[1:]:
            overlap &= {c.address for c in ds.output_concepts}
        if not (overlap - merge_key_addr):
            continue

        conditions = [
            c.non_partial_for.conditional for c in combo_list if c.non_partial_for
        ]
        if not _enum_fully_covered(conditions, enum_type):
            continue

        score = sum(_datasource_score(ds) for ds in combo_list)
        if score > best_score:
            best_score = score
            best = combo_list

    return best


def get_union_sources(
    datasources: list[BuildDatasource], concepts: list[BuildConcept]
) -> List[list[BuildDatasource]]:
    candidates: list[BuildDatasource] = []

    for x in datasources:
        if any([c.address in x.output_concepts for c in concepts]):
            if (
                any([c.address in x.partial_concepts for c in concepts])
                and x.non_partial_for
            ):
                candidates.append(x)
    assocs: dict[str, list[BuildDatasource]] = defaultdict(list[BuildDatasource])
    for x in candidates:
        if not x.non_partial_for:
            continue
        if not len(x.non_partial_for.concept_arguments) == 1:
            continue
        merge_key = x.non_partial_for.concept_arguments[0]
        assocs[merge_key.address].append(x)
    final: list[list[BuildDatasource]] = []
    for _, dses in assocs.items():
        if not dses or not dses[0].non_partial_for:
            continue
        merge_key = dses[0].non_partial_for.concept_arguments[0]
        if isinstance(merge_key.datatype, EnumType):
            result = _best_enum_union(dses, merge_key.datatype, merge_key)
            if result:
                final.append(result)
        else:
            conditions = [
                c.non_partial_for.conditional for c in dses if c.non_partial_for
            ]
            if simplify_conditions(conditions):
                final.append(dses)
    return final
