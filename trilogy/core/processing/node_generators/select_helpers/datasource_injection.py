from collections import defaultdict
from datetime import date, datetime, timedelta

from trilogy.core.enums import ComparisonOperator
from trilogy.core.models import (
    Comparison,
    Concept,
    Datasource,
    DataType,
    Function,
    FunctionType,
)


def simplify_conditions(conditions: list[Comparison]) -> bool:
    """
    Simplify a set of boolean conditions involving number or date inequalities.
    Conditions are represented as a list of tuples: ('x', '>', 1), ('x', '<=', 1).
    """
    # Group conditions by variable
    grouped: dict[str, tuple[ComparisonOperator, str | int | float | date]] = (
        defaultdict(list)
    )
    for condition in conditions:
        if not isinstance(
            condition.left, (int, float, date, Function)
        ) and not isinstance(condition.right, (int, float, date, Function)):
            return False
        if not isinstance(condition.left, Concept) and not isinstance(
            condition.right, Concept
        ):
            return False
        vars = [condition.left, condition.right]
        concept = [x for x in vars if isinstance(x, Concept)][0]
        comparison = [x for x in vars if not isinstance(x, Concept)][0]
        if isinstance(comparison, Function):
            if not comparison.operator == FunctionType.CONSTANT:
                return False
            comparison = comparison.arguments[0]

        var = concept
        op = condition.operator
        grouped[var].append((op, comparison))

    simplified = []
    for var, var_conditions in grouped.items():
        # Track ranges
        if var.datatype in (DataType.INTEGER, DataType.FLOAT):
            lower_check = float("-inf")
            upper_check = float("inf")
            increment = 0.0000001
        elif var.datatype in (DataType.DATE,):
            lower_check = date.min
            upper_check = date.max
            increment = timedelta(days=1)
        elif var.datatype in (DataType.DATETIME,):
            lower_check = date.min
            upper_check = date.max
            increment = timedelta(seconds=1)

        ranges = []
        for op, value in var_conditions:
            if op == ">":
                ranges.append([value + increment, upper_check])
            elif op == ">=":
                ranges.append([value, upper_check])
            elif op == "<":
                ranges.append([lower_check, value - increment])
            elif op == "<=":
                ranges.append([lower_check, value])
            elif op == "=":
                ranges.append([value, value])
            else:
                raise

        simplified.append(is_fully_covered(lower_check, upper_check, ranges, increment))

    # Final simplification
    return True if all(isinstance(s, bool) and s for s in simplified) else False


def is_fully_covered(
    start: float | int | date | datetime,
    end: float | int | date | datetime,
    ranges,
    increment: float | int | timedelta,
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
    # Sort ranges by their start values (and by end values for ties)
    ranges.sort()

    # Check for gaps
    current_end = start
    for r_start, r_end in ranges:
        # If there's a gap between the current range and the previous coverage
        if r_start - current_end > increment:
            return False
        if r_start < current_end:
            return False
        # Extend the current coverage
        current_end = max(current_end, r_end)

    # If the loop ends and we haven't reached the end, return False
    return current_end >= end


def get_union_sources(datasources: list[Datasource], concepts: list[Concept]):
    candidates: list[Datasource] = []
    for x in datasources:
        if all([c.address in x.output_concepts for c in concepts]):
            if (
                any([c.address in x.partial_concepts for c in concepts])
                and x.non_partial_for
            ):
                candidates.append(x)

    assocs = defaultdict(list[Datasource])
    for x in candidates:

        if not len(x.non_partial_for.concept_arguments) == 1:
            continue
        merge_key = x.non_partial_for.concept_arguments[0]
        assocs[merge_key.address].append(x)
    final: list[list[Datasource]] = []
    for _, dses in assocs.items():
        conditions = [c.non_partial_for.conditional for c in dses]
        print(conditions)
        if simplify_conditions(conditions):
            final.append(dses)
    return final
