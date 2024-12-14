from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import List, Tuple, TypeVar

from trilogy.core.enums import ComparisonOperator
from trilogy.core.models import (
    Comparison,
    Concept,
    Conditional,
    Datasource,
    DataType,
    Function,
    FunctionType,
    Parenthetical,
)

# Define a generic type that ensures start and end are the same type
T = TypeVar("T", int, date, datetime)


def reduce_expression(
    var: Concept, group_tuple: list[tuple[ComparisonOperator, T]]
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
    else:
        raise ValueError(f"Invalid datatype: {var.datatype}")

    ranges: list[Tuple[T, T]] = []
    for op, value in group_tuple:
        increment: int | timedelta
        if isinstance(value, date):
            increment = timedelta(days=1)
        elif isinstance(value, datetime):
            increment = timedelta(seconds=1)
        elif isinstance(value, int):
            increment = 1
        # elif isinstance(value, float):
        #     value = Decimal(value)
        #     increment = Decimal(0.0000000001)

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
        else:
            raise ValueError(f"Invalid operator: {op}")
    return is_fully_covered(lower_check, upper_check, ranges, increment)


def simplify_conditions(
    conditions: list[Comparison | Conditional | Parenthetical],
) -> bool:
    # Group conditions by variable
    grouped: dict[Concept, list[tuple[ComparisonOperator, datetime | int | date]]] = (
        defaultdict(list)
    )
    for condition in conditions:
        if not isinstance(condition, Comparison):
            return False
        if not isinstance(
            condition.left, (int, date, datetime, Function)
        ) and not isinstance(condition.right, (int, date, datetime, Function)):
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
            first_arg = comparison.arguments[0]
            if not isinstance(first_arg, (int, date, datetime)):
                return False
            comparison = first_arg
        if not isinstance(comparison, (int, date, datetime)):
            return False

        var = concept
        op = condition.operator
        grouped[var].append((op, comparison))

    simplified = []
    for var, group_tuple in grouped.items():
        simplified.append(reduce_expression(var, group_tuple))  # type: ignore

    # Final simplification
    return True if all(isinstance(s, bool) and s for s in simplified) else False


def is_fully_covered(
    start: T,
    end: T,
    ranges: List[Tuple[T, T]],
    increment: int | timedelta,
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
    print(ranges)
    for r_start, r_end in ranges:
        print(r_start, r_end)
        # If there's a gap between the current range and the previous coverage
        print(r_start - current_end)
        if (r_start - current_end) > increment:  # type: ignore
            print("gap")
            return False
        print("okay")
        # Extend the current coverage
        current_end = max(current_end, r_end)

    # If the loop ends and we haven't reached the end, return False
    print(current_end, end)
    print(current_end >= end)
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

    assocs: dict[str, list[Datasource]] = defaultdict(list[Datasource])
    for x in candidates:
        if not x.non_partial_for:
            continue
        if not len(x.non_partial_for.concept_arguments) == 1:
            continue
        merge_key = x.non_partial_for.concept_arguments[0]
        assocs[merge_key.address].append(x)
    final: list[list[Datasource]] = []
    for _, dses in assocs.items():
        conditions = [c.non_partial_for.conditional for c in dses if c.non_partial_for]
        if simplify_conditions(conditions):
            final.append(dses)
    return final
