from datetime import date, datetime

from trilogy.core.enums import ComparisonOperator
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildGrain,
    DataType,
    Purpose,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    simplify_conditions,
)

test_concept = BuildConcept(
    name="test_concept",
    canonical_name="test_concept",
    datatype=DataType.INTEGER,
    purpose=Purpose.KEY,
    build_is_aggregate=False,
    grain=BuildGrain(),
)

float_concept = BuildConcept(
    name="float_concept",
    canonical_name="float_concept",
    datatype=DataType.FLOAT,
    purpose=Purpose.KEY,
    build_is_aggregate=False,
    grain=BuildGrain(),
)

date_concept = BuildConcept(
    name="date_concept",
    canonical_name="date_concept",
    datatype=DataType.DATE,
    purpose=Purpose.KEY,
    build_is_aggregate=False,
    grain=BuildGrain(),
)

datetime_concept = BuildConcept(
    name="datetime_concept",
    canonical_name="datetime_concept",
    datatype=DataType.DATETIME,
    purpose=Purpose.KEY,
    build_is_aggregate=False,
    grain=BuildGrain(),
)

boolean_concept = BuildConcept(
    name="boolean_concept",
    canonical_name="boolean_concept",
    datatype=DataType.BOOL,
    purpose=Purpose.KEY,
    build_is_aggregate=False,
    grain=BuildGrain(),
)


def test_reduce_expression_integer():
    # test mathematical ranges
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.EQ
                )
            ]
        )
        is False
    ), "Expected False for single value comparison"
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.GT
                ),
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.LT
                ),
            ]
        )
        is True
    ), "Expected True for x > 1, x = 1, x < 1 as it covers the whole range"
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.GT
                ),
                BuildComparison(
                    left=test_concept, right=2, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=test_concept, right=1, operator=ComparisonOperator.LT
                ),
            ]
        )
        is True
    ), "Expected True for x > 1, x = 1, x=2, x < 1 as it covers the whole range"


def test_reduce_expression_float():
    # Test single float value
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=float_concept, right=1.5, operator=ComparisonOperator.EQ
                )
            ]
        )
        is False
    ), "Expected False for single float value comparison"

    # Test float range coverage
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=float_concept, right=1.5, operator=ComparisonOperator.GT
                ),
                BuildComparison(
                    left=float_concept, right=1.5, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=float_concept, right=1.5, operator=ComparisonOperator.LT
                ),
            ]
        )
        is True
    ), "Expected True for float x > 1.5, x = 1.5, x < 1.5 as it covers the whole range"

    # Test partial range
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=float_concept, right=1.5, operator=ComparisonOperator.GT
                ),
                BuildComparison(
                    left=float_concept, right=2.5, operator=ComparisonOperator.LT
                ),
            ]
        )
        is True
    ), "Expected True for partial float range 1.5 < x < 2.5"


def test_reduce_expression_date():
    test_date = date(2024, 1, 1)

    # Test single date value
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=date_concept, right=test_date, operator=ComparisonOperator.EQ
                )
            ]
        )
        is False
    ), "Expected False for single date value comparison"

    # Test date range coverage
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=date_concept, right=test_date, operator=ComparisonOperator.GT
                ),
                BuildComparison(
                    left=date_concept, right=test_date, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=date_concept, right=test_date, operator=ComparisonOperator.LT
                ),
            ]
        )
        is True
    ), "Expected True for date x > test_date, x = test_date, x < test_date as it covers the whole range"

    # Test date range with GTE and LTE (OR conditions cover all dates)
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=date_concept, right=test_date, operator=ComparisonOperator.GTE
                ),
                BuildComparison(
                    left=date_concept,
                    right=date(2024, 12, 31),
                    operator=ComparisonOperator.LTE,
                ),
            ]
        )
        is True
    ), "Expected True for date x >= 2024-01-01 OR x <= 2024-12-31 as it covers all dates"

    # Test non-overlapping date ranges
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=date_concept,
                    right=date(2024, 6, 1),
                    operator=ComparisonOperator.GT,
                ),
                BuildComparison(
                    left=date_concept,
                    right=date(2024, 3, 1),
                    operator=ComparisonOperator.LT,
                ),
            ]
        )
        is False
    ), "Expected False for non-overlapping date ranges x > 2024-06-01 OR x < 2024-03-01"


def test_reduce_expression_datetime():
    test_datetime = datetime(2024, 1, 1, 12, 0, 0)

    # Test single datetime value
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=datetime_concept,
                    right=test_datetime,
                    operator=ComparisonOperator.EQ,
                )
            ]
        )
        is False
    ), "Expected False for single datetime value comparison"

    # Test datetime range coverage
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=datetime_concept,
                    right=test_datetime,
                    operator=ComparisonOperator.GT,
                ),
                BuildComparison(
                    left=datetime_concept,
                    right=test_datetime,
                    operator=ComparisonOperator.EQ,
                ),
                BuildComparison(
                    left=datetime_concept,
                    right=test_datetime,
                    operator=ComparisonOperator.LT,
                ),
            ]
        )
        is True
    ), "Expected True for datetime x > test_datetime, x = test_datetime, x < test_datetime as it covers the whole range"

    # Test datetime range with GTE and LTE (OR conditions cover all datetimes)
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=datetime_concept,
                    right=test_datetime,
                    operator=ComparisonOperator.GTE,
                ),
                BuildComparison(
                    left=datetime_concept,
                    right=datetime(2024, 12, 31, 23, 59, 59),
                    operator=ComparisonOperator.LTE,
                ),
            ]
        )
        is True
    ), "Expected True for datetime x >= test_datetime OR x <= end_datetime as it covers all datetimes"

    # Test non-overlapping datetime ranges
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=datetime_concept,
                    right=datetime(2024, 6, 1, 12, 0, 0),
                    operator=ComparisonOperator.GT,
                ),
                BuildComparison(
                    left=datetime_concept,
                    right=datetime(2024, 3, 1, 12, 0, 0),
                    operator=ComparisonOperator.LT,
                ),
            ]
        )
        is False
    ), "Expected False for non-overlapping datetime ranges"


def test_reduce_expression_boolean():
    # Test single boolean value
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.GTE
                )
            ]
        )
        is False
    ), "Expected False for single boolean value comparison"

    # Test single boolean value
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.EQ
                )
            ]
        )
        is False
    ), "Expected False for single boolean value comparison"

    # Test both boolean values covered
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=boolean_concept, right=False, operator=ComparisonOperator.EQ
                ),
            ]
        )
        is True
    ), "Expected True for boolean x = True, x = False as it covers all boolean values"

    # Test boolean with NOT_EQ
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.NE
                )
            ]
        )
        is False
    ), "Expected False for boolean x != True (equivalent to x = False)"

    # Test boolean range-like operations (though not typical for booleans)
    assert (
        simplify_conditions(
            [
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.GT
                ),
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.EQ
                ),
                BuildComparison(
                    left=boolean_concept, right=True, operator=ComparisonOperator.LT
                ),
            ]
        )
        is True
    ), "Expected True for boolean x > True, x = True, x < True as it covers the whole range"
