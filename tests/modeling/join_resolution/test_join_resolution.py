from trilogy.core.models import Environment, Grain
from trilogy.core.enums import Purpose
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy import parse, Executor
import pytest


def test_ambiguous_error(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    store_id, 
    product_id,
;"""

    _, statements = parse(test_select, test_environment)
    statements[-1]

    with pytest.raises(AmbiguousRelationshipResolutionException):
        list(test_executor.execute_text(test_select)[0].fetchall())


def test_ambiguous_error_with_forced_join(
    test_environment: Environment, test_executor: Executor
):
    # check we can resolve it
    test_select = """
property store_by_warehouse <- group(store_id) by wh_id;
property store_by_order <- group(store_id) by order_id;
"""

    _, statements = parse(test_select, test_environment)
    grouped = test_environment.concepts["store_by_warehouse"]
    assert grouped.purpose == Purpose.PROPERTY
    assert grouped.lineage.arguments == [
        test_environment.concepts["store_id"],
        test_environment.concepts["wh_id"],
    ]
    assert grouped.grain.components == [
        test_environment.concepts["store_id"],
        test_environment.concepts["wh_id"],
    ]
    test_select = """
SELECT
    store_by_warehouse,
    product_id,
;"""

    _, statements = parse(test_select, test_environment)

    results = test_executor.execute_text(test_select)[0].fetchall()

    assert len(results) == 3


def test_ambiguous_error_with_forced_join_order(
    test_environment: Environment, test_executor: Executor
):
    # check we can resolve it
    test_select = """
property store_by_warehouse <- group(store_id) by wh_id;
property store_by_order <- group(store_id) by order_id;
"""

    _, statements = parse(test_select, test_environment)
    grouped = test_environment.concepts["store_by_order"]
    assert grouped.purpose == Purpose.PROPERTY
    target_grain = Grain(
        components=[
            test_environment.concepts["store_id"],
            test_environment.concepts["order_id"],
        ]
    )
    assert Grain(components=grouped.lineage.arguments) == target_grain
    assert grouped.grain == target_grain
    test_select = """
SELECT
    store_by_order,
    product_id,
;"""

    _, statements = parse(test_select, test_environment)

    results = test_executor.execute_text(test_select)[0].fetchall()

    # different when we group via order
    assert len(results) == 5
