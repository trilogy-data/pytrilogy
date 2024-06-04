from preql.core.models import Environment
from preql import parse, Executor
from preql.core.enums import Purpose, PurposeLineage
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from preql.core.models import LooseConceptList


def test_rowset(test_environment: Environment, test_executor: Executor):
    test_select = """

    rowset even_orders <- select order_id, store_id where (order_id % 2) = 0;
    SELECT
        even_orders.order_id,
        even_orders.store_id
    order by even_orders.order_id asc
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    # assert len(results) == 3
    assert results[0] == (2, 1)
    assert results[1] == (4, 2)


def test_rowset_with_addition(test_environment: Environment, test_executor: Executor):
    test_select = """

    rowset even_orders <- select order_id, store_id where (order_id % 2) = 0;
    SELECT
        order_id,
        even_orders.order_id,
        even_orders.store_id
    order by order_id asc
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    # assert len(results) == 3
    assert results[0] == (1, None, None)
    assert results[1] == (2, 2, 1)
    assert results[2] == (3, None, None)
    assert results[3] == (4, 4, 2)


def test_rowset_with_aggregation(
    test_environment: Environment, test_executor: Executor
):
    test_select = """

    rowset even_orders <- select order_id, store_id where (order_id % 2) = 0;
    SELECT
        even_orders.store_id,
        count(even_orders.order_id) -> even_order_count,
        sum(revenue) by even_orders.store_id -> even_order_store_revenue,
    order by 
        even_order_count desc, 
        even_orders.store_id asc
    ;"""

    _, statements = parse(test_select, test_environment)

    group = test_environment.concepts["even_order_store_revenue"]

    assert group.derivation == PurposeLineage.AGGREGATE
    grain_c_lcl = LooseConceptList(group.grain.components_copy)
    assert "even_orders.store_id" in grain_c_lcl.addresses
    x = resolve_function_parent_concepts(group)

    x_lcl = LooseConceptList(x)

    assert "local.revenue" in x_lcl
    assert "even_orders.store_id" in x_lcl

    for count in [
        test_environment.concepts["local.even_order_count"],
        [x for x in statements[-1].output_components if x.name == "even_order_count"][
            0
        ],
    ]:

        assert count.derivation == PurposeLineage.AGGREGATE
        assert count.purpose == Purpose.METRIC
        count_grain_lcl = LooseConceptList(count.grain.components_copy)
        assert "even_orders.store_id" in count_grain_lcl.addresses
        assert "local.even_order_count" not in count_grain_lcl.addresses
        count_parents = resolve_function_parent_concepts(count)

        count_lcl = LooseConceptList(count_parents)
        assert "even_orders.store_id" in count_lcl
        assert "local.even_order_count" not in count_lcl
    results = list(test_executor.execute_text(test_select)[0].fetchall())
    # assert len(results) == 3
    assert results[0] == (1, 1, 10.0)
    assert results[1] == (2, 1, 5.0)
