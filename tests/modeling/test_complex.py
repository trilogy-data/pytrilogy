from trilogy.core.models import Environment, SelectStatement
from trilogy import parse, Executor
from trilogy.core.enums import Purpose, PurposeLineage
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.models import LooseConceptList
from trilogy.core.query_processor import get_query_datasources


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

    rowset even_orders <- select order_id, store_id, revenue where (order_id % 2) = 0;
    SELECT
        even_orders.store_id,
        count(even_orders.order_id) -> even_order_count,
        sum(even_orders.revenue) by even_orders.store_id -> even_order_store_revenue,
    order by 
        even_order_count desc, 
        even_orders.store_id asc
    ;"""

    _, statements = parse(test_select, test_environment)

    group = test_environment.concepts["even_order_store_revenue"]

    assert group.derivation == PurposeLineage.AGGREGATE
    grain_c_lcl = LooseConceptList(concepts=group.grain.components_copy)
    assert "even_orders.store_id" in grain_c_lcl.addresses
    x = resolve_function_parent_concepts(group)

    x_lcl = LooseConceptList(concepts=x)

    # assert "local.revenue" in x_lcl
    assert "even_orders.store_id" in x_lcl

    for count in [
        test_environment.concepts["local.even_order_count"],
        [x for x in statements[-1].output_components if x.name == "even_order_count"][
            0
        ],
    ]:

        assert count.derivation == PurposeLineage.AGGREGATE
        assert count.purpose == Purpose.METRIC
        count_grain_lcl = LooseConceptList(concepts=count.grain.components_copy)
        assert "even_orders.store_id" in count_grain_lcl.addresses
        assert "local.even_order_count" not in count_grain_lcl.addresses
        count_parents = resolve_function_parent_concepts(count)

        count_lcl = LooseConceptList(concepts=count_parents)
        assert "even_orders.store_id" in count_lcl
        assert "local.even_order_count" not in count_lcl
    results = list(test_executor.execute_text(test_select)[0].fetchall())
    # assert len(results) == 3
    assert results[0] == (1, 1, 10.0)
    assert results[1] == (2, 1, 5.0)


def test_in_select(test_environment: Environment, test_executor: Executor):
    test_select = """
    auto even_stores <- unnest([1,2,3,4]);

    auto filtered <- filter order_id where order_id in even_stores and order_id %2 = 0;

    SELECT
        order_id,
    WHERE
        order_id in filtered

    ;"""
    _, statements = parse(test_select, test_environment)

    select: SelectStatement = statements[-1]
    assert select.where_clause.conditional.existence_arguments, str(
        select.where_clause.conditional.__class__
    ) + str(select.where_clause.conditional)
    assert select.where_clause.existence_arguments

    datasource = get_query_datasources(test_environment, select)

    assert "local.filtered" in datasource.existence_source_map

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 2
    assert results[0] == (2,)
    assert results[1] == (4,)


def test_window_clone(test_environment: Environment, test_executor: Executor):
    test_select = """
    auto nums <- unnest([1,2]);

    auto filtered <- lag nums order by nums asc;

    SELECT
        filtered,
        order_id,
    WHERE
        order_id in filtered
    order by filtered asc
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 2
    assert results[0] == (1, 1)
    assert results[1] == (None, 1)
