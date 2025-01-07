from trilogy import Dialects, Executor, parse
from trilogy.core.enums import Derivation
from trilogy.core.models_environment import Environment
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.statements_author import SelectStatement


def test_rowset(test_environment: Environment, test_executor: Executor):
    test_select = """

    rowset even_orders <- select order_id, store_id where order_id % 2 = 0;
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
    test_select = """   auto even_order_store_revenue <- sum(even_orders.revenue);
    rowset even_orders <- select order_id, store_id, revenue where (order_id % 2) = 0;
    auto even_order_store_revenue <- sum(even_orders.revenue);
    auto even_order_count <-count(even_orders.order_id);
    SELECT
        even_orders.store_id,
        even_order_count,
        even_order_store_revenue,
    order by 
        even_order_count desc, 
        even_orders.store_id asc
    ;"""

    _, statements = parse(test_select, test_environment)

    group = test_environment.concepts["even_order_store_revenue"]

    assert group.derivation == Derivation.AGGREGATE
    # grain_c_lcl = LooseConceptList(concepts=group.grain.components_copy)
    # assert "even_orders.store_id" in grain_c_lcl.addresses
    resolve_function_parent_concepts(group, environment=test_environment)

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
    order by filtered asc, order_id asc
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 2
    assert results[0] == (1, 1)
    assert results[1] == (None, 1)


def test_window_alt(test_environment: Environment, test_executor: Executor):
    test_select = """
    auto nums <- unnest([1,2]);

    auto filtered <- row_number nums;

    SELECT
        filtered
    where
        filtered = 1
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == (1,)


def test_maps():
    test_executor = Dialects.DUCK_DB.default_executor()
    test_select = """
    const num_map <- {1: 10, 2: 20};

    SELECT
        num_map[1] -> num_map_1
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == (10,)


def test_anon_agg():
    test_executor = Dialects.DUCK_DB.default_executor()
    test_select = """
    auto nums <- [1,2];

    SELECT
    sum(unnest(nums)+1) -> sum_plus_1
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == (5,)
