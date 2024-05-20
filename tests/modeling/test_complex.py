from preql.core.models import Environment
from preql import parse, Executor


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
