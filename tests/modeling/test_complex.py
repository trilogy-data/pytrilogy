from preql.core.models import Environment
from preql.core.enums import Purpose
from preql import parse, Executor
from preql.core.processing.node_generators import gen_select_node
from preql.core.env_processor import generate_graph
import pytest


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
