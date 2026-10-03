"""A filter concept (`filter X where COND`, `X ? COND`) is a VALUE: `X` where
the predicate holds, NULL elsewhere. In a SELECT it narrows only itself, never
the other columns' rows. The one shape whose rows it does narrow is a statement
showing nothing but filter values over one predicate, where a NULL row is one
nothing would keep. Materialization invariance cannot judge this (a bound
column restricting the stream is the same bug), so the rows are pinned here.
"""

import pytest

from tests.helpers.models import PRODUCT_ORDERS
from tests.helpers.rows import executor_for, sorted_rows
from trilogy.core.models.execute import DatasourceCTE
from trilogy.executor import Executor


@pytest.fixture(scope="module")
def executor() -> Executor:
    return executor_for(PRODUCT_ORDERS)


def test_beside_its_key_every_row_survives(executor: Executor):
    assert sorted_rows(executor, "select product_id, even_name") == [
        (1, None),
        (2, "bean"),
        (3, None),
        (4, "date"),
    ]


def test_beside_its_content_every_row_survives(executor: Executor):
    assert sorted_rows(executor, "select name, even_name") == [
        ("apple", None),
        ("bean", "bean"),
        ("corn", None),
        ("date", "date"),
    ]


def test_beside_a_finer_key_every_row_survives(executor: Executor):
    assert sorted_rows(executor, "select order_id, even_name") == [
        (100, None),
        (101, None),
        (102, "bean"),
    ]


def test_aggregate_predicate_beside_its_key(executor: Executor):
    """The count's population is products with an order: the value is a
    function of `n_orders`, so those are the rows."""
    assert sorted_rows(executor, "select product_id, popular_name") == [
        (1, "apple"),
        (2, None),
    ]


def test_alone_it_is_the_population(executor: Executor):
    assert sorted_rows(executor, "select even_name") == [("bean",), ("date",)]
    assert sorted_rows(executor, "select popular_name") == [("apple",)]
    assert sorted_rows(executor, "select bulk_name") == [("apple",)]


def test_predicate_finer_than_the_content_collapses_to_its_grain(executor: Executor):
    """`name` if ANY of the product's orders is bulk: one row per product, not
    one per order fanning the value out into {name, NULL}."""
    assert sorted_rows(executor, "select product_id, bulk_name") == [
        (1, "apple"),
        (2, None),
    ]
    assert sorted_rows(executor, "select name, bulk_name") == [
        ("apple", "apple"),
        ("bean", None),
    ]


def test_a_having_aggregate_is_not_shown(executor: Executor):
    """A HAVING's aggregate rides the projection hidden; the statement still
    shows nothing but the filter value, so its NULL rows are dropped and the
    predicate is a WHERE, not a CASE (TPC-DS q41). Orderless products are
    absent by the model's complete binding, not by the filter."""
    query = "select even_name having n_orders >= 0"
    assert sorted_rows(executor, query) == [("bean",)]
    sql = executor.generate_sql(query + ";")[-1]
    assert "CASE" not in sql, sql


def test_a_having_responsive_aggregate_is_not_shown(executor: Executor):
    """`count(order_id)` in the HAVING is grouped by the shown value, so it
    reads the unfiltered orders only for what rides the filter's own row
    stream: the predicate is a WHERE, and the odd products' NULL group (two
    orders between them) is not a row nobody would keep. The filter node
    collapses into the aggregate's SELECT over scans that each carry the
    predicate, INNER-joined, so its CASE is not rendered."""
    assert sorted_rows(executor, "select even_name having count(order_id) > 1") == []
    assert sorted_rows(executor, "select even_name having count(order_id) >= 1") == [
        ("bean",)
    ]
    assert sorted_rows(executor, "select even_name having sum(quantity) > 5") == [
        ("bean",)
    ]
    query = "select even_name having count(order_id) > 1;"
    scans = [
        cte
        for cte in executor.parse_text(query)[-1].ctes
        if isinstance(cte, DatasourceCTE)
    ]
    assert len(scans) == 2 and all(cte.condition is not None for cte in scans)
    assert "CASE" not in executor.generate_sql(query)[-1]


def test_grouped_by_the_value_the_null_group_stays(executor: Executor):
    assert sorted_rows(executor, "select even_name, count(order_id) as n") == [
        ("bean", 1),
        (None, 2),
    ]
