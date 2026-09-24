"""A filter concept (`filter X where COND`, `X ? COND`) is a VALUE: `X` where
the predicate holds, NULL elsewhere. In a SELECT it narrows only itself, never
the other columns' rows. The one shape whose rows it does narrow is a statement
showing nothing but filter values over one predicate, where a NULL row is one
nothing would keep. Materialization invariance cannot judge this (a bound
column restricting the stream is the same bug), so the rows are pinned here.
"""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

_MODEL = """
key product_id int;
property product_id.name string;
key order_id int;
property order_id.quantity int;

root datasource products (product_id: product_id, name: name)
grain (product_id)
query '''
select 1 as product_id, 'apple' as name union all
select 2, 'bean' union all
select 3, 'corn' union all
select 4, 'date'
''';

root datasource orders (order_id: order_id, product_id: product_id, quantity: quantity)
grain (order_id)
query '''
select 100 as order_id, 1 as product_id, 5 as quantity union all
select 101, 1, 20 union all
select 102, 2, 8
''';

auto even_name <- filter name where product_id % 2 = 0;
auto n_orders <- count(order_id) by product_id;
auto popular_name <- filter name where n_orders > 1;
auto bulk_name <- filter name where quantity > 10;
"""


def _rows(executor: Executor, query: str) -> list[tuple]:
    rows = [tuple(r) for r in executor.execute_text(query + ";")[-1].fetchall()]
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


@pytest.fixture(scope="module")
def executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_MODEL)
    return executor


def test_beside_its_key_every_row_survives(executor: Executor):
    assert _rows(executor, "select product_id, even_name") == [
        (1, None),
        (2, "bean"),
        (3, None),
        (4, "date"),
    ]


def test_beside_its_content_every_row_survives(executor: Executor):
    assert _rows(executor, "select name, even_name") == [
        ("apple", None),
        ("bean", "bean"),
        ("corn", None),
        ("date", "date"),
    ]


def test_beside_a_finer_key_every_row_survives(executor: Executor):
    assert _rows(executor, "select order_id, even_name") == [
        (100, None),
        (101, None),
        (102, "bean"),
    ]


def test_aggregate_predicate_beside_its_key(executor: Executor):
    """The count's population is products with an order: the value is a
    function of `n_orders`, so those are the rows."""
    assert _rows(executor, "select product_id, popular_name") == [
        (1, "apple"),
        (2, None),
    ]


def test_alone_it_is_the_population(executor: Executor):
    assert _rows(executor, "select even_name") == [("bean",), ("date",)]
    assert _rows(executor, "select popular_name") == [("apple",)]
    assert _rows(executor, "select bulk_name") == [("apple",)]


def test_predicate_finer_than_the_content_collapses_to_its_grain(executor: Executor):
    """`name` if ANY of the product's orders is bulk: one row per product, not
    one per order fanning the value out into {name, NULL}."""
    assert _rows(executor, "select product_id, bulk_name") == [
        (1, "apple"),
        (2, None),
    ]
    assert _rows(executor, "select name, bulk_name") == [
        ("apple", "apple"),
        ("bean", None),
    ]


def test_grouped_by_the_value_the_null_group_stays(executor: Executor):
    assert _rows(executor, "select even_name, count(order_id) as n") == [
        ("bean", 1),
        (None, 2),
    ]
