"""A WHERE over an aggregate by a key, beside that key and one of its
properties. The property peels into a dimension bucket of its own, and the
atom's value comes from a condition branch no bucket holds as a column."""

from trilogy import Dialects
from trilogy.executor import Executor

MODEL = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.delivery_date date?;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''
select 1 as customer_id, 'ann' as name union all
select 2, 'bob' union all
select 3, 'cat'
''';

root datasource orders (
    order_id: order_id, customer_id: customer_id, delivery_date: delivery_date,
)
grain (order_id)
query '''
select 100 as order_id, 1 as customer_id, date '2026-01-01' as delivery_date union all
select 101, 1, null union all
select 102, 2, date '2026-01-02'
''';

auto n_orders <- count(order_id) by customer_id;
auto activity <- case when n_orders > 1 then 'repeat' else 'single' end;
auto status <- case when delivery_date is not null then 'delivered' else 'in-transit' end;
"""


def _rows(executor: Executor, query: str) -> list[tuple]:
    return sorted(tuple(r) for r in executor.execute_text(query)[-1].fetchall())


def _executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    return executor


def test_key_beside_its_property_keeps_the_aggregate_where():
    executor = _executor()
    assert _rows(executor, "select customer_id, name where n_orders > 1;") == [
        (1, "ann")
    ]
    assert _rows(executor, "select customer_id, name where activity = 'single';") == [
        (2, "bob")
    ]


def test_key_alone_keeps_the_aggregate_where():
    executor = _executor()
    assert _rows(executor, "select customer_id where n_orders > 1;") == [(1,)]
    assert _rows(executor, "select name where activity = 'single';") == [("bob",)]


def test_scalar_over_aggregate_where_beside_a_row_derivation():
    executor = _executor()
    assert _rows(executor, "select customer_id, status where activity = 'repeat';") == [
        (1, "delivered"),
        (1, "in-transit"),
    ]
    assert _rows(executor, "select customer_id, status where n_orders > 1;") == [
        (1, "delivered"),
        (1, "in-transit"),
    ]
