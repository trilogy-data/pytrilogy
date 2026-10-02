"""A WHERE over an aggregate by a key, beside that key and one of its
properties. The property peels into a dimension bucket of its own, and the
atom's value comes from a condition branch no bucket holds as a column."""

import pytest

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


# The condition branch is grouped by a DERIVED concept: its host must carry
# that grain to join the branch on it (the ROOT scan has no `status`; it
# rendered `Missing source map entry for local.status`).
def test_aggregate_by_a_derived_grain_in_where():
    executor = _executor()
    assert _rows(
        executor, "select customer_id, status where count(order_id) by status > 1;"
    ) == [(1, "delivered"), (2, "delivered")]
    assert _rows(
        executor, "select order_id, status where count(order_id) by status > 1;"
    ) == [(100, "delivered"), (102, "delivered")]
    assert _rows(
        executor, "select customer_id, name where count(order_id) by status > 1;"
    ) == [(1, "ann"), (2, "bob")]


MEMBERSHIP_MODEL = """
key id int;
property id.x int;
property id.g string;
key v int;

root datasource t (id: id, x: x, g: g)
grain (id)
query '''
select 1 as id, 10 as x, 'a' as g union all
select 2, 20, 'a' union all
select 3, 30, 'b' union all
select 4, 40, 'b'
''';

root datasource vs (v: v)
grain (v)
query '''select 20 as v union all select 60 union all select 30''';

auto m <- sum(x) by g;
auto fx <- x ? x > 15;
"""


@pytest.mark.parametrize(
    "query, expected",
    [
        ("select g where m in v;", [("a",)]),
        ("select g where m not in v;", [("b",)]),
        ("select g where m in v and g = 'b';", []),
        ("select g where m in v or g = 'b';", [("a",), ("b",)]),
        ("select g, m where m in v;", [("a", 30)]),
        ("select id where m in v;", [(1,), (2,)]),
        ("select id where m + 0 in v;", [(1,), (2,)]),
        ("select id where m in v and x > 15;", [(2,)]),
        ("select id where x in v or m in v;", [(1,), (2,), (3,)]),
        ("select g where m in fx;", [("a",)]),
        ("auto m2 <- m + 0; select id where m2 in v;", [(1,), (2,)]),
        (
            "auto dy <- fx + 0; auto lo <- min(dy) by g; select id, g where lo in fx;",
            [(1, "a"), (2, "a"), (3, "b"), (4, "b")],
        ),
    ],
)
def test_unselected_aggregate_membership_filters(query: str, expected: list[tuple]):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MEMBERSHIP_MODEL)
    assert _rows(executor, query) == expected
