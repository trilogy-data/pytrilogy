"""A NULL-absorbing select output (COALESCE, CASE ... ELSE) is evaluated on the
select's row: the select keys its reads do not cover are its inputs, as a bare
aggregate's `by` is the select's grain. A column persisting it at its reads'
own grain answers only a select at that grain."""

from functools import cache

import pytest

from tests.helpers.models import CUSTOMERS_DERIVED
from tests.helpers.rows import executor_for, sorted_rows
from trilogy.core.exceptions import NoDatasourceException
from trilogy.executor import Executor

# sentinels: a read of the persisted column is visible in the rows
_ORDER_CACHE = """
datasource order_cache (order_id: order_id, amount_or_zero: amount_or_zero, status: status)
grain (order_id)
query '''
select 100 as order_id, -10 as amount_or_zero, 'CACHED' as status union all
select 101, -20, 'CACHED' union all
select 102, -30, 'CACHED'
''';
"""


@cache
def _executor() -> Executor:
    return executor_for(CUSTOMERS_DERIVED + _ORDER_CACHE)


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select customer_id, amount_or_zero",
            [(1, 10), (1, 20), (2, 30), (3, 0)],
        ),
        (
            "select customer_id, coalesce(amount, 0) as a",
            [(1, 10), (1, 20), (2, 30), (3, 0)],
        ),
        (
            "select customer_id, order_id, amount_or_zero",
            [(1, 100, 10), (1, 101, 20), (2, 102, 30), (3, None, 0)],
        ),
        (
            "select name, status",
            [
                ("ann", "delivered"),
                ("ann", "in-transit"),
                ("bob", "delivered"),
                ("cat", "in-transit"),
            ],
        ),
        (
            "select customer_id, order_id, status, amount_or_zero",
            [
                (1, 100, "delivered", 10),
                (1, 101, "in-transit", 20),
                (2, 102, "delivered", 30),
                (3, None, "in-transit", 0),
            ],
        ),
    ],
)
def test_fallback_fires_on_the_select_row(query: str, expected: list[tuple]):
    assert sorted_rows(_executor(), query) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("select order_id, amount_or_zero", [(100, -10), (101, -20), (102, -30)]),
        (
            "select order_id, status",
            [(100, "CACHED"), (101, "CACHED"), (102, "CACHED")],
        ),
    ],
)
def test_persisted_column_answers_its_own_grain(query: str, expected: list[tuple]):
    assert sorted_rows(_executor(), query) == expected


def test_window_partition_is_not_a_select_key():
    assert sorted_rows(_executor(), "select status, order_seq") == [
        ("delivered", 1),
        ("in-transit", 2),
    ]


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select status, count(customer_id) as n",
            [("delivered", 2), ("in-transit", 1), (None, 1)],
        ),
        (
            "select status as s2, sum(amount) as a by rollup (status)",
            [("delivered", 40), ("in-transit", 20), (None, 60)],
        ),
    ],
)
def test_no_other_select_key_leaves_it_unpinned(query: str, expected: list[tuple]):
    assert sorted_rows(_executor(), query) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select customer_id, status, count(order_id) as n",
            [
                (1, "delivered", 1),
                (1, "in-transit", 1),
                (2, "delivered", 1),
                (3, "in-transit", 0),
            ],
        ),
        ("select customer_id where status = 'in-transit'", [(1,), (3,)]),
        ("select customer_id where coalesce(amount, 0) = 0", [(3,)]),
        ("select customer_id, sum(amount_or_zero) as t", [(1, 30), (2, 30), (3, 0)]),
        (
            "select customer_id, sum(coalesce(amount, 0)) as t",
            [(1, 30), (2, 30), (3, 0)],
        ),
        (
            "select customer_id, count(coalesce(amount, 0)) as n",
            [(1, 2), (2, 1), (3, 1)],
        ),
        (
            "select customer_id, amount_or_zero, sum(amount_or_zero) by customer_id as t",
            [(1, 10, 30), (1, 20, 30), (2, 30, 30), (3, 0, 0)],
        ),
        (
            "select customer_id, label",
            [
                (1, "ann-delivered"),
                (1, "ann-in-transit"),
                (2, "bob-delivered"),
                (3, "cat-in-transit"),
            ],
        ),
    ],
)
def test_every_read_in_the_statement_carries_the_pinned_value(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(), query) == expected


# `amount_or_zero` is stored on orders; its definition reads `_raw`, which no
# table binds: like a persisted aggregate, it answers its own keyspace only
_STORED_ONLY = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id._raw int;
auto amount_or_zero <- coalesce(_raw, 0);

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name union all select 3, 'cat' ''';

root datasource orders (
    order_id: order_id, customer_id: ~customer_id, amount_or_zero: amount_or_zero
)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id, 10 as amount_or_zero''';
"""


def test_stored_only_value_answers_its_own_keyspace():
    executor = executor_for(_STORED_ONLY)
    assert sorted_rows(executor, "select order_id, amount_or_zero") == [(100, 10)]


def test_stored_only_value_cannot_be_pinned_to_another_keyspace():
    executor = executor_for(_STORED_ONLY)
    with pytest.raises(NoDatasourceException):
        sorted_rows(executor, "select customer_id, amount_or_zero")
