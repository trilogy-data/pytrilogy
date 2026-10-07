"""A NULL-absorbing select output (COALESCE, CASE ... ELSE) is evaluated on the
select's row: the select keys its reads do not cover are its inputs, as a bare
aggregate's `by` is the select's grain. A column persisting it at its reads'
own grain answers only a select at that grain."""

from functools import cache

import pytest

from tests.helpers.models import CUSTOMERS_DERIVED
from tests.helpers.rows import executor_for, sorted_rows
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
def test_grouping_key_is_read_on_the_aggregate_input(query: str, expected: list[tuple]):
    assert sorted_rows(_executor(), query) == expected


@pytest.mark.xfail(
    strict=True, reason="the output and the aggregate argument share one address"
)
def test_pinned_output_beside_an_aggregate_of_itself():
    rows = sorted_rows(
        _executor(),
        "select customer_id, amount_or_zero, sum(amount_or_zero) by customer_id as t",
    )
    assert rows[-1][:2] == (3, 0)
