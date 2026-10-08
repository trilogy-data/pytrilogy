"""Two regions beside a `?` key bound on a second table.

A key NULL by absence never pairs with a value-NULL group, wherever the
padding happened: inside a source the merge reads, or in an earlier join of
the merge itself. A WHERE is tested on every row it can keep, the regions'
included, before the aggregates it precedes.

Cat has no order, so her `bucket` is padding; order 100's NULL bucket is a
value, and `targets` has a row for it. Bucket `z` has no order: the bucket
region."""

from decimal import Decimal as D
from functools import cache

import pytest

from tests.helpers.models import LINE_ITEMS
from tests.helpers.rows import executor_for, sorted_rows
from trilogy.executor import Executor

_BASE = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.amount int;
key bucket string;
property bucket.target int;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''
select 1 as customer_id, 'ann' as name union all
select 2, 'bob' union all
select 3, 'cat'
''';

root datasource targets (bucket: ?bucket, target: target)
grain (bucket)
query '''
select null as bucket, 5 as target union all
select 'a', 7 union all
select 'b', 8 union all
select 'z', 9
''';
"""

_ORDER_ROWS = """
select 100 as order_id, 1 as customer_id, 10 as amount, null as bucket union all
select 101, 1, 20, 'a' union all
select 102, 2, 30, 'b'
"""

TWO_REGIONS = _BASE + f"""
root datasource orders (
    order_id: order_id, customer_id: ~customer_id, amount: amount, bucket: ~?bucket
)
grain (order_id)
query '''{_ORDER_ROWS}''';
"""

BUCKET_REGION = (_BASE + f"""
root datasource orders (
    order_id: order_id, customer_id: customer_id, amount: amount, bucket: ~?bucket
)
grain (order_id)
query '''{_ORDER_ROWS}''';
""").replace(" union all\nselect 3, 'cat'", "")


@cache
def _executor(model: str) -> Executor:
    return executor_for(model)


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select customer_id, bucket, target",
            [(1, "a", 7), (1, None, 5), (2, "b", 8), (3, None, None), (None, "z", 9)],
        ),
        (
            "select customer_id, bucket, sum(target) by bucket as t",
            [(1, "a", 7), (1, None, 5), (2, "b", 8), (3, None, None), (None, "z", 9)],
        ),
        (
            "select customer_id, target, sum(target) by bucket as t where bucket is null",
            [(1, 5, 5), (3, None, None)],
        ),
        (
            "select customer_id, bucket, sum(target) by bucket as t, count(customer_id) by bucket as n",
            [
                (1, "a", 7, 1),
                (1, None, 5, 2),
                (2, "b", 8, 1),
                (3, None, None, 2),
                (None, "z", 9, 0),
            ],
        ),
        (
            "select customer_id, bucket, sum(amount) by bucket as s, sum(target) by bucket as t",
            [
                (1, "a", 20, 7),
                (1, None, 10, 5),
                (2, "b", 30, 8),
                (3, None, None, None),
                (None, "z", None, 9),
            ],
        ),
        (
            "select customer_id, bucket, count(order_id) by customer_id as c, sum(target) by bucket as t",
            [
                (1, "a", 2, 7),
                (1, None, 2, 5),
                (2, "b", 1, 8),
                (3, None, 0, None),
                (None, "z", 0, 9),
            ],
        ),
        (
            "select customer_id, name, bucket, sum(amount) by customer_id as a, sum(target) by bucket as t",
            [
                (1, "ann", "a", 30, 7),
                (1, "ann", None, 30, 5),
                (2, "bob", "b", 30, 8),
                (3, "cat", None, None, None),
                (None, None, "z", None, 9),
            ],
        ),
    ],
)
def test_padding_from_an_earlier_join_never_pairs_a_value_null(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(TWO_REGIONS), query) == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select customer_id, name, bucket, sum(amount) by customer_id as a, sum(target) by bucket as t",
            [
                (1, "ann", "a", 30, 7),
                (1, "ann", None, 30, 5),
                (2, "bob", "b", 30, 8),
                (None, None, "z", None, 9),
            ],
        ),
        (
            "select customer_id, bucket, count(order_id) by customer_id as c, sum(target) by bucket as t",
            [(1, "a", 2, 7), (1, None, 2, 5), (2, "b", 1, 8), (None, "z", 0, 9)],
        ),
    ],
)
def test_contributors_paired_only_among_themselves_get_a_bridge(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(BUCKET_REGION), query) == expected


@pytest.mark.xfail(
    strict=True,
    reason="the padded stream the FINAL reads carries no column NULL exactly "
    "on cat's row, so no guard can tell her padded bucket from a value NULL",
)
def test_where_over_the_value_null_group_aggregate_keeps_padding_apart():
    assert sorted_rows(
        _executor(TWO_REGIONS),
        "select customer_id, bucket, sum(target) by bucket as t where coalesce(sum(target) by bucket, 0) = 0",
    ) == [(3, None, None)]


@pytest.mark.parametrize(
    "query,expected",
    [
        ("select bucket, sum(amount) as s where name is null", [("z", None)]),
        (
            "select bucket, sum(amount) as s where name is null or name = 'ann'",
            [("a", 20), ("z", None), (None, 10)],
        ),
        (
            "select bucket, sum(amount) by customer_id as a where name is null",
            [("z", None)],
        ),
        (
            "select customer_id, bucket, sum(amount) as s where bucket is null",
            [(1, None, 10), (3, None, None)],
        ),
        (
            "select customer_id, bucket, max(target) as mt where name is null",
            [(None, "z", 9)],
        ),
    ],
)
def test_where_testing_a_region_precedes_the_aggregates_grouping_it(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(TWO_REGIONS), query) == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select customer_id, bucket, sum(amount) by customer_id as a where bucket is null",
            [(1, None, 10), (3, None, None)],
        ),
        (
            "select customer_id, bucket, sum(amount) by customer_id as a where amount is null",
            [(3, None, None), (None, "z", None)],
        ),
    ],
)
def test_a_member_the_where_emptied_is_not_a_region_row(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(TWO_REGIONS), query) == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select customer_id, bucket, count(customer_id) by bucket as cb",
            [(1, "a", 1), (1, None, 2), (2, "b", 1), (3, None, 2), (None, "z", 0)],
        ),
        (
            "select customer_id, count(customer_id) by bucket as cb",
            [(1, 1), (1, 2), (2, 1), (3, 2), (None, 0)],
        ),
    ],
)
def test_an_aggregate_holding_both_regions_pairs_after_they_are_stitched(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(TWO_REGIONS), query) == expected


@pytest.mark.parametrize("model", [TWO_REGIONS, BUCKET_REGION])
def test_a_region_row_the_where_keeps_beside_a_total_it_lacks(model: str):
    assert sorted_rows(
        _executor(model),
        "select bucket, sum(amount) by customer_id as a where target > 6",
    ) == [("a", 20), ("b", 30), ("z", None)]


@pytest.mark.xfail(
    strict=True,
    reason="the WHERE must reach the solid total's input and FINAL's region "
    "rows apart; both read one orders group, so it is refused",
)
def test_where_kept_region_row_beside_a_solid_total_by_another_key():
    assert sorted_rows(
        _executor(BUCKET_REGION),
        "select bucket, sum(amount) by customer_id as a where amount is null",
    ) == [("z", None)]


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select line_id, sum(cost) by product_id as sc",
            [(1, D("1.0")), (2, D("2.0")), (3, D("1.0")), (None, D("3.0"))],
        ),
        (
            "select user_id, product_id, sum(cost) by product_id as sc",
            [
                (1, 1, D("1.0")),
                (1, 2, D("2.0")),
                (2, 1, D("1.0")),
                (3, None, None),
                (None, 3, D("3.0")),
            ],
        ),
    ],
)
def test_a_property_summed_by_its_own_key_over_a_region_counts_each_once(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(LINE_ITEMS), query) == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select state, product_id, revenue",
            [
                ("ca", 1, D("5.0")),
                ("ca", 2, D("7.0")),
                ("ny", 1, D("3.0")),
                ("wa", None, None),
                (None, 3, None),
            ],
        ),
        (
            "select user_id, sum(cost) as sc",
            [(1, D("3.0")), (2, D("1.0")), (3, None), (None, D("3.0"))],
        ),
    ],
)
def test_a_region_row_with_a_null_key_is_no_subset_match(
    query: str, expected: list[tuple]
):
    assert sorted_rows(_executor(LINE_ITEMS), query) == expected


def test_a_lookup_off_a_padded_provider_reads_the_key_where_it_survives():
    assert sorted_rows(
        _executor(LINE_ITEMS),
        "select user_id, sum(cost) by product_id as sc where cost is null",
    ) == [(3, None)]


def test_a_coalesced_inner_key_pairs_value_nulls_null_safely():
    assert sorted_rows(
        _executor(TWO_REGIONS),
        "select customer_id, target, count(customer_id) by bucket as cb where name = 'ann'",
    ) == [(1, 5, 1), (1, 7, 1)]


def test_a_where_the_padded_row_passes_is_not_pushed_into_the_padded_side():
    assert sorted_rows(
        _executor(LINE_ITEMS),
        "select user_id, cost, sum(cost) by product_id as sc where cost is null",
    ) == [(3, None, None)]


@pytest.mark.xfail(
    strict=True,
    reason="FINAL reads the WHERE's `cost` off the filtered aggregate, which "
    "groups by it, instead of off the products the lines name",
)
def test_a_where_read_off_a_filtered_aggregate_misses_the_rejected_members():
    assert sorted_rows(
        _executor(LINE_ITEMS),
        "select user_id, count(user_id) by product_id as cu where cost is null",
    ) == [(3, 1)]
