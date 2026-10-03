"""A source with no declared grain is identified by every key it binds.

Twin: `lines (order_id, line_no, product_id)` with its grain declared and
without. Declaring the grain a source actually has must never change a
query's rows. The old rule dropped a key that identified another source on
its own (`order_id`, `product_id`), leaving `lines` one row per `line_no`, so
a return joined every order's first line. The twin exposed two more shapes on
the DECLARED side (`FINAL_DEDUP`, `UNDEMANDED_PIVOT`), pinned here too."""

import pytest

from tests.helpers.rows import executor_for, sorted_rows
from trilogy.executor import Executor

_MODEL = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.status string;
key line_no int;
key product_id int;
property product_id.pname string;
properties <order_id, line_no> (qty int);
key return_id int;
property return_id.reason string;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name union all select 2, 'bob' union all select 3, 'cat' ''';

root datasource products (product_id: product_id, pname: pname)
grain (product_id)
query '''select 10 as product_id, 'a' as pname union all select 20, 'b' union all select 30, 'c' union all select 40, 'd' ''';

root datasource orders (order_id: order_id, customer_id: ~customer_id, status: status)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id, 'shipped' as status union all
select 101, 1, 'open' union all select 102, 2, 'shipped' union all select 103, 2, 'open' ''';

root datasource lines (order_id: ~order_id, line_no: line_no, product_id: ~product_id, qty: qty)
{grain}
query '''select 100 as order_id, 1 as line_no, 10 as product_id, 5 as qty union all
select 100, 2, 20, 7 union all select 101, 1, 10, 2 union all select 102, 1, 30, 1 ''';

root datasource returns (return_id: return_id, order_id: ~order_id, line_no: ~line_no, reason: reason)
grain (return_id)
query '''select 900 as return_id, 100 as order_id, 2 as line_no, 'damaged' as reason union all
select 901, 102, 1, 'late' ''';
"""

GRAINED = _MODEL.format(grain="grain (order_id, line_no)")
UNGRAINED = _MODEL.format(grain="")

TWIN_QUERIES = [
    "select order_id, line_no, qty",
    "select line_no, sum(qty) as q",
    "select line_no, count(order_id) as n",
    "select pname, line_no, qty",
    "select name, line_no, qty",
    "select product_id, line_no, count(order_id) as n",
    "select return_id, reason, qty",
    "select return_id, reason, status, name",
    "select order_id, line_no, qty, reason",
    "select order_id, line_no, qty, count(return_id) as n",
    "select line_no, count(return_id) as n",
    "select pname, count(return_id) as n",
    "select name, count(return_id) as n",
    "select order_id, status, count(return_id) as n",
    "select order_id, line_no, qty, reason where reason is null",
    "select reason, sum(qty) as q",
    "select reason, count(line_no) as n",
    "select line_no, reason, sum(qty) as q",
    "select product_id, reason, sum(qty) as q",
]

# a return reaches ITS line only, never every order's line of the same number
HAND_ROWS = [
    ("select reason, sum(qty) as q", [("damaged", 7), ("late", 1), (None, 7)]),
    ("select line_no, count(return_id) as n", [(1, 1), (2, 1)]),
    (
        "select return_id, reason, qty where reason is not null",
        [(900, "damaged", 7), (901, "late", 1)],
    ),
    (
        "select product_id, reason, sum(qty) as q",
        [(10, None, 7), (20, "damaged", 7), (30, "late", 1), (40, None, None)],
    ),
]

# the FINAL over `lines FULL returns FULL products` projects (return, product):
# `return_id` FD-determines the line through `returns`' grain, but only where
# a return is PRESENT, and product `a`'s two unreturned lines were two rows
FINAL_DEDUP = [
    (
        "select return_id, reason, pname",
        [
            (900, "damaged", "b"),
            (901, "late", "c"),
            (None, None, "a"),
            (None, None, "d"),
        ],
    ),
]

# the same fold, with `lines` complete on the order: the unreturned lines and
# the unsold product are one NULL group, not one row each
COMPLETE_ORDER = _MODEL.replace(
    "root datasource lines (order_id: ~order_id",
    "root datasource lines (order_id: order_id",
).format(grain="grain (order_id, line_no)")

# two facts partial on `order_id` are related through the complete `orders`
# scan, and that FULL admitted the order with neither line nor return although
# nothing in the statement reaches from the order
UNDEMANDED_PIVOT = [
    (
        "select return_id, reason, qty",
        [(900, "damaged", 7), (901, "late", 1), (None, None, 2), (None, None, 5)],
    ),
    (
        "select line_no, reason, sum(qty) as q",
        [(1, "late", 1), (1, None, 7), (2, "damaged", 7)],
    ),
]


@pytest.fixture(scope="module")
def grained() -> Executor:
    return executor_for(GRAINED)


@pytest.fixture(scope="module")
def ungrained() -> Executor:
    return executor_for(UNGRAINED)


@pytest.mark.parametrize("query", TWIN_QUERIES)
def test_ungrained_matches_grained(grained: Executor, ungrained: Executor, query: str):
    assert sorted_rows(ungrained, query) == sorted_rows(grained, query)


@pytest.mark.parametrize("query,expected", HAND_ROWS)
def test_a_return_reaches_its_own_line(
    grained: Executor, ungrained: Executor, query: str, expected: list[tuple]
):
    assert sorted_rows(grained, query) == expected
    assert sorted_rows(ungrained, query) == expected


@pytest.mark.parametrize("query,expected", FINAL_DEDUP)
@pytest.mark.parametrize("twin", ["grained", "ungrained"])
def test_final_dedups_to_the_output_grain(
    request: pytest.FixtureRequest, twin: str, query: str, expected: list[tuple]
):
    assert sorted_rows(request.getfixturevalue(twin), query) == expected


def test_null_padded_key_groups_the_lines_it_does_not_name():
    executor = executor_for(COMPLETE_ORDER)
    assert sorted_rows(executor, "select return_id, count(product_id) as n") == [
        (900, 1),
        (901, 1),
        (None, 3),
    ]


@pytest.mark.parametrize("query,expected", UNDEMANDED_PIVOT)
@pytest.mark.parametrize("twin", ["grained", "ungrained"])
def test_undemanded_pivot_dimension_is_not_a_row(
    request: pytest.FixtureRequest, twin: str, query: str, expected: list[tuple]
):
    assert sorted_rows(request.getfixturevalue(twin), query) == expected


@pytest.mark.parametrize("twin", ["grained", "ungrained"])
def test_undemanded_pivot_dimension_is_not_scanned(
    request: pytest.FixtureRequest, twin: str
):
    executor = request.getfixturevalue(twin)
    assert '"orders"' not in executor.generate_sql("select return_id, reason, qty;")[-1]
