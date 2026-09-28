"""A source with no declared grain is identified by every key it binds.

Twin: `lines (order_id, line_no, product_id)` with its grain declared and
without. Declaring the grain a source actually has must never change a
query's rows. The old rule dropped a key that identified another source on
its own (`order_id`, `product_id`), leaving `lines` one row per `line_no`, so
a return joined every order's first line (`OWED` below are the shapes the
twin exposed on the DECLARED side too; strict xfails until fixed)."""

import pytest

from trilogy import Dialects
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

# the FINAL over `lines FULL returns FULL products` projects (return, product)
# with no dedup: `return_id` FD-determines the line through `returns`' grain,
# but only where a return is PRESENT, and product `a`'s two unreturned lines
# come out as two rows
OWED_FINAL_DEDUP = [
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

# two facts partial on `order_id` are related through the complete `orders`
# scan, and that FULL admits the order with neither line nor return although
# nothing in the statement reaches from the order
OWED_UNDEMANDED_PIVOT = [
    (
        "select return_id, reason, qty",
        [(900, "damaged", 7), (901, "late", 1), (None, None, 2), (None, None, 5)],
    ),
    (
        "select line_no, reason, sum(qty) as q",
        [(1, "late", 1), (1, None, 7), (2, "damaged", 7)],
    ),
]


def _executor(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    return executor


def _rows(executor: Executor, query: str) -> list[tuple]:
    rows = [tuple(r) for r in executor.execute_text(query + ";")[-1].fetchall()]
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


@pytest.fixture(scope="module")
def grained() -> Executor:
    return _executor(GRAINED)


@pytest.fixture(scope="module")
def ungrained() -> Executor:
    return _executor(UNGRAINED)


@pytest.mark.parametrize("query", TWIN_QUERIES)
def test_ungrained_matches_grained(grained: Executor, ungrained: Executor, query: str):
    assert _rows(ungrained, query) == _rows(grained, query)


@pytest.mark.parametrize("query,expected", HAND_ROWS)
def test_a_return_reaches_its_own_line(
    grained: Executor, ungrained: Executor, query: str, expected: list[tuple]
):
    assert _rows(grained, query) == expected
    assert _rows(ungrained, query) == expected


@pytest.mark.xfail(
    strict=True, reason="FINAL dedup reads an FD through an absent entity"
)
@pytest.mark.parametrize("query,expected", OWED_FINAL_DEDUP)
def test_final_dedups_to_the_output_grain(
    grained: Executor, query: str, expected: list[tuple]
):
    assert _rows(grained, query) == expected


@pytest.mark.xfail(strict=True, reason="two-fact pivot through an undemanded dimension")
@pytest.mark.parametrize("query,expected", OWED_UNDEMANDED_PIVOT)
@pytest.mark.parametrize("twin", ["grained", "ungrained"])
def test_undemanded_pivot_dimension_is_not_a_row(
    request: pytest.FixtureRequest, twin: str, query: str, expected: list[tuple]
):
    assert _rows(request.getfixturevalue(twin), query) == expected
