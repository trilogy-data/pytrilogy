"""The region shapes docs/keyspace_phase_plan.md once listed as not modelled,
rows first: a composite-key `~` dimension, a materialized rollup beside its
base fact, and two facts partial on one dimension. The first two are twins
(materialization invariance), the third is hand-computed."""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

# ---------------------------------------------------------------- composite key
_VEHICLES = """
key vname string;
key variant string;
property <vname,variant>.vclass string;
key launch_id int;
property launch_id.mass int;

datasource vehicles (n: vname, v: variant, c: vclass)
grain (vname, variant)
query '''select 'A' as n, '1' as v, 'heavy' as c
union all select 'A', '2', 'heavy'
union all select 'B', '1', 'light'
union all select 'C', '1', 'medium' ''';
"""
_LAUNCH_ROWS = """select 1 as id, 'A' as n, '1' as v, 10 as m
union all select 2, 'A', '1', 20
union all select 3, 'B', '1', 5"""
COMPOSITE_DERIVED = _VEHICLES + f"""
datasource launches (id: launch_id, n: ~vname, v: ~variant, m: mass)
grain (launch_id)
query '''{_LAUNCH_ROWS}''';
auto status <- case when mass > 8 then 'big' else 'small' end;
"""
COMPOSITE_MATERIALIZED = _VEHICLES + f"""
property launch_id.status string;
datasource launches (id: launch_id, n: ~vname, v: ~variant, m: mass, s: status)
grain (launch_id)
query '''select l.*, case when l.m > 8 then 'big' else 'small' end as s from ({_LAUNCH_ROWS}) l''';
"""
# the vehicle with no launch (C/1, and A/2) is a row of the statement whenever
# a property of the composite key is projected, and `status` is NULL there:
# the region is demanded by what its two spans reach TOGETHER
COMPOSITE_QUERIES = [
    "select vname, variant, status",
    "select vclass, status",
    "select vname, variant, vclass, status",
    "select vclass, status, count(launch_id) as n",
    "select vclass, count(launch_id) as n where status = 'big' or status is null",
    "select vname, variant where status is null",
    "select vname, status where vclass != 'light'",
    "select vclass, count(vname) as n",
    "select vname, variant, count(status) as n, count(launch_id) as l",
]

# ---------------------------------------------------------------- materialized rollup
ROLLUP_BASE = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.amount int;
property order_id.delivery_date date?;

datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name union all select 2, 'bob' union all select 3, 'cat' ''';

datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount, delivery_date: delivery_date)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id, 10 as amount, date '2026-01-01' as delivery_date
union all select 101, 1, 20, null
union all select 102, 2, 30, date '2026-01-02' ''';

auto status <- case when delivery_date is not null then 'delivered' else 'in-transit' end;
auto total_amount <- sum(amount) by customer_id;
auto order_count <- count(order_id) by customer_id;
"""
# the summary has no row for the customer with no order: her count is 0
# and her total NULL, as the granular path says
ROLLUP_SUMMARY = ROLLUP_BASE + """
datasource customer_sales (customer_id: ~customer_id, total_amount: total_amount, order_count: order_count)
grain (customer_id)
query '''select 1 as customer_id, 30 as total_amount, 2 as order_count union all select 2, 30, 1''';
"""
ROLLUP_QUERIES = [
    "select customer_id, total_amount",
    "select customer_id, order_count",
    "select name, count(order_id) as n, total_amount",
    "select name, count(order_id) as n",
    "select customer_id, name where order_count = 0 or order_count is null",
    "select customer_id, total_amount, status",
    "select customer_id, coalesce(total_amount, 0) as t",
    "select status, sum(total_amount) as t",
    "select customer_id, total_amount, order_count, count(status) as s",
]

# ---------------------------------------------------------------- two facts
TWO_FACTS = """
key item_sk int;
property item_sk.item_desc string;
key sale_id int;
property sale_id.sale_amt int;
property sale_id.channel string;
key return_id int;
property return_id.ret_amt int;
property return_id.reason string;

datasource items (sk: item_sk, d: item_desc)
grain (item_sk)
query '''select 10 as sk, 'alpha' as d union all select 20, 'beta' union all select 30, 'gamma' union all select 40, 'delta' ''';

datasource sales (id: sale_id, sk: ~item_sk, amt: sale_amt, ch: channel)
grain (sale_id)
query '''select 1 as id, 10 as sk, 5 as amt, 'web' as ch
union all select 2, 10, 7, 'store'
union all select 3, 20, 9, 'web' ''';

datasource returns (id: return_id, sk: ~item_sk, amt: ret_amt, r: reason)
grain (return_id)
query '''select 1 as id, 10 as sk, 2 as amt, 'broken' as r
union all select 2, 30, 4, 'late' ''';
"""
# item 10: two sales, one return; 20: one sale; 30: one return; 40: nothing
TWO_FACTS_CASES = [
    (
        "select item_sk, count(sale_id) as s, count(return_id) as r",
        [(10, 2, 1), (20, 1, 0), (30, 0, 1), (40, 0, 0)],
    ),
    (
        "select item_sk, sum(sale_amt) as s, sum(ret_amt) as r",
        [(10, 12, 2), (20, 9, None), (30, None, 4), (40, None, None)],
    ),
    (
        "select item_sk, channel, count(return_id) as r",
        [
            (10, "store", 1),
            (10, "web", 1),
            (20, "web", 0),
            (30, None, 1),
            (40, None, 0),
        ],
    ),
    (
        "select item_sk, count(sale_id) as s where reason is null",
        [(20, 1), (40, 0)],
    ),
    # a WHERE over an aggregate BY the span: 0 on the items no return
    # references, and the atom filters the region's rows at FINAL, not the
    # sales aggregate's input (which would drop item 10's rows and let the
    # domain pad it back)
    (
        "select item_sk, count(sale_id) as s where count(return_id) by item_sk = 0",
        [(20, 1), (40, 0)],
    ),
    (
        "select channel, count(item_sk) as items, count(return_id) as r",
        [("store", 1, 1), ("web", 2, 1), (None, 2, 1)],
    ),
    # the same atom under an aggregate by a carried value: the returns count
    # joins the UNITED rows of the host's input, or item 30 (a return, no
    # sale) is padded past the sales-side merge and reads its count as 0
    (
        "select item_desc, count(sale_id) as s where count(return_id) by item_sk = 0",
        [("beta", 1), ("delta", 0)],
    ),
    (
        "select channel, count(item_sk) as items where count(return_id) by item_sk = 0",
        [("web", 1), (None, 1)],
    ),
    (
        "select reason, count(item_sk) as items where count(sale_id) by item_sk = 0",
        [("late", 1), (None, 1)],
    ),
    (
        "select channel, count(item_sk) as items where count(return_id) by item_sk = 0 or channel = 'store'",
        [("store", 1), ("web", 1), (None, 1)],
    ),
    ("select item_desc where count(return_id) by item_sk = 0", [("beta",), ("delta",)]),
    ("select item_desc where reason is null", [("beta",), ("delta",)]),
]


def _executor(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    return executor


def _rows(executor: Executor, query: str) -> list[tuple]:
    rows = [tuple(r) for r in executor.execute_text(query + ";")[-1].fetchall()]
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


@pytest.fixture(scope="module")
def composite() -> tuple[Executor, Executor]:
    return _executor(COMPOSITE_DERIVED), _executor(COMPOSITE_MATERIALIZED)


@pytest.fixture(scope="module")
def rollup() -> tuple[Executor, Executor]:
    return _executor(ROLLUP_BASE), _executor(ROLLUP_SUMMARY)


@pytest.mark.parametrize("query", COMPOSITE_QUERIES)
def test_composite_key_region(composite: tuple[Executor, Executor], query: str):
    derived, materialized = composite
    rows = _rows(derived, query)
    assert rows == _rows(materialized, query)
    assert rows


def test_composite_key_region_has_the_unlaunched_vehicle(
    composite: tuple[Executor, Executor],
):
    derived, _ = composite
    assert set(_rows(derived, "select vclass, status")) == {
        ("heavy", "big"),
        ("light", "small"),
        ("heavy", None),
        ("medium", None),
    }


@pytest.mark.parametrize("query", ROLLUP_QUERIES)
def test_materialized_rollup_matches_its_base(
    rollup: tuple[Executor, Executor], query: str
):
    base, summary = rollup
    rows = _rows(base, query)
    assert rows == _rows(summary, query)
    assert rows


def test_summary_count_is_zero_for_the_customer_it_lacks(
    rollup: tuple[Executor, Executor],
):
    _, summary = rollup
    assert _rows(summary, "select name, count(order_id) as n, total_amount") == [
        ("ann", 2, 30),
        ("bob", 1, 30),
        ("cat", 0, None),
    ]


@pytest.mark.parametrize("query,expected", TWO_FACTS_CASES)
def test_two_facts_partial_on_one_dimension(query: str, expected: list[tuple]):
    assert _rows(_executor(TWO_FACTS), query) == sorted(
        expected, key=lambda r: tuple((v is None, str(v)) for v in r)
    )
