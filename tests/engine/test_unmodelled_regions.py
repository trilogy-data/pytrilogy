"""Region shapes the keyspace does not model with a region of their own,
rows first: a composite-key `~` dimension, a materialized rollup beside its
base fact, two facts partial on one dimension, and two facts at ONE grain
where only their join witnesses the full region (TPC-DS q64's returned sale
line). The first two are twins (materialization invariance), the third is
hand-computed, the fourth is both."""

import pytest

from tests.helpers.rows import executor_for, sort_rows, sorted_rows, twin_rows
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
"""
_LAUNCH_STATUS = """
auto status <- case when mass > 8 then 'big' else 'small' end;
"""
COMPOSITE_DERIVED += _LAUNCH_STATUS
COMPOSITE_MATERIALIZED = _VEHICLES + _LAUNCH_STATUS + f"""
datasource launches (id: launch_id, n: ~vname, v: ~variant, m: mass, s: status)
grain (launch_id)
query '''select l.*, case when l.m > 8 then 'big' else 'small' end as s from ({_LAUNCH_ROWS}) l''';
"""
# the vehicle with no launch (C/1, and A/2) is a row of the statement whenever
# a property of the composite key is projected, and `status` is pinned to its
# ELSE there: the region is demanded by what its two spans reach TOGETHER
COMPOSITE_QUERIES = [
    "select vname, variant, status",
    "select vclass, status",
    "select vname, variant, vclass, status",
    "select vclass, status, count(launch_id) as n",
    "select vclass, count(launch_id) as n where status = 'big' or status is null",
    "select vname, variant where status = 'small'",
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


# ---------------------------------------------------------------- two facts at one grain
_TWO_FACT_JOIN_BASE = """
key item_sk int;
property item_sk.item_desc string;
key ticket int;
key sale_date int;
property sale_date.sale_year int;
property <item_sk, ticket>.qty int;
property <item_sk, ticket>.ret_qty int?;
key reason_sk int;
property reason_sk.reason_desc string;

datasource items (sk: item_sk, d: item_desc)
grain (item_sk)
query '''select 10 as sk, 'alpha' as d union all select 20, 'beta' union all select 30, 'gamma' ''';

datasource dates (d: sale_date, y: sale_year)
grain (sale_date)
query '''select 1 as d, 1999 as y union all select 2, 2000''';

datasource returns (sk: ~item_sk, t: ~ticket, rq: ret_qty, r: reason_sk)
grain (item_sk, ticket)
query '''select 10 as sk, 1 as t, 1 as rq, 1 as r union all select 20, 3, 9, 2''';
"""
_SALE_ROWS = """select 10 as sk, 1 as t, 1 as d, 5 as q
union all select 10, 2, 2, 7
union all select 20, 3, 1, 9
union all select 30, 4, 2, 2"""
_REASON_ROWS = "select 1 as sk, 'broken' as d union all select 2, 'late' union all select 3, 'wrong' "
# `sales` carries {item, ticket, sale_date}; `returns`, at the same grain,
# {~item, ~ticket, reason}: no single source carries every entity, and the
# solid rows of the {item, ticket} region are the RETURNED lines
TWO_FACT_JOIN_DERIVED = _TWO_FACT_JOIN_BASE + f"""
datasource sales (sk: item_sk, t: ticket, d: sale_date, q: qty)
grain (item_sk, ticket)
query '''{_SALE_ROWS}''';

datasource reasons (sk: reason_sk, d: reason_desc)
grain (reason_sk)
query '''{_REASON_ROWS}''';

"""
_TWO_FACT_JOIN_DERIVATIONS = """
auto reason_class <- case when reason_desc = 'broken' then 'defect' else 'other' end;
auto is_returned <- ret_qty is not null;
"""
TWO_FACT_JOIN_DERIVED += _TWO_FACT_JOIN_DERIVATIONS
TWO_FACT_JOIN_MATERIALIZED = _TWO_FACT_JOIN_BASE + _TWO_FACT_JOIN_DERIVATIONS + f"""

datasource sales (sk: item_sk, t: ticket, d: sale_date, q: qty, ir: is_returned)
grain (item_sk, ticket)
query '''select s.*, s.t in (1, 3) as ir from ({_SALE_ROWS}) s''';

datasource reasons (sk: reason_sk, d: reason_desc, c: reason_class)
grain (reason_sk)
query '''select r.*, case when r.d = 'broken' then 'defect' else 'other' end as c from ({_REASON_ROWS}) r''';
"""
# lines: (10,1,1999,q5; returned, broken) (10,2,2000,q7) (20,3,1999,q9; returned, late) (30,4,2000,q2)
TWO_FACT_JOIN_CASES = [
    (
        "select item_sk, ticket, reason_class",
        [(10, 1, "defect"), (10, 2, "other"), (20, 3, "other"), (30, 4, "other")],
    ),
    (
        "select sale_year, reason_class",
        [(1999, "defect"), (1999, "other"), (2000, "other")],
    ),
    (
        "select sale_year, reason_class, sum(qty) as q",
        [(1999, "defect", 5), (1999, "other", 9), (2000, "other", 9)],
    ),
    pytest.param(
        "select item_desc, reason_class, is_returned",
        [
            ("alpha", "defect", True),
            ("alpha", "other", False),
            ("beta", "other", True),
            ("gamma", "other", False),
        ],
        marks=pytest.mark.xfail(
            strict=True,
            reason="pre-existing: the bound is_returned column reads NULL on "
            "an unreturned line beside the reason region (materialized twin)",
        ),
    ),
    (
        "select reason_class, count(ticket) as n",
        [("defect", 1), ("other", 1), (None, 2)],
    ),
    (
        "select reason_class, count(ticket) as n where is_returned",
        [("defect", 1), ("other", 1)],
    ),
    # outputs that are exactly the region's spans under a null-accepting WHERE
    # over the pinned value: the solid stream and the domain ask the search for
    # the same columns under different promotions, and the domain must not take
    # the solid stream's cached partial scan (`no complete sources found`)
    ("select item_sk, ticket where reason_class is null", []),
    ("select ticket where reason_class is null", []),
    (
        "select item_sk, ticket where reason_class is null or reason_class = 'defect'",
        [(10, 1)],
    ),
    (
        "select sale_year, count(ticket) as n where reason_class = 'other' or reason_class is null",
        [(1999, 1), (2000, 2)],
    ),
    (
        "select sale_year, reason_desc, count(ticket) as n",
        [(1999, "broken", 1), (1999, "late", 1), (2000, None, 2)],
    ),
    (
        "select item_desc, count(ticket) as n, count(reason_sk) as r",
        [("alpha", 2, 1), ("beta", 1, 1), ("gamma", 1, 0)],
    ),
    ("select sale_year, count(reason_sk) as r", [(1999, 2), (2000, 0)]),
    (
        "select reason_class, sum(ret_qty) as rq",
        [("defect", 1), ("other", 9), (None, None)],
    ),
    (
        "select item_desc, reason_class where sale_year = 2000",
        [("alpha", "other"), ("gamma", "other")],
    ),
    (
        "select sale_year, is_returned, count(ticket) as n",
        [(1999, True, 2), (2000, False, 2)],
    ),
    # ticket is not requested: the item/reason relation is the returns rows,
    # and item 10 is referenced by a return, so only item 30 is reason-less
    (
        "select reason_class, count(item_sk) as items",
        [("defect", 1), ("other", 1), (None, 1)],
    ),
    (
        "select sale_year, reason_class, count(item_sk) as i where qty > 4",
        [(1999, "defect", 1), (1999, "other", 1), (2000, "other", 1)],
    ),
    ("select reason_desc, sum(qty) as q", [("broken", 5), ("late", 9), (None, 9)]),
    ("select sale_year, count(ticket) as n where reason_class is null", []),
    ("select sale_year, count(ticket) as n where reason_class = 'defect'", [(1999, 1)]),
    (
        "select item_desc, count(ticket) as n where reason_class is null",
        [],
    ),
    (
        "select reason_class, count(sale_date) as d",
        [("defect", 1), ("other", 1), (None, 1)],
    ),
    (
        "select sale_year, count(ticket) as n, count(reason_class) as c",
        [(1999, 2, 2), (2000, 2, 2)],
    ),
]


@pytest.fixture(scope="module")
def composite() -> tuple[Executor, Executor]:
    return executor_for(COMPOSITE_DERIVED), executor_for(COMPOSITE_MATERIALIZED)


@pytest.fixture(scope="module")
def rollup() -> tuple[Executor, Executor]:
    return executor_for(ROLLUP_BASE), executor_for(ROLLUP_SUMMARY)


@pytest.mark.parametrize("query", COMPOSITE_QUERIES)
def test_composite_key_region(composite: tuple[Executor, Executor], query: str):
    derived, materialized = composite
    rows = twin_rows(derived, materialized, query)
    assert rows


def test_composite_key_region_has_the_unlaunched_vehicle(
    composite: tuple[Executor, Executor],
):
    derived, _ = composite
    assert set(sorted_rows(derived, "select vclass, status")) == {
        ("heavy", "big"),
        ("heavy", "small"),
        ("light", "small"),
        ("medium", "small"),
    }


@pytest.mark.parametrize("query", ROLLUP_QUERIES)
def test_materialized_rollup_matches_its_base(
    rollup: tuple[Executor, Executor], query: str
):
    base, summary = rollup
    rows = twin_rows(base, summary, query)
    assert rows


def test_summary_count_is_zero_for_the_customer_it_lacks(
    rollup: tuple[Executor, Executor],
):
    _, summary = rollup
    assert sorted_rows(summary, "select name, count(order_id) as n, total_amount") == [
        ("ann", 2, 30),
        ("bob", 1, 30),
        ("cat", 0, None),
    ]


@pytest.mark.parametrize("query,expected", TWO_FACTS_CASES)
def test_two_facts_partial_on_one_dimension(query: str, expected: list[tuple]):
    assert sorted_rows(executor_for(TWO_FACTS), query) == sort_rows(expected)


@pytest.fixture(scope="module")
def two_fact_join() -> tuple[Executor, Executor]:
    return executor_for(TWO_FACT_JOIN_DERIVED), executor_for(TWO_FACT_JOIN_MATERIALIZED)


@pytest.mark.parametrize("query,expected", TWO_FACT_JOIN_CASES)
def test_two_facts_at_one_grain(
    two_fact_join: tuple[Executor, Executor], query: str, expected: list[tuple]
):
    derived, materialized = two_fact_join
    rows = twin_rows(derived, materialized, query)
    assert rows == sort_rows(expected)
