"""A derived concept is a function of its keys: NULL wherever a key is absent.

Oracle is materialization invariance: storing a derivation as a column at its
grain must never change a query's rows. `OWED` queries are strict xfails: the
planner still evaluates them over rows padded for a `~` extension. A fix shows
up as XPASS; move the query to `HOLDS`. See docs/keyspace_phase_plan.md and
docs/handoff_extension_row_semantics.md.
"""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

_BASE = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.delivery_date date?;
property order_id.amount int;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''
select 1 as customer_id, 'ann' as name union all
select 2, 'bob' union all
select 3, 'cat'
''';
"""

_ORDER_ROWS = """
select 100 as order_id, 1 as customer_id, date '2026-01-01' as delivery_date, 10 as amount union all
select 101, 1, null, 20 union all
select 102, 2, date '2026-01-02', 30
"""

_DERIVED = _BASE + f"""
root datasource orders (
    order_id: order_id, customer_id: ~customer_id,
    delivery_date: delivery_date, amount: amount,
)
grain (order_id)
query '''{_ORDER_ROWS}''';

auto status <- case when delivery_date is not null then 'delivered' else 'in-transit' end;
auto undelivered <- delivery_date is null;
auto amount_or_zero <- coalesce(amount, 0);
auto label <- concat(name, '-', status);
auto flag <- case when undelivered then 1 else 0 end;
auto order_seq <- row_number order_id over customer_id order by amount asc;
auto order_rank <- rank order_id by amount desc;
"""

_MATERIALIZED = _BASE + f"""
property order_id.status string;
property order_id.undelivered bool;
property order_id.amount_or_zero int;
property order_id.label string;
property order_id.flag int;
property order_id.order_seq int;
property order_id.order_rank int;

root datasource orders (
    order_id: order_id, customer_id: ~customer_id,
    delivery_date: delivery_date, amount: amount,
    status: status, undelivered: undelivered,
    amount_or_zero: amount_or_zero, label: label,
    flag: flag, order_seq: order_seq, order_rank: order_rank,
)
grain (order_id)
query '''
select o.*,
    case when o.delivery_date is not null then 'delivered' else 'in-transit' end as status,
    o.delivery_date is null as undelivered,
    coalesce(o.amount, 0) as amount_or_zero,
    concat(c.name, '-', case when o.delivery_date is not null then 'delivered' else 'in-transit' end) as label,
    case when o.delivery_date is null then 1 else 0 end as flag,
    row_number() over (partition by o.customer_id order by o.amount asc) as order_seq,
    rank() over (order by o.amount desc) as order_rank
from ({_ORDER_ROWS}) o
join (select 1 as customer_id, 'ann' as name union all select 2, 'bob') c
    on o.customer_id = c.customer_id
''';
"""

_ACTIVITY = """
auto activity <- case when count(order_id) by customer_id > 0 then 'active' else 'dormant' end;
"""

# already evaluated on the key's own rows (or NULL-propagating) today
HOLDS = [
    "select customer_id, coalesce(sum(amount), 0) as total",
    "select customer_id, name where status = 'in-transit'",
    "select customer_id, order_id, order_rank",
    "select customer_id, activity",
    "select customer_id, status where status = 'delivered'",
    "select customer_id, status where amount > 15",
    "select customer_id, status where status is null",
    "select customer_id, status where status is null or status = 'delivered'",
    "select customer_id, status",
    "select customer_id, order_id, status",
    "select customer_id, status, count(order_id) as n",
    "select customer_id, count(status) as n_status, count(order_id) as n_orders",
    "select customer_id, undelivered",
    "select customer_id, sum(case when undelivered then 1 else 0 end) as n_undelivered",
    "select customer_id, amount_or_zero",
    "select customer_id, sum(amount_or_zero) as total",
    "select customer_id, label",
    "select customer_id, name, status, amount_or_zero, label",
    "select customer_id, status, activity",
    "select customer_id, sum(flag) as n_undelivered",
    "select customer_id, order_id, order_seq",
    "select customer_id, order_seq",
    "select customer_id, count(order_seq) as numbered",
    "select customer_id, name where order_seq = 1",
    "select customer_id, status where name = 'cat'",
    "select customer_id, status, amount where amount is null",
    "select customer_id, status, amount where amount is null or amount > 15",
    # the span is demanded only through a member it determines
    "select name, status",
    # the span is demanded only as an aggregate argument
    "select status, count(customer_id) as customers",
    # a WHERE over an off-span column the statement does not project
    "select customer_id, status where amount is null",
    # the span is never named and the aggregate groups by a member it carries:
    # the solid stream holds that member for customers WITH an order only
    "select name, count(status) as n",
    "select status, name, count(order_id) as n",
    "select name, label, order_seq",
    # aggregates over the region beside aggregates absent on it
    "select status, count(customer_id) as c, sum(amount) as s",
    "select status, count(customer_id) as c, count(order_id) as o",
    "select status, count(name) as n",
    "select label, count(customer_id) as n",
    "select status, activity, count(customer_id) as n",
]

OWED: list[str] = []

QUERIES = HOLDS + [
    pytest.param(q, marks=pytest.mark.xfail(strict=True, reason="owed")) for q in OWED
]

# the same expression spelled inline and as a named concept
SPELLINGS = [
    ("sum(flag)", "sum(case when undelivered then 1 else 0 end)"),
    ("count(amount_or_zero)", "count(coalesce(amount, 0))"),
    (
        "max(status)",
        "max(case when delivery_date is not null then 'delivered' else 'in-transit' end)",
    ),
]


def _executor(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model + _ACTIVITY)
    return executor


def _rows(executor: Executor, query: str) -> list[tuple]:
    rows = [tuple(r) for r in executor.execute_text(query + ";")[-1].fetchall()]
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


@pytest.fixture(scope="module")
def derived() -> Executor:
    return _executor(_DERIVED)


@pytest.fixture(scope="module")
def materialized() -> Executor:
    return _executor(_MATERIALIZED)


@pytest.mark.parametrize("query", QUERIES)
def test_materialization_invariance(
    derived: Executor, materialized: Executor, query: str
):
    assert _rows(derived, query) == _rows(materialized, query)


@pytest.mark.parametrize("named,inline", SPELLINGS)
def test_inline_spelling_matches_named(derived: Executor, named: str, inline: str):
    assert _rows(derived, f"select customer_id, {named} as v") == _rows(
        derived, f"select customer_id, {inline} as v"
    )


def test_orderless_customer_has_no_status(derived: Executor):
    assert _rows(derived, "select customer_id, status, count(order_id) as n") == [
        (1, "delivered", 1),
        (1, "in-transit", 1),
        (2, "delivered", 1),
        (3, None, 0),
    ]


def test_else_fires_when_the_key_is_present(derived: Executor):
    assert _rows(derived, "select customer_id, activity") == [
        (1, "active"),
        (2, "active"),
        (3, "dormant"),
    ]


# `key is null` does not witness absence: these shapes have a NULL key or an
# unbound property on a REAL row.
_NULLABLE_FK = """
key customer_id int;
property customer_id.name string;
key order_id int;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name''';

root datasource orders (order_id: order_id, customer_id: ?customer_id)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id union all select 101, null''';

auto customer_label <- coalesce(name, 'unknown');
"""


def test_nullable_key_is_a_value_not_absence():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_NULLABLE_FK)
    assert _rows(executor, "select order_id, customer_label") == [
        (100, "ann"),
        (101, "unknown"),
    ]


# `returns` binds its OWN grain keys `~`: a line with no return still has its
# (order, item) entity, from `lines`, so a derivation keyed on it evaluates.
_PARTIAL_PROPERTY_SOURCE = """
key order_id int;
key item_id int;
properties <order_id, item_id> (qty int, ret_order int?);
auto is_returned <- ret_order is not null;

root datasource lines (o: order_id, i: item_id, q: qty)
grain (order_id, item_id)
query '''select 1 as o, 10 as i, 5 as q union all select 2, 10, 7''';

root datasource returns (o: ~order_id, i: ~item_id, ro: ret_order)
grain (order_id, item_id)
query '''select 1 as o, 10 as i, 1 as ro''';
"""


def test_present_entity_with_an_unbound_property_still_evaluates():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_PARTIAL_PROPERTY_SOURCE)
    assert _rows(executor, "select order_id, is_returned") == [(1, True), (2, False)]
    assert _rows(executor, "select order_id, bool_or(is_returned) as any_return") == [
        (1, True),
        (2, False),
    ]


def test_rollup_subtotal_row_keeps_its_value(derived: Executor):
    query = (
        "select customer_id, coalesce(sum(amount), 0) as total by rollup (customer_id)"
    )
    assert _rows(derived, query) == [(1, 30), (2, 30), (3, 0), (None, 60)]


# An OPTIONAL entity: `returns` is looked up through its `~` bound (order, item)
# and brings a further key. A line with no return has no `return_id` entity, so
# a derivation keyed on it is NULL there, not its COALESCE default.
_LINES = """
key order_id int;
key item_id int;
key return_id int;
properties <order_id, item_id> (qty int);
property return_id.reason string?;

root datasource lines (o: order_id, i: item_id, q: qty)
grain (order_id, item_id)
query '''select 1 as o, 10 as i, 5 as q union all select 2, 10, 7 union all select 3, 11, 9''';
"""

_OPTIONAL_DERIVED = (
    _LINES
    + """
root datasource returns (o: ~order_id, i: ~item_id, r: return_id, reason: reason)
grain (return_id)
query '''select 1 as o, 10 as i, 900 as r, 'broken' as reason union all select 3, 11, 901, null''';

auto reason_label <- coalesce(reason, 'none');
"""
)

_OPTIONAL_MATERIALIZED = (
    _LINES
    + """
property return_id.reason_label string;

root datasource returns (
    o: ~order_id, i: ~item_id, r: return_id, reason: reason, rl: reason_label,
)
grain (return_id)
query '''select 1 as o, 10 as i, 900 as r, 'broken' as reason, 'broken' as rl
union all select 3, 11, 901, null, 'none' ''';
"""
)


@pytest.mark.parametrize(
    "query",
    [
        "select order_id, item_id, reason_label",
        "select order_id, reason_label",
        "select item_id, qty, reason_label",
        "select order_id, item_id, qty, return_id, reason_label",
        "select order_id, count(reason_label) as n",
    ],
)
def test_optional_entity_is_absent_on_rows_without_it(query: str):
    derived = Dialects.DUCK_DB.default_executor()
    derived.execute_text(_OPTIONAL_DERIVED)
    materialized = Dialects.DUCK_DB.default_executor()
    materialized.execute_text(_OPTIONAL_MATERIALIZED)
    rows = _rows(derived, query)
    assert rows == _rows(materialized, query)
    assert any(r[-1] in (None, 0) for r in rows)
