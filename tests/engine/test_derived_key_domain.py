"""A derived concept is a function of its keys: NULL wherever a key is absent.

Oracle is materialization invariance: storing a derivation as a column at its
grain must never change a query's rows.
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
"""

_MATERIALIZED = _BASE + f"""
property order_id.status string;
property order_id.undelivered bool;
property order_id.amount_or_zero int;
property order_id.label string;

root datasource orders (
    order_id: order_id, customer_id: ~customer_id,
    delivery_date: delivery_date, amount: amount,
    status: status, undelivered: undelivered,
    amount_or_zero: amount_or_zero, label: label,
)
grain (order_id)
query '''
select o.*,
    case when o.delivery_date is not null then 'delivered' else 'in-transit' end as status,
    o.delivery_date is null as undelivered,
    coalesce(o.amount, 0) as amount_or_zero,
    concat(c.name, '-', case when o.delivery_date is not null then 'delivered' else 'in-transit' end) as label
from ({_ORDER_ROWS}) o
join (select 1 as customer_id, 'ann' as name union all select 2, 'bob') c
    on o.customer_id = c.customer_id
''';
"""

_ACTIVITY = """
auto activity <- case when count(order_id) by customer_id > 0 then 'active' else 'dormant' end;
"""

QUERIES = [
    "select customer_id, status",
    "select customer_id, order_id, status",
    "select customer_id, status, count(order_id) as n",
    "select customer_id, count(status) as n_status, count(order_id) as n_orders",
    "select customer_id, undelivered",
    "select customer_id, sum(case when undelivered then 1 else 0 end) as n_undelivered",
    "select customer_id, amount_or_zero",
    "select customer_id, sum(amount_or_zero) as total",
    "select customer_id, coalesce(sum(amount), 0) as total",
    "select customer_id, label",
    "select customer_id, name, status, amount_or_zero, label",
    "select customer_id, name where status = 'in-transit'",
    "select customer_id, status, activity",
    "select status, count(customer_id) as customers",
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
