"""Two facts each bind `~customer_id`, and no source holds a row of both: each
fact's derivations read only its own rows, and a select keyed by customer
pins each label to its ELSE on the other's padding. Oracle: the same labels also persisted as columns at their grain.
"""

import pytest

from tests.helpers.rows import executor_for, sorted_rows, twin_rows
from trilogy.executor import Executor

_BASE = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.amount int;
key ticket_id int;
property ticket_id.severity int?;
root datasource customers (customer_id: customer_id, name: name) grain (customer_id)
query '''select 1 as customer_id, 'ann' as name union all select 2, 'bob'
union all select 3, 'cat' union all select 4, 'dan' ''';
"""

DERIVED = _BASE + """
root datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id, 5 as amount
union all select 101, 1, 7 union all select 102, 2, 9''';
root datasource tickets (ticket_id: ticket_id, customer_id: ~customer_id, severity: severity)
grain (ticket_id)
query '''select 500 as ticket_id, 2 as customer_id, 3 as severity
union all select 501, 3, null''';
"""
_LABELS = """
auto sev_label <- case when severity > 2 then 'hi' else 'lo' end;
auto amt_label <- case when amount > 6 then 'big' else 'small' end;
"""
DERIVED += _LABELS

MATERIALIZED = _BASE + _LABELS + """
root datasource orders (
    order_id: order_id, customer_id: ~customer_id, amount: amount, amt_label: amt_label
)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id, 5 as amount, 'small' as amt_label
union all select 101, 1, 7, 'big' union all select 102, 2, 9, 'big' ''';
root datasource tickets (
    ticket_id: ticket_id, customer_id: ~customer_id, severity: severity, sev_label: sev_label
)
grain (ticket_id)
query '''select 500 as ticket_id, 2 as customer_id, 3 as severity, 'hi' as sev_label
union all select 501, 3, null, 'lo' ''';
"""

QUERIES = [
    "select customer_id, count(order_id) as o, count(ticket_id) as t",
    "select customer_id, amt_label, sev_label",
    "select customer_id, count(order_id) as o, max(sev_label) as s",
    "select name, sum(amount) as a, count(ticket_id) as t where sev_label = 'lo' or sev_label is null",
    "select customer_id, sev_label",
    "select customer_id, sev_label, count(order_id) as o",
    "select sev_label, count(customer_id) as c",
    "select amt_label, sev_label, count(customer_id) as c",
    "select customer_id, sum(amount) as a where count(ticket_id) by customer_id = 0",
    "select customer_id, amt_label where sev_label is null",
]


@pytest.fixture(scope="module")
def derived() -> Executor:
    return executor_for(DERIVED)


@pytest.fixture(scope="module")
def materialized() -> Executor:
    return executor_for(MATERIALIZED)


@pytest.mark.parametrize("query", QUERIES)
def test_materialization_invariance(
    derived: Executor, materialized: Executor, query: str
):
    twin_rows(derived, materialized, query)


def test_each_label_takes_its_else_on_the_other_facts_rows(derived: Executor):
    assert sorted_rows(derived, "select customer_id, amt_label, sev_label") == [
        (1, "big", "lo"),
        (1, "small", "lo"),
        (2, "big", "hi"),
        (3, "small", "lo"),
        (4, "small", "lo"),
    ]
