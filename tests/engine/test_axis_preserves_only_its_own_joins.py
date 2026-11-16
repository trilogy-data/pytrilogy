"""A FULL on a merged customer axis licenses preserving a later join's right
only when that join is keyed on the axis too. A sale's date hangs off one
channel's rows: a date no web sale fell on is no customer's row, so its
padding must not reach the per-customer totals, whichever join is walked
first."""

import pytest

from tests.helpers.rows import sorted_rows
from tests.helpers.walk_order import WALKS, flip_walk_spelling
from trilogy import Dialects, Environment

_FILES = {
    "customer.preql": """
key sk int;
property sk.id string;

datasource customers (c_sk: sk, c_id: id)
grain (sk)
query '''
select 1 as c_sk, 'ann' as c_id union all
select 2, 'bob'
''';
""",
    "date.preql": """
key sk int;
property sk.year int;

datasource dates (d_sk: sk, d_year: year)
grain (sk)
query '''
select 10 as d_sk, 2002 as d_year union all
select 11, 2002 union all
select 12, 2001
''';
""",
    "store_sales.preql": """
import customer as customer;
key ticket int;
property ticket.amount int;

datasource store_sales (ticket: ticket, customer_sk: customer.sk, amount: amount)
grain (ticket)
query '''
select 1 as ticket, 1 as customer_sk, 5 as amount
''';
""",
    "web_sales.preql": """
import customer as billing_customer;
import date as sale_date;
key order_id int;
property order_id.amount int;

datasource web_sales (
    order_id: order_id,
    customer_sk: ?billing_customer.sk,
    date_sk: ?sale_date.sk,
    amount: amount
)
grain (order_id)
query '''
select 100 as order_id, 1 as customer_sk, 10 as date_sk, 7 as amount union all
select 101, 2, 12, 9
''';
""",
}

QUERY = """
import store_sales as store;
import web_sales as web;
merge store.customer.sk into web.billing_customer.sk;
where web.sale_date.year = 2002
select web.billing_customer.id as cid, sum(web.amount) as val;
"""


@pytest.mark.parametrize("walk", WALKS)
def test_a_date_beside_a_merged_axis_adds_no_customer(tmp_path, monkeypatch, walk):
    flip_walk_spelling(monkeypatch, walk)
    for name, text in _FILES.items():
        (tmp_path / name).write_text(text)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    try:
        assert sorted_rows(executor, QUERY) == [("ann", 7)]
    finally:
        executor.close()
