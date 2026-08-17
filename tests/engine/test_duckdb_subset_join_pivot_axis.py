"""`subset join` onto a pivot rowset stack, projecting only properties of the
raw side and never the authored join key itself: the all_sales year-over-year
pivot (TPC-DS q04 family).

The authored axis (`cust.cid = id`) is hosted by no group: the rowset side
exposes its handle, but `id` is only FD-reachable from the raw scan (`sk`
determines the `unique` business key), so demand has to re-source it or the
FINAL merge loses the join key entirely, a silent ON 1=1 cartesian before the
keyless-join guard, an UnresolvableQueryException after it.
"""

import pytest

from trilogy import Dialects

FIXTURE = """
key sk int;
unique property sk.id string;
property sk.name string;

key order_id int;
property order_id.amount float;
property order_id.order_flag string;

datasource customers (c: sk, id: id, name: name)
grain (sk)
query '''
select 1 as c, 'C1' as id, 'Alice' as name union all
select 2, 'C2', 'Bob' union all
select 3, 'C3', 'Cara'
''';

datasource orders (o: order_id, c: sk, amount: amount, flg: order_flag)
grain (order_id)
query '''
select 1 as o, 1 as c, 10.0 as amount, 'a' as flg union all
select 2, 1, 5.0, 'b' union all
select 3, 2, 7.0, 'a' union all
select 4, 3, 2.0, 'b'
''';
"""

QUERY = """
with annual as
select id as cid, order_flag as flg, sum(amount) as total;

with cust as
select
    annual.cid as cid,
    sum(annual.total ? annual.flg = 'a') as total_a,
    sum(annual.total ? annual.flg = 'b') as total_b;

where order_flag = 'a'
select
    cust.cid,
    name
subset join cust.cid = id
having cust.total_a > 0 and cust.total_b > 0
order by cust.cid asc
limit 100;
"""


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def test_pivot_subset_join_resources_the_raw_axis(executor):
    sql = executor.generate_sql(QUERY)[0]
    assert "on 1=1" not in sql.lower(), sql


def test_pivot_subset_join_rows(executor):
    rows = [tuple(row) for row in executor.execute_text(QUERY)[0].fetchall()]
    # Only C1 has both an 'a' and a 'b' order; the row filter keeps 'a'
    # customers. One row per customer, no order-grain fan-out.
    assert rows == [("C1", "Alice")]
