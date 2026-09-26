"""TPC-H q22's shape: a scalar aggregate `by *` over a `~` customer region,
a per-country count, `then where`, and `order_id is null` for the customers
without an order. Each spelling is checked against reference SQL over the
same tables (docs/keyspace_phase_plan.md, open item "TPC-H q22")."""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

MODEL = """
key customer_id int;
property customer_id.phone string;
property customer_id.bal float;
key order_id int;
property order_id.amount int;

root datasource customers (customer_id: customer_id, phone: phone, bal: bal)
grain (customer_id)
address customers;

root datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount)
grain (order_id)
address orders;

auto cntrycode <- substring(phone, 1, 2);
auto avg_bal_in_target <- avg(bal) by *;
auto avg_bal <- avg(bal) by *;
"""

CUSTOMERS = """create table customers as
select 1 as customer_id, '13-111' as phone, 100.0 as bal union all
select 2, '13-222', 300.0 union all
select 3, '31-333', 500.0 union all
select 4, '31-444', -50.0 union all
select 5, '99-555', 900.0 union all
select 6, '13-666', 460.0"""

ORDERS = """create table orders as
select 100 as order_id, 1 as customer_id, 10 as amount union all
select 101, 1, 20 union all
select 102, 3, 20 union all
select 103, 5, 5"""

NO_ORDER = (
    "not exists (select 1 from orders o where o.customer_id = customers.customer_id)"
)

CASES = [
    (
        """where cntrycode in ('13', '31') and bal > 0
        then where bal > avg_bal_in_target and order_id is null
        select cntrycode, count(customer_id) as numcust, sum(bal) as totacctbal
        order by cntrycode asc;""",
        f"""select substring(phone,1,2) as c, count(*) as n, sum(bal) as t from customers
        where substring(phone,1,2) in ('13','31') and bal > 0
        and bal > (select avg(bal) from customers where substring(phone,1,2) in ('13','31') and bal > 0)
        and {NO_ORDER} group by 1 order by 1""",
    ),
    # one stage: the average is over every customer
    (
        """where cntrycode in ('13', '31') and bal > avg_bal and order_id is null
        select cntrycode, count(customer_id) as numcust, sum(bal) as totacctbal
        order by cntrycode asc;""",
        f"""select substring(phone,1,2) as c, count(*) as n, sum(bal) as t from customers
        where substring(phone,1,2) in ('13','31') and bal > (select avg(bal) from customers)
        and {NO_ORDER} group by 1 order by 1""",
    ),
    # the average beside a per-country count is one value, not weighted by orders
    (
        "select cntrycode, count(customer_id) as numcust, avg_bal order by cntrycode asc;",
        """select substring(phone,1,2) as c, count(*) as n, (select avg(bal) from customers) as a
        from customers group by 1 order by 1""",
    ),
    (
        "select cntrycode, count(order_id) as norders, avg_bal order by cntrycode asc;",
        """select substring(c.phone,1,2) as c, count(o.order_id) as n, (select avg(bal) from customers) as a
        from customers c left join orders o on o.customer_id = c.customer_id group by 1 order by 1""",
    ),
    (
        "where order_id is null select cntrycode, count(customer_id) as numcust order by cntrycode asc;",
        f"""select substring(phone,1,2) as c, count(*) as n from customers
        where {NO_ORDER} group by 1 order by 1""",
    ),
    (
        """where bal > 0
        then where bal > avg_bal_in_target and order_id is null
        select cntrycode, count(customer_id) as numcust order by cntrycode asc;""",
        f"""select substring(phone,1,2) as c, count(*) as n from customers
        where bal > 0 and bal > (select avg(bal) from customers where bal > 0)
        and {NO_ORDER} group by 1 order by 1""",
    ),
    # stage 1 rejects on the order side; the stage-2 average is over the customers
    # that survive it, each once, not once per order (weighted by customer 1's two
    # orders the average drops to 400 and country 31 would pass)
    (
        """where amount < 25
        then where bal > avg_bal_in_target
        select cntrycode, count(customer_id) as numcust order by cntrycode asc;""",
        """with s1 as (select distinct c.* from customers c join orders o on o.customer_id = c.customer_id where o.amount < 25)
        select substring(phone,1,2) as c, count(*) as n from s1
        where bal > (select avg(bal) from s1) group by 1 order by 1""",
    ),
    (
        "select customer_id, bal, avg_bal where bal > avg_bal order by customer_id asc;",
        """select customer_id, bal, (select avg(bal) from customers) as a from customers
        where bal > (select avg(bal) from customers) order by 1""",
    ),
]


@pytest.fixture(scope="module")
def executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(CUSTOMERS)
    executor.execute_raw_sql(ORDERS)
    executor.execute_text(MODEL)
    return executor


@pytest.mark.parametrize("query,reference", CASES)
def test_scalar_aggregate_over_a_region_matches_reference(
    executor: Executor, query: str, reference: str
):
    got = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    want = [tuple(r) for r in executor.execute_raw_sql(reference).fetchall()]
    assert got == want
    # an empty result agrees with anything
    assert got
