"""Flat WHERE conjuncts do not filter each other: an aggregate read only by the
WHERE sees every row, while the row atoms decide which entities qualify. The
outputs here come from an entity peel keyed by `customer_id`, so the row atom
needs a host FINAL keeps; the statement's row stream carries no output."""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

MODEL = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.channel string;
property order_id.amount float;
property order_id.year int;

datasource customers (cid: customer_id, cname: name) grain (customer_id) address customers;
datasource sales (oid: order_id, cid: customer_id, ch: channel, amt: amount, yr: year)
grain (order_id) address sales;

auto store_total <- sum(amount ? channel = 'STORE') by customer_id;
auto all_total <- sum(amount) by customer_id;
auto store_ok <- filter customer_id where store_total > 0;
"""

SETUP = [
    "create table customers as select * from (values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')) t(cid,cname)",
    """create table sales as select * from (values
     (1,1,'STORE',10.0,2001),(2,1,'WEB',5.0,2001),
     (3,2,'STORE',7.0,2001),
     (4,3,'WEB',3.0,2002),(5,3,'CATALOG',50.0,2001),
     (6,4,'CATALOG',8.0,2001),(7,4,'STORE',1.0,2002),
     (8,5,'STORE',4.0,2002),(9,5,'WEB',6.0,2001)
    ) t(oid,cid,ch,amt,yr)""",
]


@pytest.fixture(scope="module")
def executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    for statement in SETUP:
        executor.execute_raw_sql(statement)
    executor.parse_text(MODEL)
    return executor


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "where channel = 'WEB' and store_total > 0 select name order by name asc;",
            [("a",), ("e",)],
        ),
        (
            "where channel = 'WEB' and store_total > 0 select name as n order by n asc;",
            [("a",), ("e",)],
        ),
        (
            "where channel = 'WEB' and store_total > 0 select upper(name) as n order by n asc;",
            [("A",), ("E",)],
        ),
        (
            "where channel = 'WEB' and all_total > 10 select name order by name asc;",
            [("a",), ("c",)],
        ),
        (
            "where year = 2001 and store_total > 5 select name order by name asc;",
            [("a",), ("b",)],
        ),
        (
            "where channel = 'WEB' and customer_id in store_ok select name order by name asc;",
            [("a",), ("e",)],
        ),
    ],
)
def test_where_aggregate_reads_every_row(
    executor: Executor, query: str, expected: list[tuple]
):
    assert [tuple(r) for r in executor.execute_text(query)[-1].fetchall()] == expected
