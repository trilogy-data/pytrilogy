from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.executor import Executor

MODEL = """
key customer_id int;
property customer_id.name string;
key date_id int;
property date_id.year int;
key order_id int;
property order_id.channel string;
property order_id.amount float;

datasource customers (cid: customer_id, cname: name) grain (customer_id) address customers;
datasource dates (did: date_id, yr: year) grain (date_id) address dates;
datasource sales (oid: order_id, cid: customer_id, did: date_id, ch: channel, amt: amount)
grain (order_id) address sales;

auto store_total <- sum(amount ? channel = 'STORE') by customer_id;
auto web_total <- sum(amount ? channel = 'WEB') by customer_id;
auto store_01 <- sum(amount ? channel = 'STORE' and year = 2001) by customer_id;
auto all_total <- sum(amount) by customer_id;
"""

SETUP = [
    "create table customers as select * from (values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')) t(cid,cname)",
    "create table dates as select * from (values (10,2001),(11,2002),(12,2003)) t(did,yr)",
    """create table sales as select * from (values
     (1,1,10,'STORE',10.0),(2,1,11,'WEB',5.0),
     (3,2,10,'STORE',7.0),
     (4,3,11,'WEB',3.0),(5,3,10,'CATALOG',50.0),
     (6,4,10,'CATALOG',8.0),(7,4,11,'STORE',1.0),
     (8,5,11,'STORE',4.0),(9,5,10,'WEB',6.0),
     (10,9,10,'STORE',9.0),(11,9,10,'WEB',9.0),
     (12,NULL,10,'STORE',3.0),(13,NULL,11,'WEB',2.0),
     (14,2,99,'WEB',1.0),(15,4,NULL,'WEB',1.0)
    ) t(oid,cid,did,ch,amt)""",
]

FOLDED = (
    "where channel in ('STORE','WEB') and store_total > 0 and web_total > 0 "
    "select name order by name asc;"
)
COUNTED = (
    "where channel in ('STORE','WEB') and all_total > 0 and store_total > 0 "
    "select name order by name asc;"
)
QUERIES = [
    FOLDED,
    COUNTED,
    "where channel in ('WEB', 'CATALOG') and all_total > 5 select name order by name asc;",
    "where channel = 'WEB' and store_total > 0 select name as n order by n asc;",
    "where year = 2001 and store_total > 0 select name order by name asc;",
    "where year in (2001, 2002) and channel = 'WEB' and store_01 > 0 select name order by name asc;",
    "where channel = 'WEB' and all_total > 5 select name, customer_id order by name asc;",
    "where name like 'a%' and channel = 'WEB' and store_total > 0 select name order by name asc;",
    "where (channel = 'WEB' or year = 2003) and all_total > 1 select upper(name) as n order by n asc;",
]


@contextmanager
def _fold(enabled: bool) -> Iterator[None]:
    original = CONFIG.optimizations.fold_existence_into_aggregate
    CONFIG.optimizations.fold_existence_into_aggregate = enabled
    try:
        yield
    finally:
        CONFIG.optimizations.fold_existence_into_aggregate = original


@pytest.fixture(scope="module")
def executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    for statement in SETUP:
        executor.execute_raw_sql(statement)
    executor.parse_text(MODEL)
    return executor


def _compiled(executor: Executor, query: str, enabled: bool) -> str:
    with _fold(enabled):
        return executor.generate_sql(query)[-1]


def _fact_scans(sql: str) -> int:
    return sql.count('    "sales"\n') + sql.count('    "sales" as')


def _rows(executor: Executor, sql: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]


@pytest.mark.parametrize("query", QUERIES)
def test_fold_keeps_rows(executor: Executor, query: str):
    folded = _compiled(executor, query, True)
    assert _rows(executor, folded) == _rows(executor, _compiled(executor, query, False))


def test_implied_where_filters_the_aggregate_input(executor: Executor):
    sql = _compiled(executor, FOLDED, True)
    assert "count(CASE" not in sql
    assert _fact_scans(sql) == 1
    assert _rows(executor, sql) == [("a",), ("b",), ("d",), ("e",)]


def test_existence_becomes_a_having_count(executor: Executor):
    sql = _compiled(executor, COUNTED, True)
    assert "count(CASE" in sql
    assert _fact_scans(sql) == 1
    assert _rows(executor, sql) == [("a",), ("b",), ("d",), ("e",)]


def test_aggregate_with_filtered_input_is_not_folded(executor: Executor):
    sql = _compiled(
        executor,
        "where channel = 'WEB' and store_total > 0 select name order by name asc;",
        True,
    )
    assert "count(CASE" not in sql
    assert _rows(executor, sql) == [("a",), ("b",), ("d",), ("e",)]
