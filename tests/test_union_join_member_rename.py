"""A rename of a `union join` member is the coalesced axis, as the bare member
is, checked against hand-written DuckDB SQL: alone, on either side, beside an
aggregate of either side, in a 3-way relation, over model keys, and under a
HAVING presence test."""

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SS = "select * from (values (1,1,10.0),(2,1,5.0),(3,2,7.0)) as t(id,st,amt)"
SR = "select * from (values (1,1,2.0),(2,3,4.0)) as t(id,st,amt)"
WS = "select * from (values (1,3,1.0),(2,4,6.0)) as t(id,st,amt)"

MODEL = f"""
key ss_id int;
property ss_id.ss_store int;
property ss_id.ss_amt float;
key sr_id int;
property sr_id.sr_store int;
property sr_id.sr_amt float;
key ws_id int;
property ws_id.ws_store int;
property ws_id.ws_amt float;
datasource ss (id: ss_id, st: ss_store, amt: ss_amt) grain (ss_id)
query '''{SS}''';
datasource sr (id: sr_id, st: sr_store, amt: sr_amt) grain (sr_id)
query '''{SR}''';
datasource ws (id: ws_id, st: ws_store, amt: ws_amt) grain (ws_id)
query '''{WS}''';
with a as select ss_store as k, sum(ss_amt) as sales;
with b as select sr_store as k, sum(sr_amt) as rets;
with c as select ws_store as k, sum(ws_amt) as web;
"""

AXIS = """
with a as (select st as k, sum(amt) as sales from ss group by 1),
b as (select st as k, sum(amt) as rets from sr group by 1),
c as (select st as k, sum(amt) as web from ws group by 1),
ab as (select coalesce(a.k, b.k) as k, a.k as ak, b.k as bk, sales, rets
       from a full join b on a.k = b.k),
abc as (select coalesce(ab.k, c.k) as k, sales, rets, web
        from ab full join c on ab.k = c.k)
"""

CASES = [
    ("select a.k as k union join a.k = b.k;", "select k from ab"),
    ("select b.k as k union join a.k = b.k;", "select k from ab"),
    ("select a.k as k union join b.k = a.k;", "select k from ab"),
    (
        "select a.k as k, sum(b.rets) as r union join a.k = b.k;",
        "select k, sum(rets) from ab group by 1",
    ),
    (
        "select b.k as k, sum(a.sales) as s union join a.k = b.k;",
        "select k, sum(sales) from ab group by 1",
    ),
    (
        "select a.k as k, sum(a.sales) as s, sum(b.rets) as r union join a.k = b.k;",
        "select k, sum(sales), sum(rets) from ab group by 1",
    ),
    ("select a.k as k, a.sales as s union join a.k = b.k;", "select k, sales from ab"),
    (
        "select a.k as k union join a.k = b.k having b.k is null;",
        "select k from ab where bk is null",
    ),
    (
        "select a.k as k union join a.k = b.k having a.k is null;",
        "select k from ab where ak is null",
    ),
    (
        "with s as select a.k as k union join a.k = b.k having a.k is null;\nselect s.k;",
        "select k from ab where ak is null",
    ),
    ("select a.k as k union join a.k = b.k = c.k;", "select k from abc"),
    ("select c.k as k union join a.k = b.k = c.k;", "select k from abc"),
    (
        "select b.k as k, sum(c.web) as w union join a.k = b.k = c.k;",
        "select k, sum(web) from abc group by 1",
    ),
]


@pytest.fixture(scope="module")
def oracle():
    con = duckdb.connect()
    for name, query in (("ss", SS), ("sr", SR), ("ws", WS)):
        con.execute(f"create table {name} as {query}")
    yield con
    con.close()


def _sorted(rows):
    return sorted(
        (tuple(r) for r in rows), key=lambda r: tuple((v is None, str(v)) for v in r)
    )


@pytest.mark.parametrize("query,expected_sql", CASES, ids=[c[0] for c in CASES])
def test_union_join_member_rename(oracle, query, expected_sql):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL)
    got = executor.execute_query(query).fetchall()
    assert _sorted(got) == _sorted(oracle.execute(AXIS + expected_sql).fetchall())
