from decimal import Decimal

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key ss_id int;
property ss_id.ss_store int;
property ss_id.ss_amt float;
key sr_id int;
property sr_id.sr_store int;
property sr_id.sr_amt float;
datasource ss (id: ss_id, st: ss_store, amt: ss_amt) grain (ss_id)
query '''select * from (values (1,1,10.0),(2,1,5.0),(3,2,7.0)) as t(id,st,amt)''';
datasource sr (id: sr_id, st: sr_store, amt: sr_amt) grain (sr_id)
query '''select * from (values (1,1,2.0),(2,3,4.0)) as t(id,st,amt)''';
with a as select ss_store as k, sum(ss_amt) as sales;
with b as select sr_store as k, sum(sr_amt) as returns;
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return sorted(tuple(r) for r in e.execute_query(body).fetchall())


def test_coalesce_over_padded_side_evaluates_after_join():
    rows = _rows(
        "select a.k, a.sales, coalesce(b.returns, 0) as returns "
        "union join a.k = b.k;"
    )
    assert rows == [
        (1, Decimal("15.0"), Decimal("2.0")),
        (2, Decimal("7.0"), Decimal("0.0")),
        (3, None, Decimal("4.0")),
    ]


def test_case_else_over_padded_side_evaluates_after_join():
    rows = _rows(
        "select a.k, a.sales, "
        "case when b.returns is null then 0 else b.returns end as returns "
        "union join a.k = b.k;"
    )
    assert rows == [
        (1, Decimal("15.0"), 2.0),
        (2, Decimal("7.0"), 0.0),
        (3, None, 4.0),
    ]


def test_key_only_side_keeps_its_rows_beside_coalesce():
    rows = _rows("select a.k, coalesce(b.returns, 0) as returns union join a.k = b.k;")
    assert rows == [
        (1, Decimal("2.0")),
        (2, Decimal("0.0")),
        (3, Decimal("4.0")),
    ]


def test_key_only_side_keeps_its_rows_beside_derived_value():
    rows = _rows("select a.k, b.returns + 1 as r union join a.k = b.k;")
    assert rows == [(1, Decimal("3.0")), (2, None), (3, Decimal("5.0"))]


PADDED_AXIS = """
with a as (select st as k, sum(amt) as sales from ss group by 1),
b as (select st as k, sum(amt) as ret from sr group by 1),
j as (select coalesce(a.k, b.k) as k, sales, ret from a full join b on a.k = b.k)
"""

NULL_ABSORBING_CASES = [
    (
        "select a.k, b.returns is null as miss union join a.k = b.k;",
        "select k, ret is null from j",
    ),
    (
        "select a.k, b.returns is not null as hit union join a.k = b.k;",
        "select k, ret is not null from j",
    ),
    (
        "select a.k, case when b.returns is null then 0 when b.returns > 0 then 1 end as c union join a.k = b.k;",
        "select k, case when ret is null then 0 when ret > 0 then 1 end from j",
    ),
    (
        "select a.k, concat('r', cast(b.returns as string)) as c union join a.k = b.k;",
        "select k, concat('r', cast(ret as varchar)) from j",
    ),
    (
        "select a.k, greatest(b.returns, 1) as g union join a.k = b.k;",
        "select k, greatest(ret, 1) from j",
    ),
    (
        "select a.k, b.returns * 2 as d union join a.k = b.k;",
        "select k, ret * 2 from j",
    ),
    (
        "select sum(coalesce(b.returns, -1)) as n union join a.k = b.k;",
        "select sum(coalesce(ret, -1)) from j",
    ),
    (
        "select sum(case when b.returns > 0 then 0 else 1 end) as n union join a.k = b.k;",
        "select sum(case when ret > 0 then 0 else 1 end) from j",
    ),
    (
        "select sum(b.returns * 2) as n union join a.k = b.k;",
        "select sum(ret * 2) from j",
    ),
    (
        "select a.k, sum(coalesce(b.returns, -1)) as n union join a.k = b.k;",
        "select k, sum(coalesce(ret, -1)) from j group by 1",
    ),
    (
        "select a.k, sum(case when b.returns is null then 1 else 0 end) as n union join a.k = b.k;",
        "select k, sum(case when ret is null then 1 else 0 end) from j group by 1",
    ),
    (
        "select count(concat('r', cast(b.returns as string))) as n union join a.k = b.k;",
        "select count(concat('r', cast(ret as varchar))) from j",
    ),
    (
        "select sum(b.returns) as n, sum(a.sales) as s union join a.k = b.k;",
        "select sum(ret), sum(sales) from j",
    ),
    (
        "auto x <- coalesce(b.returns, -1);\nselect sum(x) as n union join a.k = b.k;",
        "select sum(coalesce(ret, -1)) from j",
    ),
    (
        "where b.returns is null select a.k, a.sales union join a.k = b.k;",
        "select k, sales from j where ret is null",
    ),
]


@pytest.mark.parametrize("query,expected_sql", NULL_ABSORBING_CASES)
def test_null_sensitive_expression_over_padded_side(query, expected_sql):
    con = duckdb.connect()
    con.execute(
        "create table ss as select * from (values (1,1,10.0),(2,1,5.0),(3,2,7.0)) as t(id,st,amt)"
    )
    con.execute(
        "create table sr as select * from (values (1,1,2.0),(2,3,4.0)) as t(id,st,amt)"
    )
    expected = sorted(
        tuple(r) for r in con.execute(PADDED_AXIS + expected_sql).fetchall()
    )
    assert _rows(query) == expected
