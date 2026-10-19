from decimal import Decimal

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
query '''select * from (values (1,1,10.0),(2,1,5.0),(3,2,7.0),(4,null,3.0)) as t(id,st,amt)''';
datasource sr (id: sr_id, st: sr_store, amt: sr_amt) grain (sr_id)
query '''select * from (values (1,1,2.0),(2,3,4.0),(3,null,1.0)) as t(id,st,amt)''';
with a as select ss_store as k, sum(ss_amt) as sales;
with b as select sr_store as k, sum(sr_amt) as returns;
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return sorted((tuple(r) for r in e.execute_query(body).fetchall()), key=str)


def test_rowset_having_on_union_join_key():
    rows = _rows(
        "with s as select a.k as k, a.sales as sales, "
        "coalesce(b.returns, 0) as returns "
        "union join a.k = b.k having a.k is not null;\n"
        "select s.k, s.sales, s.returns;"
    )
    assert rows == [
        (1, Decimal("15.0"), Decimal("2.0")),
        (2, Decimal("7.0"), Decimal("0.0")),
    ]


def test_union_arms_having_on_union_join_key():
    rows = _rows(
        "with u as union("
        "(select 'store' as ch, a.k as k, a.sales as sales "
        "union join a.k = b.k having a.k is not null), "
        "(select 'web' as ch, b.k as k, b.returns as sales "
        "union join b.k = a.k having b.k is not null)"
        ") -> (ch, k, sales);\n"
        "select u.ch, u.k, sum(u.sales) as s;"
    )
    assert rows == [
        ("store", 1, Decimal("15.0")),
        ("store", 2, Decimal("7.0")),
        ("web", 1, Decimal("2.0")),
        ("web", 3, Decimal("4.0")),
    ]


SINGLE_SIDED = """
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
with b as select sr_store as k, sum(sr_amt) as rets;
"""


def _single_sided_rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(SINGLE_SIDED)
    return sorted((tuple(r) for r in e.execute_query(body).fetchall()), key=str)


def test_having_presence_of_the_unprojected_side():
    assert _single_sided_rows(
        "select a.k, a.sales union join a.k = b.k having b.k is null;"
    ) == [(2, Decimal("7.0"))]
    assert _single_sided_rows(
        "select a.k, a.sales union join a.k = b.k having b.k is not null;"
    ) == [(1, Decimal("15.0")), (3, None)]


def test_having_presence_of_the_projected_side():
    assert _single_sided_rows(
        "select a.k, a.sales union join a.k = b.k having a.k is null;"
    ) == [(3, None)]
