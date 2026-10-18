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
