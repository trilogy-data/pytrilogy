from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key inv_item int;
key wh int;
property <inv_item, wh>.qty int;
key ss_item int;
key ticket int;
property <ss_item, ticket>.amt float;
datasource inventory (i: inv_item, w: wh, q: qty) grain (inv_item, wh)
query '''select * from (values (1,1,5),(2,1,6),(3,1,7)) as t(i,w,q)''';
datasource store_sales (i: ss_item, t: ticket, a: amt) grain (ss_item, ticket)
query '''select * from (values (1,1,1.0),(2,1,2.0),(4,2,3.0)) as t(i,t,a)''';
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_membership_set_beside_aggregate_over_the_set():
    rows = _rows(
        "select count_distinct(ss_item) as b, "
        "count_distinct(inv_item ? inv_item in ss_item) as m;"
    )
    assert rows == [(3, 2)]
