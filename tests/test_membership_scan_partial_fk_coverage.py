from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key demo int;
key cust int;
property cust.city string;
key ticket int;
key item int;
datasource demos (d: demo) grain (demo)
query '''select * from (values (1),(2),(3),(4)) as t(d)''';
datasource customers (c: cust, city: city, d: demo) grain (cust)
query '''select * from (values (1,'E',1),(2,'E',2),(3,'X',3)) as t(c,city,d)''';
datasource returns (t: ticket, i: item, d: ~?demo) grain (ticket, item)
query '''select * from (values (1,1,1),(1,2,null),(2,1,2),(3,1,3),(4,1,1)) as t(t,i,d)''';
rowset t <- where city = 'E' select demo as demo_sk;
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_filtered_grain_count_on_partial_fk_under_rowset_membership():
    rows = _rows(
        "where demo in t.demo_sk "
        "select count(grain(ticket, item) ? demo is not null) as with_demo;"
    )
    assert rows == [(3,)]
