from trilogy import Dialects

MODEL = """
key a_id int;
property a_id.a_x int;
datasource a (id: a_id, x: a_x) grain (a_id)
query '''select 1 as id, 1 as x
union all select 2 as id, 1 as x
union all select 3 as id, 2 as x''';

key b_x int;
property b_x.vb int;
datasource b (x: b_x, vb: vb) grain (b_x)
query '''select 1 as x, 100 as vb
union all select 3 as x, 300 as vb''';
"""


def test_rowset_body_does_not_build_under_the_statement_join():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    rows = executor.execute_text("""
with rs as select a_x, count(a_id) as c;
select b_x, vb, rs.a_x, rs.c
union join a_x = b_x = rs.a_x
order by b_x asc;
""")[-1].fetchall()
    assert [tuple(r) for r in rows] == [
        (1, 100, 1, 2),
        (2, None, 2, 1),
        (3, 300, 3, None),
    ]
