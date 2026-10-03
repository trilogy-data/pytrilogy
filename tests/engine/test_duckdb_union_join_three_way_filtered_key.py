from trilogy import Dialects

MODEL = """
key cid int;
property cid.name string;
key oid int;
key rid int;
property oid.o_cid int;
property rid.r_cid int;

datasource customers (id:cid, nm:name) grain (cid) query '''
select 1 id, 'a' as nm union all select 2, 'b' union all select 3, 'c'
''';
datasource orders (oid:oid, cid:o_cid) grain (oid) query '''
select 10 oid, 1 cid union all select 11, 2
''';
datasource returns (rid:rid, cid:r_cid) grain (rid) query '''
select 20 rid, 2 cid union all select 21, 3
''';
"""


def test_filtered_side_joins_on_the_coalesced_key():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql("""
where name != 'c'
select cid, name, count(oid) as orders, count(rid) as returns
union join o_cid = r_cid = cid
order by cid asc;
""")[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    assert [tuple(row) for row in rows] == [(1, "a", 1, 0), (2, "b", 1, 1)]
