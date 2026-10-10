from trilogy import Dialects

MODEL = """
key sale_id int;
property sale_id.qty int;
property sale_id.price int;
key web_id int;
property web_id.web_qty int;
property web_id.web_price int;

datasource sales (sale_id: sale_id, qty: qty, price: price)
grain (sale_id)
query '''
select 1 as sale_id, 2 as qty, 10 as price union all
select 2 as sale_id, 2 as qty, 10 as price union all
select 3 as sale_id, 0 as qty, 7 as price
''';

datasource web (web_id: web_id, web_qty: web_qty, web_price: web_price)
grain (web_id)
query '''
select 1 as web_id, 2 as web_qty, 10 as web_price union all
select 2 as web_id, 1 as web_qty, 5 as web_price
''';

with u as union(
    (select sale_id as o, qty as q, price as p),
    (select web_id as o, web_qty as q, web_price as p)
) -> (o, q, p);
"""


def test_union_filtered_sum_keeps_duplicate_lines():
    executor = Dialects.DUCK_DB.default_executor()
    rows = executor.execute_text(
        MODEL + "select sum(u.q * u.p ? u.q > 0) as filtered, sum(u.q * u.p) as plain,"
        " sum(u.p ? u.q > 0) as filtered_price;"
    )[-1].fetchall()
    assert [tuple(r) for r in rows] == [(65, 65, 35)]
