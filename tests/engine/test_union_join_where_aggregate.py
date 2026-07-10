from trilogy import Dialects

MODEL = """
key sale_id int;
property sale_id.sale_state string;
property sale_id.sale_category string;
property sale_id.sale_price int;
property sale_id.sale_year int;

datasource sales (
    id: sale_id,
    state: sale_state,
    category: sale_category,
    price: sale_price,
    y: sale_year
)
grain (sale_id)
query '''
select 1 id, 'NY' state, 'A' category, 15 price, 2001 y
union all select 2, 'NY', 'A', 5, 2001
union all select 3, 'NY', 'A', 15, 2000
union all select 4, 'CA', 'A', 5, 2001
''';

key price_id int;
property price_id.price_category string;
property price_id.current_price int;

datasource prices (
    id: price_id,
    category: price_category,
    price: current_price
)
grain (price_id)
query '''
select 1 id, 'A' category, 5 price
union all select 2, 'A', 15
''';

with category_avg as
select
    price_category as category,
    avg(current_price) as avg_price
;
"""


def test_union_join_output_aggregate_includes_entire_where():
    executor = Dialects.DUCK_DB.default_executor()
    statements = executor.parse_text(MODEL + """
select
    sale_state,
    count(sale_id) as qualifying_count
union join sale_category = category_avg.category
where
    sale_year = 2001
    and sale_price > category_avg.avg_price
having
    qualifying_count >= 1
order by
    sale_state
;
""")
    sql = executor.generate_sql(statements[-1])[-1]

    assert executor.execute_raw_sql(sql).fetchall() == [("NY", 1)]
