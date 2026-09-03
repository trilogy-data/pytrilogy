from trilogy import Dialects

FIXTURE = """
key sale_id int;
property sale_id.channel string;
property sale_id.amount float;

datasource sales (sale_id: sale_id, channel: channel, amount: amount)
grain (sale_id)
query '''
select 1 as sale_id, 'A' as channel, 10.0 as amount union all
select 2, 'B', 20.0 union all
select 3, 'A', 5.0
''';
"""


def test_order_by_case_over_unprojected_leaf_executes():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(FIXTURE)
    rows = executor.execute_text("""
select lower(channel) as ch, sum(amount) as total
order by case when channel = 'B' then 1 else 2 end asc;
""")[0].fetchall()
    assert [tuple(r) for r in rows] == [("b", 20.0), ("a", 15.0)]


def test_order_by_comparison_over_unprojected_leaf_executes():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(FIXTURE)
    rows = executor.execute_text("""
select lower(channel) as ch, sum(amount) as total
order by channel = 'A' desc;
""")[0].fetchall()
    assert [tuple(r) for r in rows] == [("a", 15.0), ("b", 20.0)]


def test_order_by_simple_case_over_unprojected_leaf_executes():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(FIXTURE)
    rows = executor.execute_text("""
select lower(channel) as ch, sum(amount) as total
order by case channel when 'B' then 1 else 2 end asc;
""")[0].fetchall()
    assert [tuple(r) for r in rows] == [("b", 20.0), ("a", 15.0)]
