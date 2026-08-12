from trilogy import Dialects


def test_ranking_import():
    duckdb = Dialects.DUCK_DB.default_executor()
    all_results = duckdb.execute_query("""
import std.ranking;
import std.money;

key order int;
key order_score int::rank;
property order.amount numeric::usd;            
datasource orders (
    order:order,
    order_score:order_score,
    amount:amount)
grain (order)
query '''
SELECT
    1 AS order,
    1 AS order_score,
    2.0 as amount,
    union all
    select 2 as order,
    2 as order_score,
    2.0 as amount
    union all
    select 3 as order,
    3 as order_score,
    6.0 as amount
    '''
;

SELECT
    order,
    amount,
    --order_score

order by order_score desc;
""")

    results = all_results.fetchall()

    assert len(results) == 3, "Expected 3 result"
    assert results[0][0] == 3, "Expected order 3"
    assert results[0][1] == 6.0, "Expected amount 6.0"
    assert results[1][0] == 2, "Expected order 2"
    assert results[1][1] == 2.0, "Expected amount 4.0"
    assert results[2][0] == 1, "Expected order 1"
    assert results[2][1] == 2.0, "Expected amount 2.0"


TOP_X_MODEL = """
import std.ranking;

key order int;
key order_item int;
property order_item.amount float;
datasource orders (
    order:order,
    order_item:order_item,
    amount:amount)
grain (order_item)
query '''
select 1 as order, 1 as order_item, 2.0 as amount
union all select 1 as order, 2 as order_item, 3.0 as amount
union all select 2 as order, 3 as order_item, 10.0 as amount
union all select 3 as order, 4 as order_item, 1.0 as amount
'''
;
"""


def test_is_top_x_by():
    duckdb = Dialects.DUCK_DB.default_executor()
    results = duckdb.execute_query(TOP_X_MODEL + """
where @is_top_x_by(order, sum(amount), 2)
select order, sum(amount) as total
order by total desc;
""").fetchall()

    assert results == [(2, 10.0), (1, 5.0)]


def test_is_top_x_by_default_count():
    duckdb = Dialects.DUCK_DB.default_executor()
    results = duckdb.execute_query(TOP_X_MODEL + """
where @is_top_x_by(order, sum(amount))
select order, sum(amount) as total
order by total desc;
""").fetchall()

    assert results == [(2, 10.0), (1, 5.0), (3, 1.0)]


def test_is_top_x_by_as_select_output():
    duckdb = Dialects.DUCK_DB.default_executor()
    results = duckdb.execute_query(TOP_X_MODEL + """
select order, @is_top_x_by(order, sum(amount), 2) as is_top
order by order asc;
""").fetchall()

    assert results == [(1, True), (2, True), (3, False)]
