
from trilogy import Dialects
from trilogy.hooks import DebuggingHook


def test_top_x_by_metric():
    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor()
    all_results = duckdb.execute_query(
        """
import std.report;
import std.money;
                        
key order int;
key order_item int;
property order_item.amount numeric::usd;            
datasource orders (
    order:order,
    order_item:order_item,
    amount:amount)
grain (order_item)
query '''
SELECT
    1 AS order,
    1 AS order_item,
    2.0 as amount,
    union all
    select 2 as order,
    2 as order_item,
    2.0 as amount
    union all
    select 3 as order,
    3 as order_item,
    6.0 as amount
    '''
;

SELECT
    @top_x_by_metric(order, sum(amount), 1, -1) AS top_orders, sum(amount) as total_amount,

    order by top_orders desc;
"""
    )

    results = all_results.fetchall()

    assert len(results) == 2, "Expected 2 result"
    assert results[0][0] == 3, "Expected order 3"
    assert results[0][1] == 6.0, "Expected amount 6.0"
    assert results[1][0] == -1, "Expected order 2"
