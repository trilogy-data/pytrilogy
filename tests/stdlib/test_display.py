from trilogy import Dialects
from trilogy.hooks import DebuggingHook


def test_calc_percent():
    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor()
    all_results = duckdb.execute_query(
        """
import std.report;
import std.money;
import std.display;

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
    7.0 as amount
    '''
;

SELECT
    order,
    @calc_percent(amount, sum(amount) by *) as percent_of_total,
    @calc_percent(amount, sum(amount) by *, 1) as percent_of_total_rounded
order by
    order asc
    ;
"""
    )

    results = all_results.fetchall()
    assert len(results) == 3
    assert results[0][0] == 1
    assert round(results[0][1], 2) == 0.18
    assert round(results[0][2], 2) == 0.2
    assert results[1][0] == 2
    assert round(results[1][1], 2) == 0.18
    assert round(results[1][2], 2) == 0.2
    assert results[2][0] == 3
    assert round(results[2][1], 2) == 0.64
    assert round(results[2][2], 2) == 0.6
