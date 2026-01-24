from trilogy import Dialects
from trilogy.hooks import DebuggingHook


def test_ranking_import():
    DebuggingHook()
    duckdb = Dialects.DUCK_DB.default_executor()
    all_results = duckdb.execute_query(
        """
import std.ranking;
import std.money;

key order int;
key order_score int::rank;
property order_item.amount numeric::usd;            
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
"""
    )

    results = all_results.fetchall()

    assert len(results) == 3, "Expected 3 result"
    assert results[0][0] == 3, "Expected order 3"
    assert results[0][1] == 6.0, "Expected amount 6.0"
    assert results[1][0] == 2, "Expected order 2"
    assert results[1][1] == 2.0, "Expected amount 4.0"
    assert results[2][0] == 1, "Expected order 1"
    assert results[2][1] == 2.0, "Expected amount 2.0"
