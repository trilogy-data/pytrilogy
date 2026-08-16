"""q95's `count(<key> ? <helper>) by <key>` over a nullable partial datasource.

Companion to tests/test_filtered_count_at_regroup_grain.py, which pins the same
defect on a synthetic model. This file covers the configuration the q95 agent
actually hit and that the synthetic one cannot reach: `is_returned` is computed
from a LEFT-join-padded column of a partial datasource, so the filtered content
renders as a `coalesce(WR_ORDER_NUMBER, WS_ORDER_NUMBER)` union key rather than a
plain column. It compiled to `count(max(CASE ...))`, which DuckDB rejects.
"""

import re

NESTED_AGGREGATE = re.compile(
    r"(count|count_distinct|sum|avg|min|max|array_agg)\s*\(\s*(distinct\s+)?max\s*\(",
    re.IGNORECASE,
)

QUERY = """
import web_sales as ws;

select
    ws.order_number,
    count(ws.order_number ? ws.is_returned) by ws.order_number as returned_flag
order by ws.order_number asc
;
"""

TRUTH = """
select ws.WS_ORDER_NUMBER,
       count(distinct case when wr.WR_ORDER_NUMBER is not null
                           then ws.WS_ORDER_NUMBER end)
from memory.web_sales ws
left join memory.web_returns wr
  on ws.WS_ITEM_SK = wr.WR_ITEM_SK
 and ws.WS_ORDER_NUMBER = wr.WR_ORDER_NUMBER
group by 1
order by 1 asc
"""

# the agent-written construct from the original report
ELIGIBLE_QUERY = """
import web_sales as ws;

auto eligible_order <- (count_distinct(ws.warehouse.sk) by ws.order_number) > 1
                   and (count(ws.order_number ? ws.is_returned) by ws.order_number) > 0;
select
    ws.order_number
where eligible_order
order by ws.order_number asc
;
"""

ELIGIBLE_TRUTH = """
select ws.WS_ORDER_NUMBER
from memory.web_sales ws
left join memory.web_returns wr
  on ws.WS_ITEM_SK = wr.WR_ITEM_SK
 and ws.WS_ORDER_NUMBER = wr.WR_ORDER_NUMBER
group by 1
having count(distinct ws.WS_WAREHOUSE_SK) > 1
   and count(distinct case when wr.WR_ORDER_NUMBER is not null
                           then ws.WS_ORDER_NUMBER end) > 0
order by 1 asc
"""


def test_q95_filtered_count_by_order_matches_nullable_return_control(engine):
    sql = engine.generate_sql(QUERY)[-1]
    assert not NESTED_AGGREGATE.search(sql), sql
    expected = engine.execute_raw_sql(TRUTH).fetchall()
    assert engine.execute_raw_sql(sql).fetchall() == expected


def test_q95_eligible_order_conjunct_plans_and_matches(engine):
    sql = engine.generate_sql(ELIGIBLE_QUERY)[-1]
    assert not NESTED_AGGREGATE.search(sql), sql
    expected = engine.execute_raw_sql(ELIGIBLE_TRUTH).fetchall()
    assert engine.execute_raw_sql(sql).fetchall() == expected
