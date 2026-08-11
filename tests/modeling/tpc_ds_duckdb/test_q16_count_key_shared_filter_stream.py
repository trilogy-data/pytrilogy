"""Regression for `count(<key>)` over-counting beside a filtered sibling count.

`count(order_number)` is planned as dedup-then-COUNT: the normalization GROUP
collapses the input stream to the aggregate's input grain (`order_number`), so a
plain COUNT of the key is already distinct. That GROUP must also project every
argument the bucket's aggregates read, and `count(order_number ? is_returned)`
contributes a filter virtual that `order_number` does NOT determine. It became a
second GROUP BY key, so an order with both returned and unreturned lines
survived as two rows and the sibling count reported 274,743 for the 160,000
distinct catalog orders (q16 messy-warehouse probe).
"""

import pytest

# The first attempt (rewrite the flagged COUNTs to COUNT(DISTINCT) at bucket
# partition time) was reverted: it fires blind to whether the builder will
# actually emit the normalization GROUP, so it broke `count(x ? x)` over a
# `const x <- unnest([...])`, whose plan has no GROUP at all and whose count is
# a row count by design. The fix belongs at the assembly site instead — group to
# the declared `aggregate_input_grain` and collapse the un-determined argument.
pytestmark = pytest.mark.xfail(
    reason="q16 count(<key>) grain contract not yet enforced at the group node",
    strict=False,
)

QUERY = """
import catalog_sales as cs;

where 1=1
select
    count(cs.order_number) as all_orders,
    count_distinct(cs.warehouse.sk) as all_warehouses,
    count(cs.order_number ? cs.is_returned = true) as returned_orders
;
"""

TRUTH = """
select
    count(distinct cs.CS_ORDER_NUMBER),
    (select count(distinct W_WAREHOUSE_SK) from memory.warehouse),
    count(distinct case when cr.CR_ORDER_NUMBER is not null
                        then cs.CS_ORDER_NUMBER end)
from memory.catalog_sales cs
left join memory.catalog_returns cr
    on cs.CS_ITEM_SK = cr.CR_ITEM_SK
   and cs.CS_ORDER_NUMBER = cr.CR_ORDER_NUMBER
"""


def test_q16_count_key_is_distinct_beside_filtered_sibling(engine):
    expected = engine.execute_raw_sql(TRUTH).fetchall()[0]
    assert engine.execute_text(QUERY)[-1].fetchall() == [expected]


BY_DIM_QUERY = """
import catalog_sales as cs;

select
    cs.warehouse.sk,
    count(cs.order_number) as all_orders,
    count(cs.order_number ? cs.is_returned = true) as returned_orders
order by cs.warehouse.sk asc
;
"""

BY_DIM_TRUTH = """
select
    cs.CS_WAREHOUSE_SK,
    count(distinct cs.CS_ORDER_NUMBER),
    count(distinct case when cr.CR_ORDER_NUMBER is not null
                        then cs.CS_ORDER_NUMBER end)
from memory.catalog_sales cs
left join memory.catalog_returns cr
    on cs.CS_ITEM_SK = cr.CR_ITEM_SK
   and cs.CS_ORDER_NUMBER = cr.CR_ORDER_NUMBER
group by 1
order by 1 asc
"""


def test_q16_count_key_is_distinct_at_a_grouped_output_grain(engine):
    expected = engine.execute_raw_sql(BY_DIM_TRUTH).fetchall()
    assert engine.execute_text(BY_DIM_QUERY)[-1].fetchall() == expected


def test_q16_count_key_alone_still_dedups(engine):
    # The rewrite is scoped to a widened bucket; a lone count(key) keeps the
    # cheaper dedup-then-COUNT shape.
    sql = engine.generate_sql(
        "import catalog_sales as cs;\nselect count(cs.order_number) as c;"
    )[-1]
    assert "count(distinct" not in sql
    assert engine.execute_raw_sql(sql).fetchall() == [(160000,)]
