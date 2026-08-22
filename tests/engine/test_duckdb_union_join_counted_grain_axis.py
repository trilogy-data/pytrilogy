"""A relation member inside a counted `grain(...)` tuple is not a grouping axis.

Under a `union join` to a rowset, an aggregate whose inputs ride the relation
computes per coalesced axis row, so its grouping grain is widened by the axis
members. `count(grain(a, b, ...))` counts DISTINCT combinations, though: a
relation member the author named in that tuple is the count's own dedup key,
not an axis its value varies along. Widening by it makes the member a branch
GROUP BY key, the branches re-pair on it, and the outer select can only dedup
-- per-member slivers instead of the authored totals (TPC-DS q72).
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key s_order int;
key s_item int;
property <s_order,s_item>.s_week int;
property <s_order,s_item>.s_qty int;
property <s_order,s_item>.s_promo int?;

datasource sales_ds (o: s_order, i: s_item, w: s_week, q: s_qty, p: s_promo)
grain (s_order, s_item)
query '''
select 1 as o, 1 as i, 10 as w, 10 as q, cast(null as int) as p
union all select 2 as o, 2 as i, 10 as w, 20 as q, 7 as p
union all select 3 as o, 1 as i, 11 as w, 30 as q, cast(null as int) as p
''';

key i_item int;
key i_week int;
key i_date int;
key i_wh int;
property <i_item,i_week,i_date,i_wh>.i_qoh int;

datasource inv_ds (i: i_item, w: i_week, d: i_date, h: i_wh, q: i_qoh)
grain (i_item, i_week, i_date, i_wh)
query '''
select 1 as i, 10 as w, 100 as d, 5 as h, 5 as q
union all select 2 as i, 10 as w, 100 as d, 5 as h, 3 as q
union all select 1 as i, 11 as w, 101 as d, 5 as h, 1 as q
union all select 1 as i, 11 as w, 102 as d, 5 as h, 99 as q
''';

rowset sales <- select s_order as order_number, s_item as item_sk,
    s_week as week_seq, s_qty as quantity, s_promo as promo_sk;
rowset inv_rows <- select i_item as item_sk, i_week as week_seq,
    i_date as date_sk, i_wh as warehouse_sk, i_qoh as quantity_on_hand;
"""

QUERY = """
where inv_rows.quantity_on_hand < sales.quantity
select
    sales.week_seq,
    count(grain({tuple}) ? sales.promo_sk is null) as no_promo_cnt,
    count(grain({tuple})) as total_cnt
union join sales.item_sk = inv_rows.item_sk and sales.week_seq = inv_rows.week_seq
order by sales.week_seq asc;
"""

# the join key inside the counted tuple is the q72 shape; the same query with it
# dropped is the control that already grouped to the authored grain
WITH_RELATION_MEMBER = (
    "sales.order_number, sales.item_sk, inv_rows.date_sk, inv_rows.warehouse_sk"
)
WITHOUT_RELATION_MEMBER = "sales.order_number, inv_rows.date_sk, inv_rows.warehouse_sk"


@pytest.mark.parametrize("counted", [WITH_RELATION_MEMBER, WITHOUT_RELATION_MEMBER])
def test_counted_grain_tuple_member_is_not_an_axis(counted):
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = QUERY.format(tuple=counted)
    assert [tuple(r) for r in executor.execute_query(query).fetchall()] == [
        (10, 1, 2),
        (11, 1, 1),
    ]
