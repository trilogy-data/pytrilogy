"""Aggregates over a query-scoped `union join` between two rowsets.

The authored select is one dimension plus a plain and a filtered count over the
joined rows, with a flat WHERE relating the two sides. Three failures used to
compound here (q72's ingest formulation):

- the flat WHERE reached only the branch that routes through the grain
  projection, so the plain count aggregated an unfiltered population;
- the merge surfaced the relation axis onto the aggregate branches, adding a
  GROUP BY key and emitting one row per (dimension, axis) pair;
- the raw rowset boundary re-entered the final merge as an axis contributor,
  re-admitting the rows the filter had dropped.

Data: sales (order, item, week, qty, promo) x inventory (inv, item, week, qoh),
joined on (item, week). Only w100 has a pair passing `qoh < qty`.
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key order_id int;
key s_item int;
key s_week int;
property order_id.qty int;
property order_id.promo int?;

datasource sales (o: order_id, i: s_item, w: s_week, q: qty, p: promo)
grain (order_id)
query '''select 1 as o, 10 as i, 100 as w, 5 as q, cast(null as int) as p
union all select 2, 10, 100, 9, 7
union all select 3, 20, 100, 4, cast(null as int)
union all select 4, 20, 101, 8, 8''';

key inv_id int;
key i_item int;
key i_week int;
property inv_id.qoh int;

datasource inventory (id: inv_id, i: i_item, w: i_week, q: qoh)
grain (inv_id)
query '''select 100 as id, 10 as i, 100 as w, 3 as q
union all select 101, 10, 100, 7
union all select 102, 20, 100, 1
union all select 103, 20, 101, 99''';
"""

ROWSETS = """
rowset s <- select order_id, s_item as item, s_week as week, qty, promo;
rowset iv <- select inv_id, i_item as item, i_week as week, qoh;
"""

BOTH_COUNTS = """
select
    s.week,
    count(grain(s.order_id, iv.inv_id) ? s.promo is null) as no_promo,
    count(grain(s.order_id, iv.inv_id)) as total
union join s.item = iv.item and s.week = iv.week
order by s.week asc;
"""

TOTAL_ONLY = """
select
    s.week,
    count(grain(s.order_id, iv.inv_id)) as total
union join s.item = iv.item and s.week = iv.week
order by s.week asc;
"""

WHERE = "where iv.qoh < s.qty\n"


@pytest.mark.parametrize(
    "select_body,filtered,expected",
    [
        (BOTH_COUNTS, True, [(100, 2, 4)]),
        (BOTH_COUNTS, False, [(100, 3, 5), (101, 0, 1)]),
        (TOTAL_ONLY, True, [(100, 4)]),
        (TOTAL_ONLY, False, [(100, 5), (101, 1)]),
    ],
)
def test_union_join_aggregates_hold_the_authored_grain(select_body, filtered, expected):
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = ROWSETS + (WHERE if filtered else "") + select_body
    assert executor.execute_query(query).fetchall() == expected
