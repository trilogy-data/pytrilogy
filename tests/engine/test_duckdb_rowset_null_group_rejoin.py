"""A NULL group label must survive the rejoin of split aggregate branches.

When one select asks for both a plain and a filtered aggregate over the same
rowset, v4 splits them into sibling branch nodes that group by the projected
dimensions and then rejoin on those group keys. A group key sourced from a `?`
column carries NULL as a VALUE, so that rejoin has to pair null-safely; a plain
`=` silently drops the whole NULL group (TPC-DS q72: 2008 groups -> 1604).
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key item_sk int;
property item_sk.item_desc string?;

datasource items (i_sk: item_sk, i_desc: item_desc)
grain (item_sk)
query '''select 10 as i_sk, 'alpha' as i_desc
union all select 20, 'beta'
union all select 30, cast(null as varchar)''';

key order_number int;
property order_number.quantity int;

datasource sales (o_num: order_number, i_sk: ~item_sk, qty: quantity)
grain (order_number, item_sk)
query '''select 1 as o_num, 10 as i_sk, 5 as qty
union all select 2, 20, 15
union all select 3, 30, 20
union all select 4, 30, 3''';
"""

ROWSET_QUERY = """
rowset s <- select order_number as o, item_sk as sk, item_desc as d, quantity as q;
select
    s.d,
    count(grain(s.o, s.sk)) as total,
    count(grain(s.o, s.sk) ? s.q > 10) as hi
order by s.d asc nulls last;
"""

DIRECT_QUERY = """
select
    item_desc as d,
    count(grain(order_number, item_sk)) as total,
    count(grain(order_number, item_sk) ? quantity > 10) as hi
order by item_desc asc nulls last;
"""

EXPECTED = [("alpha", 1, 0), ("beta", 1, 1), (None, 2, 1)]


@pytest.mark.parametrize("query", [ROWSET_QUERY, DIRECT_QUERY])
def test_null_dimension_group_survives_branch_rejoin(query):
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    assert executor.execute_query(query).fetchall() == EXPECTED


# An item no sale references: `grain(order_number, item_sk)` is keyed on the
# sale line, so it is absent there and the counts are 0, not 1 for a hash of
# padding (docs/keyspace_phase_plan.md, phase 4). Through the rowset too: the
# body pads the item, and the plan reading it holds the same region
# (`keyspace.RowsetWitness`), so the counts read the body's solid rows.
UNSOLD_MODEL = MODEL.replace(
    "union all select 30, cast(null as varchar)'''",
    "union all select 30, cast(null as varchar) union all select 40, 'gamma' '''",
)
UNSOLD_EXPECTED = [("alpha", 1, 0), ("beta", 1, 1), ("gamma", 0, 0), (None, 2, 1)]

# A rowset over a rowset: `t`'s witness is spelled in `t`'s handles, and the
# body of `s` respells them (`t.o as o2`), so the witness must be read through
# that body's canonical spelling to be a region of it at all.
NESTED_ROWSET_QUERY = """
rowset t <- select order_number as o, item_sk as sk, item_desc as d, quantity as q;
rowset s <- select t.o as o2, t.sk as sk2, t.d as d2, t.q as q2;
select
    s.d2,
    count(grain(s.o2, s.sk2)) as total,
    count(grain(s.o2, s.sk2) ? s.q2 > 10) as hi
order by s.d2 asc nulls last;
"""


@pytest.mark.parametrize("query", [DIRECT_QUERY, ROWSET_QUERY, NESTED_ROWSET_QUERY])
def test_unsold_item_counts_no_lines(query):
    env = Environment()
    env.parse(UNSOLD_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    assert executor.execute_query(query).fetchall() == UNSOLD_EXPECTED
    # the plan is typed at plan time, not narrowed after: the fact pairs
    # INNER with its complete dimension, the region's domain is preserved
    # over its solid readers, and nothing is FULL
    sql = executor.generate_sql(query)[-1]
    assert "FULL JOIN" not in sql, sql
