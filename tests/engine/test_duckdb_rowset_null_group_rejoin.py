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
