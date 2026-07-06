"""Regression lock for the v4 rollup-key dimension fold
(`_fold_rollup_key_dims` in `v4_helper/group_graph.py`, the q80 family).

A BASIC dim that is purely a function of a ROLLUP group's grouping keys
(`channel <- case chan`, `outlet <- 'x' || txt`) must be folded into the
GROUP BY ROLLUP node so it is emitted as a column there and carries the
rolled-up key values on the subtotal/grand-total rows. Otherwise it buckets
into its own leaf-grain BASIC group and is joined back on the raw keys; at a
subtotal row the rolled-up key is NULL, the join finds no leaf match, and the
dim comes back NULL — dropping the dimension value v3 preserves.

This is v4-only (`CONFIG.use_v4_discovery`), so it does not run in the default
v3 coverage pass; the fixture enables it here so the fold body is exercised
every run. The discriminating evidence is the per-channel subtotal rows
(`('aa', None, 30.0)`, `('bb', None, 30.0)`): `channel` is non-NULL there only
because the derived dim rides the rollup node."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key chan int;
key oid int;
property <oid, chan>.txt string;
property <oid, chan>.amt float;
datasource sales (chan: chan, oid: oid, txt: txt, amt: amt)
grain (oid, chan)
query '''select 1 as chan, 1 as oid, 'p' as txt, 10.0 as amt
         union all select 1, 2, 'q', 20.0
         union all select 2, 1, 'p', 30.0''';
"""

_QUERY = """
select
  case when chan = 1 then 'aa' else 'bb' end as channel,
  'x' || txt as outlet,
  sum(amt) as sales,
by rollup (chan, txt)
order by channel asc, sales asc, outlet asc nulls first;
"""


def test_rollup_key_derived_dim_folds_into_rollup_node():
    env = Environment()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.parse_text(_MODEL)
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        rows = executor.execute_text(_QUERY)[-1].fetchall()
    finally:
        CONFIG.use_v4_discovery = prior
    assert [(r[0], r[1], float(r[2])) for r in rows] == [
        ("aa", "xp", 10.0),
        ("aa", "xq", 20.0),
        ("aa", None, 30.0),  # per-channel subtotal: channel folded, not NULL
        ("bb", None, 30.0),  # per-channel subtotal: channel folded, not NULL
        ("bb", "xp", 30.0),
        ("bb", None, 60.0),  # grand total
    ]
