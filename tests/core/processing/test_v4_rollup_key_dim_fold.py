"""A BASIC dim that is purely a function of a rollup's grouping keys
(`channel <- case chan`, `outlet <- 'x' || txt`) carries the rolled-up key
values on the subtotal and grand-total rows (the q80 family).

The planner once folded such a dim into the ROLLUP group
(`_fold_rollup_key_dims`); with the arm behind its TVF it is computed in
the pass's own SELECT, over the rolled keys. The per-channel subtotal rows
(`('aa', None, 30.0)`, `('bb', None, 30.0)`) are the discriminating
evidence."""

from trilogy import Dialects, Environment

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
select *rollup((select
  case when chan = 1 then 'aa' else 'bb' end as channel,
  'x' || txt as outlet,
  sum(amt) as sales
order by channel asc, sales asc, outlet asc nulls first
), chan, txt);
"""


def test_rollup_key_derived_dim_folds_into_rollup_node():
    env = Environment()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.parse_text(_MODEL)
    rows = executor.execute_text(_QUERY)[-1].fetchall()
    assert [(r[0], r[1], float(r[2])) for r in rows] == [
        ("aa", "xp", 10.0),
        ("aa", "xq", 20.0),
        ("aa", None, 30.0),  # per-channel subtotal: channel folded, not NULL
        ("bb", None, 30.0),  # per-channel subtotal: channel folded, not NULL
        ("bb", "xp", 30.0),
        ("bb", None, 60.0),  # grand total
    ]
