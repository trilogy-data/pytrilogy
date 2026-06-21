from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key chan int;
key oid int;
property <oid, chan>.txt string;
property <oid, chan>.amt float;
property <oid, chan>.prof float;
property <oid, chan>.loss float;
datasource sales (chan: chan, oid: oid, txt: txt, amt: amt, prof: prof, loss: loss)
grain (oid, chan)
query '''select 1 as chan, 1 as oid, 'p' as txt, 10.0 as amt, 5.0 as prof, 1.0 as loss
         union all select 1, 2, 'q', 20.0, 8.0, 2.0
         union all select 2, 1, 'p', 30.0, 9.0, 3.0''';
"""

QUERY = """
select
  case when chan = 1 then 'aa' else 'bb' end as channel,
  concat('x', txt) as outlet,
  sum(amt) by rollup chan, txt as sales,
  sum(prof) - sum(coalesce(loss, 0)) by rollup chan, txt as profit
order by channel asc, sales asc, profit asc, outlet asc nulls first;
"""


def gen(v4: bool):
    CONFIG.use_v4_discovery = v4
    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.parse_text(_MODEL)
    sql = engine.generate_sql(QUERY)[-1]
    return sql


def run(v4: bool):
    CONFIG.use_v4_discovery = v4
    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.parse_text(_MODEL)
    rows = engine.execute_text(QUERY)[-1].fetchall()
    return [tuple(r) for r in rows]


import sys

mode = sys.argv[1] if len(sys.argv) > 1 else "both"
if mode == "rows":
    print("V3", run(False))
    print("V4", run(True))
else:
    if mode in ("v3", "both"):
        print("===== V3 =====")
        print(gen(False))
    if mode in ("v4", "both"):
        print("\n===== V4 =====")
        print(gen(True))
