"""Keyless rowset (region spelled by the value-nullable `s.d`) vs the direct
spelling, on UNSOLD_MODEL and on a model with a guest sale (`~?item_sk`, NULL
key). Prints SAME/DIFF, whether the rowset plan has a FULL, and the SQL when
asked."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment

spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

GUEST_MODEL = t.UNSOLD_MODEL.replace("i_sk: ~item_sk", "i_sk: ~?item_sk").replace(
    "union all select 4, 30, 3'''",
    "union all select 4, 30, 3 union all select 5, null, 7'''",
)
assert "~?item_sk" in GUEST_MODEL and "select 5, null, 7" in GUEST_MODEL

R = t.KEYLESS_ROWSET
PAIRS = [
    (
        R + "select s.d, count(grain(s.o, s.d)) as total order by s.d asc nulls last;",
        "select item_desc as d, count(grain(order_number, item_desc)) as total order by d asc nulls last;",
    ),
    (
        R
        + "select s.d, case when s.q > 10 then 'hi' else 'lo' end as band order by s.d asc nulls last, band asc nulls last;",
        "select item_desc as d, case when quantity > 10 then 'hi' else 'lo' end as band order by d asc nulls last, band asc nulls last;",
    ),
    (
        R + "select s.d, count(s.o) as total order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as total order by d asc nulls last;",
    ),
    (
        R + "select s.d, sum(s.q) as total order by s.d asc nulls last;",
        "select item_desc as d, sum(quantity) as total order by d asc nulls last;",
    ),
    (
        R
        + "select s.d, count(s.o) as total where s.d is null order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as total where item_desc is null order by d asc nulls last;",
    ),
    (
        R
        + "select s.d, count(s.o) as total where s.q > 4 or s.q is null order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as total where quantity > 4 or quantity is null order by d asc nulls last;",
    ),
    (
        R + "select s.d, s.o, s.q order by s.d asc nulls last, s.o asc nulls last;",
        "select item_desc as d, order_number as o, quantity as q order by d asc nulls last, o asc nulls last;",
    ),
    (
        R
        + "select s.d, count(s.o) as total, count(grain(s.o, s.d) ? s.q > 10) as hi order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as total, count(grain(order_number, item_desc) ? quantity > 10) as hi order by d asc nulls last;",
    ),
    (
        R + "select s.d, max(s.q) as m order by s.d asc nulls last;",
        "select item_desc as d, max(quantity) as m order by d asc nulls last;",
    ),
]


def run(executor, q):
    try:
        return executor.execute_query(q).fetchall()
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:160]}"


def sql(executor, q):
    try:
        return executor.generate_sql(q)[-1]
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:160]}"


GUEST_ALLDESC = GUEST_MODEL.replace("cast(null as varchar)", "'delta'")
assert "'delta'" in GUEST_ALLDESC

for name, model in [
    ("UNSOLD", t.UNSOLD_MODEL),
    ("GUEST", GUEST_MODEL),
    ("GUEST_ALLDESC", GUEST_ALLDESC),
]:
    print(f"\n##### {name}")
    env = Environment()
    env.parse(model)
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    for rq, dq in PAIRS:
        r, d = run(ex, rq), run(ex, dq)
        s = sql(ex, rq)
        full = "FULL" if "FULL JOIN" in s else "    "
        print(("SAME" if r == d else "DIFF"), full, rq.splitlines()[-1])
        if r != d:
            print("    rowset:", r)
            print("    direct:", d)
        if "--sql" in sys.argv and full.strip():
            print(s)
