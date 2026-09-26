"""Open item 1 of docs/keyspace_phase_plan.md, rows first: a rowset whose key
has TWO properties (the witness picks the first carrier by name), a body
region no handle is keyed on alone, and an unsplit boundary joined to a
second holder of its region. Each rowset spelling against its direct one on
UNSOLD_MODEL and the two guest models. Prints SAME/DIFF, FULL, and the SQL
with `--sql`."""

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


def two_props(model: str) -> str:
    out = model.replace(
        "property item_sk.item_desc string?;",
        "property item_sk.item_desc string?;\nproperty item_sk.item_name string;",
    )
    out = out.replace(
        "datasource items (i_sk: item_sk, i_desc: item_desc)",
        "datasource items (i_sk: item_sk, i_desc: item_desc, i_name: item_name)",
    )
    out = out.replace("'alpha' as i_desc", "'alpha' as i_desc, 'A' as i_name")
    out = out.replace("select 20, 'beta'", "select 20, 'beta', 'B'")
    out = out.replace(
        "select 30, cast(null as varchar)", "select 30, cast(null as varchar), 'C'"
    )
    out = out.replace("select 30, 'delta'", "select 30, 'delta', 'C'")
    out = out.replace("select 40, 'gamma'", "select 40, 'gamma', 'G'")
    assert "i_name" in out and "'G'" in out, out
    return out


TWO = "rowset s <- select order_number as o, item_desc as d, item_name as n, quantity as q;\n"
# the second property alone: the witness spells the region by `d` (first by name)
ONE_N = "rowset s <- select order_number as o, item_name as n, quantity as q;\n"
# no handle keyed on the item alone; the body still counts the unsold item
NOKEY = "rowset s <- select order_number as o, count(item_sk) as n_items;\n"
NOKEY_D = "rowset s <- select order_number as o, concat(item_desc, '-', cast(order_number as string)) as label, quantity as q;\n"

PAIRS = [
    # two properties: read the one NOT picked as the span
    (
        TWO
        + "select s.n, count(grain(s.o, s.n)) as total order by s.n asc nulls last;",
        "select item_name as n, count(grain(order_number, item_name)) as total order by n asc nulls last;",
    ),
    (
        TWO + "select s.n, count(s.o) as total order by s.n asc nulls last;",
        "select item_name as n, count(order_number) as total order by n asc nulls last;",
    ),
    (
        TWO + "select s.n, sum(s.q) as total order by s.n asc nulls last;",
        "select item_name as n, sum(quantity) as total order by n asc nulls last;",
    ),
    (
        TWO
        + "select s.n, case when s.q > 10 then 'hi' else 'lo' end as band order by s.n asc nulls last, band asc nulls last;",
        "select item_name as n, case when quantity > 10 then 'hi' else 'lo' end as band order by n asc nulls last, band asc nulls last;",
    ),
    (
        TWO
        + "select s.n, s.d, count(s.o) as total order by s.n asc nulls last, s.d asc nulls last;",
        "select item_name as n, item_desc as d, count(order_number) as total order by n asc nulls last, d asc nulls last;",
    ),
    (
        TWO
        + "select s.n, count(s.o) as total, count(grain(s.o, s.n) ? s.q > 10) as hi order by s.n asc nulls last;",
        "select item_name as n, count(order_number) as total, count(grain(order_number, item_name) ? quantity > 10) as hi order by n asc nulls last;",
    ),
    (
        TWO
        + "select s.n, count(s.o) as total where s.d is null order by s.n asc nulls last;",
        "select item_name as n, count(order_number) as total where item_desc is null order by n asc nulls last;",
    ),
    (
        TWO
        + "select s.d, count(s.o) as total where s.n != 'B' order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as total where item_name != 'B' order by d asc nulls last;",
    ),
    # the non-nullable property alone as the span
    (
        ONE_N
        + "select s.n, count(grain(s.o, s.n)) as total order by s.n asc nulls last;",
        "select item_name as n, count(grain(order_number, item_name)) as total order by n asc nulls last;",
    ),
    (
        ONE_N
        + "select s.n, case when s.q > 10 then 'hi' else 'lo' end as band order by s.n asc nulls last, band asc nulls last;",
        "select item_name as n, case when quantity > 10 then 'hi' else 'lo' end as band order by n asc nulls last, band asc nulls last;",
    ),
    # no handle keyed on the item alone
    (
        NOKEY + "select s.o, s.n_items order by s.o asc nulls last;",
        "select order_number as o, count(item_sk) as n_items order by o asc nulls last;",
    ),
    (
        NOKEY + "select count(s.o) as orders, sum(s.n_items) as items;",
        "select count(order_number) as orders, sum(count(item_sk) by order_number) as items;",
    ),
    (
        NOKEY
        + "select s.n_items, count(s.o) as orders order by s.n_items asc nulls last;",
        "select count(item_sk) by order_number as n_items, count(order_number) as orders order by n_items asc nulls last;",
    ),
    (
        NOKEY
        + "select s.o, case when s.n_items > 1 then 'many' else 'one' end as band order by s.o asc nulls last;",
        "select order_number as o, case when count(item_sk) by order_number > 1 then 'many' else 'one' end as band order by o asc nulls last;",
    ),
    (
        NOKEY + "select s.o, s.n_items where s.o is null;",
        "select order_number as o, count(item_sk) as n_items where order_number is null;",
    ),
    (
        NOKEY_D + "select s.label, s.q order by s.label asc nulls last;",
        "select concat(item_desc, '-', cast(order_number as string)) as label, quantity as q order by label asc nulls last;",
    ),
    (
        NOKEY_D + "select s.label, count(s.o) as n order by s.label asc nulls last;",
        "select concat(item_desc, '-', cast(order_number as string)) as label, count(order_number) as n order by label asc nulls last;",
    ),
    # an unsplit boundary beside a second holder: a rowset read twice at
    # different grains, and beside a direct read of the model
    (
        t.KEYED_ROWSET
        + "select s.d, s.q, count(s.o) by s.d as n order by s.d asc nulls last, s.q asc nulls last;",
        "select item_desc as d, quantity as q, count(order_number) by item_desc as n order by d asc nulls last, q asc nulls last;",
    ),
    (
        t.KEYED_ROWSET
        + "select s.sk, s.d, count(s.o) as n, sum(s.q) by s.d as per_desc order by s.sk asc;",
        "select item_sk as sk, item_desc as d, count(order_number) as n, sum(quantity) by item_desc as per_desc order by sk asc;",
    ),
    (
        t.KEYED_ROWSET
        + "select s.d, count(s.o) as n, count(s.o) by * as all_orders order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as n, count(order_number) by * as all_orders order by d asc nulls last;",
    ),
    (
        t.KEYED_ROWSET
        + "select s.sk, s.d, s.o, s.q, rank s.o by s.q desc as r order by s.sk asc, s.o asc nulls last;",
        "select item_sk as sk, item_desc as d, order_number as o, quantity as q, rank order_number by quantity desc as r order by sk asc, o asc nulls last;",
    ),
    (
        t.KEYED_ROWSET
        + "select s.d, count(s.o) as n where s.q > 4 or s.q is null order by s.d asc nulls last;",
        "select item_desc as d, count(order_number) as n where quantity > 4 or quantity is null order by d asc nulls last;",
    ),
    (
        t.KEYED_ROWSET
        + "select s.d, s.o, s.q, s.q * 2 as dbl order by s.d asc nulls last, s.o asc nulls last;",
        "select item_desc as d, order_number as o, quantity as q, quantity * 2 as dbl order by d asc nulls last, o asc nulls last;",
    ),
    (
        t.KEYED_ROWSET
        + "select s.sk, s.d, count(s.o) as n, sum(s.q) as tq, max(s.q) as mq, count(grain(s.o, s.sk) ? s.q > 10) as hi order by s.sk asc;",
        "select item_sk as sk, item_desc as d, count(order_number) as n, sum(quantity) as tq, max(quantity) as mq, count(grain(order_number, item_sk) ? quantity > 10) as hi order by sk asc;",
    ),
]


def run(executor, q):
    try:
        return executor.execute_query(q).fetchall()
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:200]}"


def sql(executor, q):
    try:
        return executor.generate_sql(q)[-1]
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:200]}"


MODELS = [
    ("UNSOLD", two_props(t.UNSOLD_MODEL)),
    ("GUEST", two_props(t.GUEST_MODEL)),
    ("GUEST_ALLDESC", two_props(t.GUEST_ALLDESC_MODEL)),
]
only = [a for a in sys.argv[1:] if not a.startswith("--")]

for name, model in MODELS:
    if only and name not in only:
        continue
    print(f"\n##### {name}")
    env = Environment()
    env.parse(model)
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    for i, (rq, dq) in enumerate(PAIRS):
        r, d = run(ex, rq), run(ex, dq)
        s = sql(ex, rq)
        full = "FULL" if "FULL JOIN" in s else "    "
        print(f"[{i:2}]", ("SAME" if r == d else "DIFF"), full, rq.splitlines()[-1])
        if r != d:
            print("    rowset:", r)
            print("    direct:", d)
        if "--sql" in sys.argv and (r != d or "--all" in sys.argv):
            print(s)
