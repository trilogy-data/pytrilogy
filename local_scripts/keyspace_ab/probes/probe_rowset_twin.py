"""Open item 1, rows first, on models the witness probes never read through a
rowset: the oracle twin (`test_derived_key_domain.py`) and a two-family model.

Two comparisons per pair. ORACLE: the rowset query on the derived model
against the same query on the materialized twin (must match: materialization
invariance through a rowset boundary). DESIGN: the rowset query against its
direct spelling on the derived model (a rowset is a row source, so a DIFF here
is a question, not a bug, until hand-checked). `--sql <n>` prints the rowset
plan of pair n on the derived model; `--only twin|fam` picks a model."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
from trilogy import Dialects

spec = importlib.util.spec_from_file_location(
    "t", str(REPO / "tests" / "engine" / "test_derived_key_domain.py")
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

K = "rowset s <- select customer_id as c, name as n, order_id as o, status as st, amount as a, undelivered as u;\n"
KL = "rowset s <- select name as n, order_id as o, status as st, amount as a;\n"
AG = "rowset s <- select customer_id as c, name as n, count(order_id) as k, sum(amount) as t;\n"
WH = "rowset s <- select customer_id as c, name as n, status as st, amount as a where status is null or status = 'delivered';\n"
WHN = "rowset s <- select customer_id as c, name as n, status as st where status = 'delivered';\n"
UND = "rowset s <- select order_id as o, status as st, label as l;\n"
NEST = (
    "rowset t <- select customer_id as c, name as n, order_id as o, status as st;\n"
    "rowset s <- select t.c as c2, t.n as n2, t.o as o2, t.st as st2;\n"
)

TWIN_PAIRS = [
    (K, "select s.c, s.st;", "select customer_id, status;"),
    (
        K,
        "select s.c, s.n where s.st is null;",
        "select customer_id, name where status is null;",
    ),
    (K, "select s.n, count(s.o) as k;", "select name, count(order_id) as k;"),
    (K, "select s.st, count(s.c) as k;", "select status, count(customer_id) as k;"),
    (
        K,
        "select s.c, count(s.o) as k where s.st is null;",
        "select customer_id, count(order_id) as k where status is null;",
    ),
    (
        K,
        "select s.c, s.n where count(s.o) by s.c = 0;",
        "select customer_id, name where count(order_id) by customer_id = 0;",
    ),
    (K, "select s.n where s.st is null;", "select name where status is null;"),
    (
        K,
        "select s.c, case when s.st = 'delivered' then 1 else 0 end as f;",
        "select customer_id, case when status = 'delivered' then 1 else 0 end as f;",
    ),
    (
        K,
        "select s.c, coalesce(sum(s.a), 0) as t;",
        "select customer_id, coalesce(sum(amount), 0) as t;",
    ),
    (
        K,
        "select s.st, count(s.c) as c, sum(s.a) as t;",
        "select status, count(customer_id) as c, sum(amount) as t;",
    ),
    (
        K,
        "select s.c, concat(s.n, '-', s.st) as lbl;",
        "select customer_id, label as lbl;",
    ),
    (K, "select s.o, s.st;", "select order_id, status;"),
    (
        K,
        "select s.c, s.st where s.c in (2, 3);",
        "select customer_id, status where customer_id in (2, 3);",
    ),
    (
        K,
        "select s.c, s.n where s.u = true;",
        "select customer_id, name where undelivered = true;",
    ),
    (
        K,
        "select s.c, s.st, count(s.o) as k where s.u = true or s.u is null;",
        "select customer_id, status, count(order_id) as k where undelivered = true or undelivered is null;",
    ),
    (
        K,
        "select s.c, s.n, s.st where s.st is null or s.a > 15;",
        "select customer_id, name, status where status is null or amount > 15;",
    ),
    (K, "select s.n as n2, s.st;", "select name as n2, status;"),
    (
        K,
        "select s.c, count(s.st) as ns, count(s.o) as no;",
        "select customer_id, count(status) as ns, count(order_id) as no;",
    ),
    (
        K,
        "select s.c, s.st, case when count(s.o) by s.c > 0 then 'active' else 'dormant' end as act;",
        "select customer_id, status, activity as act;",
    ),
    (
        K,
        "select s.c, s.st where case when count(s.o) by s.c > 0 then 'active' else 'dormant' end = 'dormant';",
        "select customer_id, status where activity = 'dormant';",
    ),
    (
        K,
        "select s.c, s.o, row_number s.o over s.c order by s.a asc as seq;",
        "select customer_id, order_id, order_seq as seq;",
    ),
    (
        K,
        "select s.c, s.o, rank s.o by s.a desc as r;",
        "select customer_id, order_id, order_rank as r;",
    ),
    (K, "select s.n, count(s.st) as k;", "select name, count(status) as k;"),
    (
        K,
        "select s.c, s.st where s.n = 'cat';",
        "select customer_id, status where name = 'cat';",
    ),
    (
        K,
        "select s.c, s.n where sum(s.a) by s.c is null;",
        "select customer_id, name where sum(amount) by customer_id is null;",
    ),
    (KL, "select s.n, s.st;", "select name, status;"),
    (KL, "select s.n, count(s.o) as k;", "select name, count(order_id) as k;"),
    (KL, "select s.st, count(s.n) as k;", "select status, count(name) as k;"),
    (KL, "select s.n where s.st is null;", "select name where status is null;"),
    (KL, "select s.n, count(s.st) as k;", "select name, count(status) as k;"),
    (
        KL,
        "select s.n, s.st where s.st is null or s.a > 15;",
        "select name, status where status is null or amount > 15;",
    ),
    (AG, "select s.c, s.k;", "select customer_id, count(order_id) as k;"),
    (
        AG,
        "select s.n, s.k, s.t;",
        "select name, count(order_id) as k, sum(amount) as t;",
    ),
    (
        AG,
        "select s.c where s.k = 0;",
        "select customer_id where count(order_id) by customer_id = 0;",
    ),
    (
        AG,
        "select s.k, count(s.c) as cust;",
        "select count(order_id) by customer_id as k, count(customer_id) as cust;",
    ),
    (
        AG,
        "select s.c, case when s.k > 0 then 'active' else 'dormant' end as act;",
        "select customer_id, activity as act;",
    ),
    (
        WH,
        "select s.c, s.st;",
        "select customer_id, status where status is null or status = 'delivered';",
    ),
    (
        WH,
        "select s.c, s.n;",
        "select customer_id, name where status is null or status = 'delivered';",
    ),
    (
        WH,
        "select s.c, count(s.st) as k;",
        "select customer_id, count(status) as k where status is null or status = 'delivered';",
    ),
    (
        WHN,
        "select s.c, s.st;",
        "select customer_id, status where status = 'delivered';",
    ),
    (
        WHN,
        "select s.c, s.n, count(s.st) as k;",
        "select customer_id, name, count(status) as k where status = 'delivered';",
    ),
    (UND, "select s.o, s.l;", "select order_id, label as l;"),
    (UND, "select s.l, count(s.o) as k;", "select label as l, count(order_id) as k;"),
    (NEST, "select s.c2, s.st2;", "select customer_id, status;"),
    (NEST, "select s.n2, count(s.o2) as k;", "select name, count(order_id) as k;"),
    (
        NEST,
        "select s.c2, s.n2 where s.st2 is null;",
        "select customer_id, name where status is null;",
    ),
    (
        NEST,
        "select s.st2, count(s.c2) as k;",
        "select status, count(customer_id) as k;",
    ),
]

FAMILY_MODEL = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.cost float;
key item_id int;
property item_id.qty int;

datasource users (u: user_id, st: state)
grain (user_id)
query '''select 1 as u, 'CA' as st union all select 2, 'NY' union all select 3, 'TX' ''';

datasource products (p: product_id, c: cost)
grain (product_id)
query '''select 10 as p, 1.5 as c union all select 20, 2.5 union all select 30, 3.5''';

datasource items (i: item_id, u: ~user_id, p: ~product_id, q: qty)
grain (item_id)
query '''select 100 as i, 1 as u, 10 as p, 2 as q
union all select 101, 1, 20, 3
union all select 102, 2, 10, 4''';
"""

F = "rowset s <- select item_id as i, user_id as u, product_id as p, state as st, cost as c, qty as q;\n"
FU = "rowset s <- select item_id as i, user_id as u, state as st, qty as q;\n"

FAMILY_PAIRS = [
    (F, "select s.u, count(s.i) as k;", "select user_id, count(item_id) as k;"),
    (F, "select s.p, count(s.i) as k;", "select product_id, count(item_id) as k;"),
    (
        F,
        "select s.u, s.p, count(s.i) as k;",
        "select user_id, product_id, count(item_id) as k;",
    ),
    (F, "select s.st, count(s.i) as k;", "select state, count(item_id) as k;"),
    (
        F,
        "select s.u, s.st, sum(s.q * s.c) as rev;",
        "select user_id, state, sum(qty * cost) as rev;",
    ),
    (
        F,
        "select s.u where count(s.i) by s.u = 0;",
        "select user_id where count(item_id) by user_id = 0;",
    ),
    (
        F,
        "select s.p, s.c, count(s.i) as k where s.c > 2;",
        "select product_id, cost, count(item_id) as k where cost > 2;",
    ),
    (F, "select s.u, s.p;", "select user_id, product_id;"),
    (
        F,
        "select s.i, s.u, s.p, s.q * s.c as rev;",
        "select item_id, user_id, product_id, qty * cost as rev;",
    ),
    (
        F,
        "select s.st, s.p, count(s.i) as k;",
        "select state, product_id, count(item_id) as k;",
    ),
    (
        F,
        "select s.u, s.st where s.q is null;",
        "select user_id, state where qty is null;",
    ),
    (F, "select s.u, s.p, s.q;", "select user_id, product_id, qty;"),
    (FU, "select s.u, sum(s.q) as t;", "select user_id, sum(qty) as t;"),
    (FU, "select s.st, count(s.i) as k;", "select state, count(item_id) as k;"),
    (
        FU,
        "select s.u, s.st where s.i is null;",
        "select user_id, state where item_id is null;",
    ),
    (FU, "select s.i, s.st;", "select item_id, state;"),
]


def rows(executor, q):
    try:
        out = [tuple(r) for r in executor.execute_query(q).fetchall()]
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:160]}"
    return sorted(out, key=lambda r: tuple((v is None, str(v)) for v in r))


def sql(executor, q):
    try:
        return executor.generate_sql(q)[-1]
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:160]}"


def executor_for(model):
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(model)
    return ex


args = sys.argv[1:]
only = args[args.index("--only") + 1] if "--only" in args else None
show = int(args[args.index("--sql") + 1]) if "--sql" in args else None

if only in (None, "twin"):
    print("##### TWIN (derived vs materialized through the rowset; rowset vs direct)")
    derived = executor_for(t._DERIVED + t._ACTIVITY)
    materialized = executor_for(t._MATERIALIZED + t._ACTIVITY)
    for i, (body, rq, dq) in enumerate(TWIN_PAIRS):
        rd, rm, dd = (
            rows(derived, body + rq),
            rows(materialized, body + rq),
            rows(derived, dq),
        )
        oracle = "SAME" if rd == rm else "DIFF"
        design = "same" if rd == dd else "diff"
        print(
            f"[{i:2}] oracle={oracle} design={design} {body.split('<-')[0].strip()} | {rq}"
        )
        if rd != rm:
            print("    rowset/derived:     ", rd)
            print("    rowset/materialized:", rm)
        if rd != dd:
            print("    rowset/derived:", rd)
            print("    direct/derived:", dd)
        if show == i:
            print(sql(derived, body + rq))

if only in (None, "fam"):
    print("\n##### FAMILY (rowset vs direct)")
    fam = executor_for(FAMILY_MODEL)
    for i, (body, rq, dq) in enumerate(FAMILY_PAIRS):
        r, d = rows(fam, body + rq), rows(fam, dq)
        print(f"[{i:2}] {'SAME' if r == d else 'DIFF'} | {rq}")
        if r != d:
            print("    rowset:", r)
            print("    direct:", d)
        if show == i:
            print(sql(fam, body + rq))
