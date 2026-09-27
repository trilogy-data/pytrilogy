"""Rows-first probes for the three unmodelled region shapes of
docs/keyspace_phase_plan.md open item 2. `--sql <n>` prints the SQL of query n
of the chosen model (`--model A|B|C`)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from trilogy import Dialects

# ---------------------------------------------------------------- A: composite key
A_BASE = """
key vname string;
key variant string;
property <vname,variant>.vclass string;
key launch_id int;
property launch_id.mass int;

datasource vehicles (n: vname, v: variant, c: vclass)
grain (vname, variant)
query '''select 'A' as n, '1' as v, 'heavy' as c
union all select 'A', '2', 'heavy'
union all select 'B', '1', 'light'
union all select 'C', '1', 'medium' ''';
"""
A_LAUNCH_ROWS = """select 1 as id, 'A' as n, '1' as v, 10 as m
union all select 2, 'A', '1', 20
union all select 3, 'B', '1', 5"""
A_DERIVED = A_BASE + f"""
datasource launches (id: launch_id, n: ~vname, v: ~variant, m: mass)
grain (launch_id)
query '''{A_LAUNCH_ROWS}''';
auto status <- case when mass > 8 then 'big' else 'small' end;
"""
A_MATERIALIZED = A_BASE + f"""
property launch_id.status string;
datasource launches (id: launch_id, n: ~vname, v: ~variant, m: mass, s: status)
grain (launch_id)
query '''select l.*, case when l.m > 8 then 'big' else 'small' end as s from ({A_LAUNCH_ROWS}) l''';
"""
A_QUERIES = [
    "select vname, variant, status order by vname, variant, status nulls last",
    "select vname, variant, count(launch_id) as n order by vname, variant",
    "select vclass, count(launch_id) as n order by vclass",
    "select vname, variant, vclass, status order by vname, variant, status nulls last",
    "select vclass, status order by vclass, status nulls last",
    "select vname, variant, status where status is null order by vname",
    "select vname, variant, sum(mass) as t order by vname, variant",
    "select vname, count(status) as n order by vname",
    "select vclass, count(vname) as n order by vclass",
    "select vname, variant, status where vclass = 'medium' order by vname",
    "select vname, variant, count(status) as n, count(launch_id) as l order by vname, variant",
    "select vclass, status, count(launch_id) as n order by vclass, status nulls last",
    "select vname, status where vclass != 'light' order by vname, status nulls last",
    "select vname, variant, status, mass order by vname, variant, mass nulls last",
    "select vname, variant where status is null order by vname",
    "select vclass, count(launch_id) as n where status = 'big' or status is null order by vclass",
]

# ---------------------------------------------------------------- B: materialized rollup
B_BASE = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.amount int;
property order_id.delivery_date date?;

datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name union all select 2, 'bob' union all select 3, 'cat' ''';

datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount, delivery_date: delivery_date)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id, 10 as amount, date '2026-01-01' as delivery_date
union all select 101, 1, 20, null
union all select 102, 2, 30, date '2026-01-02' ''';

auto status <- case when delivery_date is not null then 'delivered' else 'in-transit' end;
auto total_amount <- sum(amount) by customer_id;
auto order_count <- count(order_id) by customer_id;
"""
B_SUMMARY = B_BASE + """
datasource customer_sales (customer_id: ~customer_id, total_amount: total_amount, order_count: order_count)
grain (customer_id)
query '''select 1 as customer_id, 30 as total_amount, 2 as order_count union all select 2, 30, 1''';
"""
B_QUERIES = [
    "select customer_id, total_amount order by customer_id",
    "select customer_id, name, total_amount order by customer_id",
    "select name, total_amount order by name",
    "select customer_id, order_count order by customer_id",
    "select customer_id, name, order_count order by customer_id",
    "select customer_id, total_amount, status order by customer_id, status nulls last",
    "select customer_id, total_amount where total_amount is null order by customer_id",
    "select customer_id, coalesce(total_amount, 0) as t order by customer_id",
    "select name, count(order_id) as n, total_amount order by name",
    "select customer_id, order_count, count(order_id) as n order by customer_id",
    "select customer_id, name where total_amount > 25 order by customer_id",
    "select customer_id, name where order_count = 0 or order_count is null order by customer_id",
    "select customer_id, status, order_count order by customer_id, status nulls last",
    "select status, sum(total_amount) as t order by status nulls last",
    "select customer_id, total_amount, order_count, count(status) as s order by customer_id",
    "select name, order_count where status is null order by name",
]

# ---------------------------------------------------------------- C: two facts on one dim
C_MODEL = """
key item_sk int;
property item_sk.item_desc string;
key sale_id int;
property sale_id.sale_amt int;
property sale_id.channel string;
key return_id int;
property return_id.ret_amt int;
property return_id.reason string;

datasource items (sk: item_sk, d: item_desc)
grain (item_sk)
query '''select 10 as sk, 'alpha' as d union all select 20, 'beta' union all select 30, 'gamma' union all select 40, 'delta' ''';

datasource sales (id: sale_id, sk: ~item_sk, amt: sale_amt, ch: channel)
grain (sale_id)
query '''select 1 as id, 10 as sk, 5 as amt, 'web' as ch
union all select 2, 10, 7, 'store'
union all select 3, 20, 9, 'web' ''';

datasource returns (id: return_id, sk: ~item_sk, amt: ret_amt, r: reason)
grain (return_id)
query '''select 1 as id, 10 as sk, 2 as amt, 'broken' as r
union all select 2, 30, 4, 'late' ''';
"""
# item 10: 2 sales (12), 1 return (2); 20: 1 sale (9), 0 returns; 30: 0 sales, 1 return (4); 40: nothing
C_EXPECTED = {
    "select item_sk, count(sale_id) as s, count(return_id) as r order by item_sk": [
        (10, 2, 1),
        (20, 1, 0),
        (30, 0, 1),
        (40, 0, 0),
    ],
    "select item_sk, sum(sale_amt) as s, sum(ret_amt) as r order by item_sk": [
        (10, 12, 2),
        (20, 9, None),
        (30, None, 4),
        (40, None, None),
    ],
    "select item_desc, count(sale_id) as s, count(return_id) as r order by item_desc": [
        ("alpha", 2, 1),
        ("beta", 1, 0),
        ("delta", 0, 0),
        ("gamma", 0, 1),
    ],
    "select item_sk, channel, count(return_id) as r order by item_sk, channel nulls last": [
        (10, "store", 1),
        (10, "web", 1),
        (20, "web", 0),
        (30, None, 1),
        (40, None, 0),
    ],
    "select item_sk, reason, count(sale_id) as s order by item_sk, reason nulls last": [
        (10, "broken", 2),
        (20, None, 1),
        (30, "late", 0),
        (40, None, 0),
    ],
    "select item_sk, count(sale_id) as s where reason is null order by item_sk": [
        (20, 1),
        (40, 0),
    ],
    "select item_sk, count(return_id) as r where channel is null order by item_sk": [
        (30, 1),
        (40, 0),
    ],
    "select item_sk, count(sale_id) as s, count(return_id) as r where item_desc != 'beta' order by item_sk": [
        (10, 2, 1),
        (30, 0, 1),
        (40, 0, 0),
    ],
    "select item_desc, sum(sale_amt) as s where ret_amt is null order by item_desc": [
        ("beta", 9),
        ("delta", None),
    ],
    "select item_sk, count(sale_id) as s where count(return_id) by item_sk = 0 order by item_sk": [
        (20, 1),
        (40, 0),
    ],
    "select item_sk, sum(sale_amt) - coalesce(sum(ret_amt), 0) as net order by item_sk": [
        (10, 10),
        (20, 9),
        (30, None),
        (40, None),
    ],
    "select channel, count(item_sk) as items, count(return_id) as r order by channel nulls last": [
        ("store", 1, 1),
        ("web", 2, 1),
        (None, 2, 1),
    ],
}


def _ex(model: str):
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(model)
    return ex


def _rows(ex, q):
    try:
        return [tuple(r) for r in ex.execute_text(q + ";")[-1].fetchall()]
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:160]}"


def twin(label, a_model, b_model, queries):
    print(f"\n##### {label}")
    a, b = _ex(a_model), _ex(b_model)
    for i, q in enumerate(queries):
        ra, rb = _rows(a, q), _rows(b, q)
        flag = "SAME" if ra == rb else "DIFF"
        print(f"[{i}] {flag} {q}")
        if ra != rb:
            print(f"      derived/base: {ra}\n      twin/summary: {rb}")


def expected(label, model, cases):
    print(f"\n##### {label}")
    ex = _ex(model)
    for i, (q, want) in enumerate(cases.items()):
        got = _rows(ex, q)
        flag = "OK  " if got == want else "BAD "
        print(f"[{i}] {flag} {q}")
        if got != want:
            print(f"      got:  {got}\n      want: {want}")


if __name__ == "__main__":
    which = None
    if "--model" in sys.argv:
        which = sys.argv[sys.argv.index("--model") + 1]
    if "--sql" in sys.argv:
        n = int(sys.argv[sys.argv.index("--sql") + 1])
        model, q = {
            "A": (A_DERIVED, A_QUERIES[n] if n < len(A_QUERIES) else None),
            "B": (B_SUMMARY, B_QUERIES[n] if n < len(B_QUERIES) else None),
            "C": (C_MODEL, list(C_EXPECTED)[n]),
        }[which]
        ex = _ex(model)
        print(_rows(ex, q))
        print(ex.generate_sql(q + ";")[-1])
        sys.exit()
    if which in (None, "A"):
        twin(
            "A composite-key ~ (derived vs materialized status)",
            A_DERIVED,
            A_MATERIALIZED,
            A_QUERIES,
        )
    if which in (None, "B"):
        twin(
            "B materialized rollup beside its base (base vs summary table)",
            B_BASE,
            B_SUMMARY,
            B_QUERIES,
        )
    if which in (None, "C"):
        expected("C two facts partial on one dim (hand-computed)", C_MODEL, C_EXPECTED)
