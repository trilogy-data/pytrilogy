"""Rows-first neighbours of the two open-item-3 fixes: (1) a FINAL atom whose
condition scan pairs a sole region holder on a span the output grain hides;
(2) a predicate over a zero-filled COUNT under an aggregate by another key.
Twins print SAME/DIFF; hand-computed models print OK/BAD."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from probe_unmodelled import (
    A_DERIVED,
    A_MATERIALIZED,
    B_BASE,
    B_SUMMARY,
    C_MODEL,
    _ex,
    _rows,
    expected,
    twin,
)


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, str(REPO / rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


tdk = _load("tdk", "tests/engine/test_derived_key_domain.py")
rj = _load("rj", "tests/engine/test_duckdb_rowset_null_group_rejoin.py")

TDK_QUERIES = [
    # (1) name-only / label-only WHERE over an absent value or a per-key count
    "select name where status is null",
    "select label where status is null",
    "select name where amount is null",
    "select name where order_seq is null",
    "select name where activity = 'dormant'",
    "select name where count(order_id) by customer_id < 2",
    "select name where sum(amount) by customer_id is null",
    "select name where status is null or amount > 15",
    "select name, activity where status is null",
    "select upper(name) as u where status is null",
    # (2) zero-filled count predicate under an aggregate by another key
    "select status, count(customer_id) as n where count(order_id) by customer_id < 2",
    "select label, count(customer_id) as n where count(order_id) by customer_id = 0",
    "select status, sum(amount) as t where count(order_id) by customer_id = 0",
    "select status, count(customer_id) as n where count(order_id) by customer_id = 0 or status = 'delivered'",
    "select activity, count(customer_id) as n where count(order_id) by customer_id = 0",
    "select status, count(customer_id) as n where count(status) by customer_id = 0",
    "select status, count(customer_id) as n where count(order_id) by customer_id != 1",
    "select status, count(name) as n where count(order_id) by customer_id = 0",
    "select status, max(name) as m where count(order_id) by customer_id = 0",
    "select status, count(customer_id) as n where count(order_id) by customer_id = 0 and name != 'bob'",
    "select status, count(customer_id) as n where count(order_id) by customer_id = 0 or count(order_id) by customer_id = 2",
]

A_QUERIES = [
    "select vclass where status is null order by vclass",
    "select vclass where status is null or status = 'big' order by vclass",
    "select vclass where mass is null order by vclass",
    "select vclass where count(launch_id) by vname, variant = 0 order by vclass",
    "select vname where count(launch_id) by vname, variant = 0 order by vname",
    "select vclass, count(launch_id) as n where count(launch_id) by vname, variant = 0 order by vclass",
    "select status, count(vname) as n where count(launch_id) by vname, variant = 0 order by status nulls last",
    "select status, count(vname) as n where count(launch_id) by vname, variant < 2 order by status nulls last",
]

B_QUERIES = [
    "select name where order_count = 0 or order_count is null order by name",
    "select name where total_amount is null order by name",
    "select name where status is null order by name",
    "select status, count(customer_id) as n where order_count = 0 or order_count is null order by status nulls last",
    "select status, count(customer_id) as n where count(order_id) by customer_id = 0 order by status nulls last",
    "select name where count(order_id) by customer_id = 0 order by name",
]

# items: 10 alpha (2 sales, 1 return), 20 beta (1 sale), 30 gamma (1 return), 40 delta (nothing)
C_CASES = {
    "select item_desc where reason is null order by item_desc": [("beta",), ("delta",)],
    "select item_desc where channel is null order by item_desc": [
        ("delta",),
        ("gamma",),
    ],
    "select item_desc where count(return_id) by item_sk = 0 order by item_desc": [
        ("beta",),
        ("delta",),
    ],
    "select item_desc where count(sale_id) by item_sk = 0 and count(return_id) by item_sk = 0 order by item_desc": [
        ("delta",)
    ],
    "select item_desc, count(sale_id) as s where count(return_id) by item_sk = 0 order by item_desc": [
        ("beta", 1),
        ("delta", 0),
    ],
    "select channel, count(item_sk) as items where count(return_id) by item_sk = 0 order by channel nulls last": [
        ("web", 1),
        (None, 1),
    ],
    "select reason, count(item_sk) as items where count(sale_id) by item_sk = 0 order by reason nulls last": [
        ("late", 1),
        (None, 1),
    ],
    "select channel, count(item_sk) as items where count(return_id) by item_sk = 0 or channel = 'store' order by channel nulls last": [
        ("store", 1),
        ("web", 1),
        (None, 1),
    ],
    "select item_desc where sum(ret_amt) by item_sk is null and channel is null order by item_desc": [
        ("delta",)
    ],
}

# rowset vs direct spelling on the unsold-item models
UNSOLD_PAIRS = [
    (
        "select item_desc where count(order_number) by item_sk = 0 order by item_desc",
        "rowset s <- select item_sk, count(order_number) as n; select item_desc where s.n = 0 order by item_desc",
    ),
    (
        "select item_desc where quantity is null order by item_desc",
        "rowset s <- select item_sk, item_desc, quantity; select s.item_desc where s.quantity is null order by s.item_desc",
    ),
    (
        "select item_desc, count(order_number) as n where count(order_number) by item_sk = 0 order by item_desc",
        "rowset s <- select item_sk, count(order_number) as n; select item_desc, s.n where s.n = 0 order by item_desc",
    ),
]


def pairs(label, model, cases):
    print(f"\n##### {label}")
    ex = _ex(model)
    for i, (a, b) in enumerate(cases):
        ra, rb = _rows(ex, a), _rows(ex, b)
        flag = "SAME" if ra == rb else "DIFF"
        print(f"[{i}] {flag} {a}\n           vs {b}")
        if ra != rb:
            print(f"      direct: {ra}\n      rowset: {rb}")


def _trace_placements() -> None:
    """`--trace`: print buckets, group-graph edges and placements per plan."""
    from trilogy.core.processing.v4_helper import group_graph as gg
    from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID

    orig = gg.plan_condition_placements

    def short(gid: str) -> str:
        return gid.replace("local.", "")

    def traced(group_graph, group_edges, buckets, conditions, mandatory_list, *rest):
        print("  buckets:")
        for gid, b in buckets.items():
            print(
                f"    {short(gid)}: grain={sorted(b.grain_components)}"
                f" primary={[short(m) for m in b.primary_members]}"
                f" secondary={[short(m) for m in b.secondary_members]}"
                f" extent={sorted(b.extent_spans) if b.extent_spans else None}"
            )
        print("  edges:")
        for u, v in group_graph.edges:
            kind = group_edges.get((u, v))
            print(
                f"    {short(u)} -> {short(v) if v != FINAL_NODE_ID else 'FINAL'}"
                f" [{kind.kind.name if kind else None}]"
            )
        placements = orig(
            group_graph, group_edges, buckets, conditions, mandatory_list, *rest
        )
        print("  placements:")
        for p in placements:
            args = sorted(short(a.address) for a in p.atom.row_arguments)
            print(
                f"    {args} -> {[short(g) if g != FINAL_NODE_ID else 'FINAL' for g in p.group_ids]} {p.reason.name}"
            )
        return placements

    gg.plan_condition_placements = traced


if __name__ == "__main__":
    if "--trace" in sys.argv:
        _trace_placements()
    which = sys.argv[sys.argv.index("--model") + 1] if "--model" in sys.argv else None
    if "--sql" in sys.argv:
        n = int(sys.argv[sys.argv.index("--sql") + 1])
        model, queries = {
            "T": (tdk._DERIVED + tdk._ACTIVITY, TDK_QUERIES),
            "M": (tdk._MATERIALIZED + tdk._ACTIVITY, TDK_QUERIES),
            "A": (A_DERIVED, A_QUERIES),
            "B": (B_SUMMARY, B_QUERIES),
            "C": (C_MODEL, list(C_CASES)),
            "U": (rj.UNSOLD_MODEL, [p[0] for p in UNSOLD_PAIRS]),
        }[which]
        q = queries[n]
        ex = _ex(model)
        print(_rows(ex, q))
        print(ex.generate_sql(q + ";")[-1])
        sys.exit()
    if which in (None, "T"):
        d, m = tdk._executor(tdk._DERIVED), tdk._executor(tdk._MATERIALIZED)
        print("\n##### T derived vs materialized (test_derived_key_domain twin)")
        for i, q in enumerate(TDK_QUERIES):
            ra, rb = _rows(d, q), _rows(m, q)
            flag = "SAME" if sorted(map(str, ra)) == sorted(map(str, rb)) else "DIFF"
            print(
                f"[{i}] {flag} {q}\n      {ra if isinstance(ra, str) else sorted(ra, key=str)}"
            )
            if flag == "DIFF":
                print(
                    f"      twin: {rb if isinstance(rb, str) else sorted(rb, key=str)}"
                )
    if which in (None, "A"):
        twin("A composite-key ~", A_DERIVED, A_MATERIALIZED, A_QUERIES)
    if which in (None, "B"):
        twin("B rollup summary beside base", B_BASE, B_SUMMARY, B_QUERIES)
    if which in (None, "C"):
        expected("C two facts (hand rows)", C_MODEL, C_CASES)
    if which in (None, "U"):
        for name in ("UNSOLD_MODEL", "GUEST_ALLDESC_MODEL", "TWO_PROP_MODEL"):
            pairs(f"U {name} rowset vs direct", getattr(rj, name), UNSOLD_PAIRS)
