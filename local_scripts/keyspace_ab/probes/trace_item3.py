"""Trace placements + buckets + group-graph edges for the ROOT (correct) and
rows-only (wrong) atoms under an aggregate by the span."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))

spec = importlib.util.spec_from_file_location(
    "tdk",
    str(REPO / "tests" / "engine" / "test_derived_key_domain.py"),
)
tdk = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tdk)

from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID

_orig = gg.plan_condition_placements


def _short(gid: str) -> str:
    return gid.replace("local.", "")


def _traced(group_graph, group_edges, buckets, conditions, mandatory_list, *rest):
    print("  buckets:")
    for gid, b in buckets.items():
        print(
            f"    {_short(gid)}: {b.derivation.name} depth={b.depth_label} grain={sorted(b.grain_components)}"
            f" primary={[_short(m) for m in b.primary_members]} secondary={[_short(m) for m in b.secondary_members]}"
            f" extent={sorted(b.extent_spans) if b.extent_spans else None} disc={b.discriminator!r}"
        )
    print("  edges:")
    for u, v in group_graph.edges:
        kind = group_edges.get((u, v))
        print(
            f"    {_short(u)} -> {_short(v) if v != FINAL_NODE_ID else 'FINAL'} [{kind}]"
        )
    placements = _orig(
        group_graph, group_edges, buckets, conditions, mandatory_list, *rest
    )
    print("  placements:")
    for p in placements:
        args = sorted(_short(a.address) for a in p.atom.row_arguments)
        print(
            f"    {args} -> {[_short(g) if g != FINAL_NODE_ID else 'FINAL' for g in p.group_ids]} {p.reason.name}"
        )
    return placements


gg.plan_condition_placements = _traced

derived = tdk._executor(tdk._DERIVED)
for q in [a for a in sys.argv[1:] if not a.startswith("--")] or [
    "select customer_id, count(order_id) as n where amount > 15 or amount is null",
    "select customer_id, count(order_id) as n where flag = 1 or flag is null",
]:
    print(f"\n=== {q} ===")
    try:
        print("  rows:", tdk._rows(derived, q))
    except Exception as e:
        import traceback

        print("  ERROR", type(e).__name__, str(e)[:300])
        print("".join(traceback.format_exc().splitlines(keepends=True)[-14:]))
    if "--sql" in sys.argv:
        try:
            print(derived.generate_sql(q + ";")[-1])
        except Exception as e:
            print("  SQL ERROR", type(e).__name__, str(e)[:200])
