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
            f" primary={[_short(m) for m in b.primary_members]} secondary={[_short(m) for m in b.carried_keys]}"
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

if "--inject" in sys.argv:
    # every condition injection, patched where it is imported
    from trilogy.core.processing.v4_helper import condition_injection as ci
    from trilogy.core.processing.v4_helper import strategy_builder as sb
    from trilogy.core.processing.v4_node_generators import condition_sources as cs
    from trilogy.core.processing.v4_node_generators import root as rt

    _orig_inject = ci.inject_condition_at_node

    def _traced_inject(node, condition, output_concepts, environment, sources, **kw):
        out = _orig_inject(node, condition, output_concepts, environment, sources, **kw)
        print(
            f"  inject {condition.conditional} at {type(node).__name__}"
            f"[{','.join(_short(c.address) for c in node.output_concepts)}]"
            f" row_parents={[type(p).__name__ for p in sources.row_parents]}"
            f" -> {type(out).__name__} cond={out.conditions}"
            f" preexisting={out.preexisting_conditions}"
        )
        return out

    for mod in (sb, cs, rt):
        mod.inject_condition_at_node = _traced_inject

    from trilogy.core.processing.nodes import base_node as bn

    _orig_resolve = bn.StrategyNode.resolve

    def _traced_node_resolve(self, *a, **k):
        out = _orig_resolve(self, *a, **k)
        print(
            f"  resolve {type(self).__name__}"
            f"[{','.join(_short(c.address) for c in self.output_concepts)}]"
            f" cond={self.conditions} -> qds cond={out.condition}"
        )
        return out

    bn.StrategyNode.resolve = _traced_node_resolve

derived = tdk._executor(
    tdk._MATERIALIZED if "--materialized" in sys.argv else tdk._DERIVED
)
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
