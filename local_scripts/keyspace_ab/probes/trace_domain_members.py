"""Trace argv queries on probe_domain_members' DERIVED model (`--model F` for
the forked one, `--materialized` for the twin): the `[v4] built grp:` bucket
lines, every plan-time `get_join_type` call (`--joins`), and the SQL
(`--sql`)."""

import inspect
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import probe_domain_members as p

from trilogy import Dialects, Environment
from trilogy.core.processing import join_resolution as jr

KEEP = ("built grp", "fail", "skip", "could not", "FINAL", "contributor")


class _Filter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "--all" in sys.argv or any(k in msg for k in KEEP)


def _short(s: str) -> str:
    s = s.replace("local.", "")
    return s if len(s) < 90 else s[:44] + "…" + s[-44:]


def _trace_joins() -> None:
    orig = jr.get_join_type
    sig = inspect.signature(orig)

    def side_of(bound, name, side):
        d = bound.arguments.get(name) or {}
        return sorted(_short(k) for k in d.get(side, []) or [])

    def traced(*args, **kwargs):
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        a = bound.arguments
        result = orig(*args, **kwargs)
        print(
            f"  JOIN {result.name}: keys={sorted(_short(k) for k in a['all_connecting_keys'])}"
        )
        for side, label in ((a["left"], "L"), (a["right"], "R")):
            holders = (a.get("region_holders") or {}).get(side, set())
            print(
                f"    {label} {_short(side)}\n"
                f"       partial={side_of(bound, 'partials', side)} nullable={side_of(bound, 'nullables', side)}"
                f" value={side_of(bound, 'value_nullables', side)} extent={side_of(bound, 'extent_nullables', side)}"
                f" holds={sorted(_short(h) for h in holders)} host={side in (a.get('host_nodes') or set())}"
            )
        return result

    jr.get_join_type = traced


def _trace_optimizer() -> None:
    """`--opt`: print every optimizer rule that changes a join type."""
    from trilogy.core import optimization as opt

    def joins_of(ctes):
        out = {}
        for c in ctes:
            for j in c.joins or []:
                left = j.left_cte.name if getattr(j, "left_cte", None) else "?"
                right = j.right_cte.name if getattr(j, "right_cte", None) else "?"
                out[(c.name, left, right)] = getattr(j, "jointype", None) or getattr(
                    j, "join_type", None
                )
        return out

    original_make = opt.OptimizationRulePlan.make_rule

    def make_rule(self):
        rule = original_make(self)
        orig_opt = rule.optimize
        phase = self.name

        def traced(cte, inverse_map):
            ctes = [cte]
            before = joins_of(ctes)
            result = orig_opt(cte, inverse_map)
            after = joins_of(ctes)
            for k in set(before) | set(after):
                if before.get(k) != after.get(k):
                    print(f"  OPT {phase}: {k} {before.get(k)} -> {after.get(k)}")
            return result

        rule.optimize = traced
        return rule

    opt.OptimizationRulePlan.make_rule = make_rule


def _trace_holders() -> None:
    """`--holders`: per merge, each side's region spans and the extent-free set."""
    from trilogy.core.processing.nodes import merge_node as mn

    original = mn.get_node_joins

    def traced(datasources, environment, *args, **kwargs):
        efs = kwargs.get("extent_free_spans", frozenset())
        print(
            f"  MERGE extent_free={sorted(efs)} host_grain={sorted(kwargs.get('host_grain') or [])}"
        )
        for ds in datasources:
            spans = sorted(getattr(ds, "region_spans", None) or [])
            outs = sorted(_short(c.address) for c in ds.output_concepts)
            print(f"    {_short(ds.identifier)} spans={spans} id={id(ds)} outs={outs}")
        return original(datasources, environment, *args, **kwargs)

    mn.get_node_joins = traced


def _trace_content_preservation() -> None:
    """`--ecp`: the join list before and after `ensure_content_preservation`."""
    original = jr.ensure_content_preservation

    def fmt(j):
        return f"{j.type.name}[{','.join(sorted(_short(x) for x in j.lefts))} -> {_short(j.right)} on {sorted(_short(k) for k in set().union(set(), *j.keys.values()))}]"

    def traced(joins, *args, **kwargs):
        before = [fmt(j) for j in joins]
        original(joins, *args, **kwargs)
        after = [fmt(j) for j in joins]
        if before != after:
            print("  ECP before:")
            for b in before:
                print("     ", b)
            print("  ECP after:")
            for a in after:
                print("     ", a)

    jr.ensure_content_preservation = traced


def _trace_downgrade() -> None:
    """`--dg`: every join `_downgrade` narrows, with the right CTE's partial
    marks, the blocked set and the source map of each pair address."""
    from trilogy.core.optimizations import join_upgrade as ju

    original = ju._downgrade

    def traced(cte, idx, join, proofs):
        result = original(cte, idx, join, proofs)
        if result is not None:
            right = join.right_cte
            print(
                f"  DOWNGRADE {cte.name}: {join.jointype.name} -> {result.name} right={right.name}"
            )
            print(
                f"    right partial={sorted(_short(a) for a in ju.partial_addresses(right))}"
            )
            print(f"    right ds={sorted(ju._source_datasources(right))}")
            for p in join.joinkey_pairs or []:
                addr = p.right.address
                print(
                    f"    pair {_short(addr)}: source_map={sorted(cte.source_map.get(addr, ()))}"
                    f" cte_keys={(right.name, addr) in proofs.cte_keys}"
                    f" proves={proofs.proves_cte_key(right, addr)}"
                )
        return result

    ju._downgrade = traced


if __name__ == "__main__":
    forked = "--model" in sys.argv and sys.argv[sys.argv.index("--model") + 1] == "F"
    if forked:
        model = (
            p.FORKED_MATERIALIZED if "--materialized" in sys.argv else p.FORKED_DERIVED
        )
    else:
        model = p.MATERIALIZED if "--materialized" in sys.argv else p.DERIVED
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(_Filter())
    handler.setFormatter(logging.Formatter("%(message)s"))
    log = logging.getLogger("trilogy")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    if "--joins" in sys.argv:
        _trace_joins()
    if "--opt" in sys.argv:
        _trace_optimizer()
    if "--holders" in sys.argv:
        _trace_holders()
    if "--nofd" in sys.argv:
        from trilogy.core.processing.v4_helper import group_graph as gg

        gg._grain_determines = lambda *a, **k: False
    if "--caps" in sys.argv:
        from trilogy.core.processing.v4_helper import group_graph as gg

        orig_compute = gg._compute_concept_sets
        orig_for = gg.GroupIOPlan.for_groups
        plans: list = []

        def for_groups(*a, **k):
            plan = orig_for(*a, **k)
            plans.append(plan)
            return plan

        gg.GroupIOPlan.for_groups = for_groups

        def compute(*a, **k):
            result = orig_compute(*a, **k)
            plan = plans[-1]
            for gid in sorted(plan.capability):
                print(f"  CAP {gid}: {sorted(_short(x) for x in plan.capability[gid])}")
                print(
                    f"  OUT {gid}: {sorted(_short(x) for x in plan.outputs.get(gid, ()))}"
                )
            return result

        gg._compute_concept_sets = compute
    if "--ecp" in sys.argv:
        _trace_content_preservation()
    if "--dg" in sys.argv:
        _trace_downgrade()
    env = Environment()
    env.parse(model)
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    args = iter(sys.argv[1:])
    queries = []
    for x in args:
        if x == "--model":
            next(args)
        elif not x.startswith("--"):
            queries.append(x)
    for q in queries:
        print(f"\n=== {q}")
        try:
            print("  rows:", p._rows(ex, q))
            if "--sql" in sys.argv:
                print(ex.generate_sql(q + ";")[-1])
        except Exception as e:
            print("  ERROR", type(e).__name__, str(e)[:300])
