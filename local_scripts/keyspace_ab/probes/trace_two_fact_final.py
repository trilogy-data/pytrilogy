"""Trace the FINAL assembly of argv queries on probe_two_fact_join's DERIVED
model: the cover, the domain contributors, the sole-contributor feeder pairing,
every condition injection, the dedup, and the assembled node's marks."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import probe_two_fact_join as p

from trilogy.core.processing.v4_helper import strategy_builder as sb


def _s(addr: str) -> str:
    return addr.replace("local.", "")


def _node(n) -> str:
    if n is None:
        return "None"
    outs = ",".join(sorted(_s(c.address) for c in n.output_concepts))
    partial = ",".join(sorted(_s(c.address) for c in n.partial_concepts or []))
    spans = ",".join(sorted(getattr(n, "region_spans", None) or []))
    ds = getattr(n, "datasource", None)
    tag = f" ds={ds.identifier}" if ds is not None else ""
    return f"{type(n).__name__}[{outs}] partial=[{partial}] spans=[{spans}]{tag}"


def _tree(n, depth=0) -> None:
    print("      " + "  " * depth + _node(n))
    for parent in n.parents or []:
        _tree(parent, depth + 1)


def _wrap(name, show_args=(), show_result=True):
    original = getattr(sb, name)

    def traced(*args, **kwargs):
        result = original(*args, **kwargs)
        print(f"  {name}")
        for i in show_args:
            a = args[i] if i < len(args) else None
            if hasattr(a, "output_concepts"):
                print(f"    arg{i}: {_node(a)}")
            elif isinstance(a, list) and a and hasattr(a[0], "output_concepts"):
                for x in a:
                    print(f"    arg{i}: {_node(x)}")
            elif isinstance(a, dict):
                for k, v in a.items():
                    print(f"    arg{i}: {_s(k)} -> {v}")
            else:
                print(f"    arg{i}: {a}")
        if show_result:
            if hasattr(result, "output_concepts"):
                print(f"    -> {_node(result)}")
            elif isinstance(result, tuple) and result and isinstance(result[0], list):
                print(
                    f"    -> nodes {[_node(x) for x in result[0]]}, {[_s(c.address) for c in result[1]]}"
                )
            elif isinstance(result, dict):
                for k, v in result.items():
                    print(
                        f"    -> {_s(k)}: {[_s(c.address) for c in v] if isinstance(v, (list, set)) else v}"
                    )
            else:
                print(f"    -> {result}")
        return result

    setattr(sb, name, traced)


_wrap("_cover_groups_for_mandatory")
_wrap("_add_region_domain_contributors", show_args=(3,), show_result=False)
_wrap("_filter_arg_parents", show_args=(2,))
_wrap("_push_row_condition_before_group", show_args=(0,))
_wrap("_subtree_applies_conditions", show_args=(0,))
_wrap("_group_to_grain_if_required", show_args=(0,))
_wrap("inject_condition_at_node", show_args=(0,))
_wrap("region_reads", show_args=(0,))
if "--agg" in sys.argv:
    _wrap("_pre_merge_parents", show_args=(0,))
    _wrap("_project_basic_aggregate_inputs", show_args=(0, 1, 2))

if "--buckets" in sys.argv:
    # the group graph as planned, and every group build with its extent scope
    import trilogy.core.processing.v4_node_generators as v4g
    from trilogy.core.processing.v4_helper import group_graph as gg
    from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID

    _orig_place = gg.plan_condition_placements

    def _traced_place(
        group_graph, group_edges, buckets, conditions, mandatory_list, *rest
    ):
        print("  buckets:")
        for gid, b in buckets.items():
            print(
                f"    {_s(gid)}: {b.derivation.name} grain={sorted(_s(x) for x in b.grain_components)}"
                f" primary={[_s(m) for m in b.primary_members]} secondary={[_s(m) for m in b.secondary_members]}"
                f" extent={sorted(b.extent_spans) if b.extent_spans else None}"
            )
        print("  edges:")
        for u, v in group_graph.edges:
            print(
                f"    {_s(u)} -> {_s(v) if v != FINAL_NODE_ID else 'FINAL'} [{group_edges.get((u, v))}]"
            )
        return _orig_place(
            group_graph, group_edges, buckets, conditions, mandatory_list, *rest
        )

    gg.plan_condition_placements = _traced_place
    _orig_build = v4g.build_node

    def _traced_build(**kw):
        env = kw["environment"]
        print(
            f"  build_node {kw['derivation'].name} outputs={[_s(c.address) for c in kw['outputs']]}"
            f" parents={[_node(p) for p in kw['parents']]}"
            f" extent_free={sorted(_s(x) for x in env.span_scope.extent_free)}"
        )
        out = _orig_build(**kw)
        print(f"    -> {_node(out)}")
        return out

    v4g.build_node = _traced_build

_orig_assemble = sb._assemble_final_node


def _assemble(*args, **kwargs):
    result = _orig_assemble(*args, **kwargs)
    print("  _assemble_final_node ->")
    if result is not None:
        _tree(result)
    return result


sb._assemble_final_node = _assemble

if "--select" in sys.argv:
    # every legacy scan request, with its promotion and the planner frames above it
    import traceback

    from trilogy.core.processing.nodes import History

    _orig_gsn = History.gen_select_node

    def _gsn(self, concepts, environment, g, depth, **kw):
        frames = [
            f"{f.name}:{f.lineno}"
            for f in traceback.extract_stack()[-14:-1]
            if "strategy_builder" in f.filename or "source_planning" in f.filename
        ]
        out = _orig_gsn(self, concepts, environment, g, depth, **kw)
        print(
            f"  gen_select_node {[_s(c.address) for c in concepts]} accept_partial={kw.get('accept_partial')}"
            f" extent_free={sorted(_s(x) for x in environment.span_scope.extent_free)}\n    via {frames}\n    -> {_node(out)}"
        )
        return out

    History.gen_select_node = _gsn

if "--search" in sys.argv:
    from trilogy.core.processing.v4_helper import network_build as nb
    from trilogy.core.processing.v4_helper import source_planning as sp

    _orig_cand = nb._candidate_for

    def _cand(
        node,
        datasource,
        node_emitted,
        conditions,
        equivalence,
        owners,
        rolled=frozenset(),
        promoted=frozenset(),
    ):
        c = _orig_cand(
            node,
            datasource,
            node_emitted,
            conditions,
            equivalence,
            owners,
            rolled,
            promoted,
        )
        if c is not None:
            print(
                f"    candidate {node}: bindings={{{', '.join(f'{_s(a)}:{b.name if hasattr(b, 'name') else b}' for a, b in sorted(c.bindings.items()))}}}"
                f" promoted={sorted(_s(x) for x in promoted)}"
            )
        return c

    nb._candidate_for = _cand
    _orig_plan = sp.plan_source

    def _plan(request):
        print(
            f"  plan_source outputs={[_s(c.address) for c in request.outputs]} extent_free={sorted(request.environment.span_scope.extent_free)} owned={sorted(request.environment.span_scope.owned)}"
        )
        result = _orig_plan(request)
        print(
            f"    -> {_node(result)}"
            + (
                f" ds={result.datasource.identifier}"
                if result is not None
                and getattr(result, "datasource", None) is not None
                else ""
            )
        )
        return result

    sp.plan_source = _plan

model = p.DERIVED
if "--rollup" in sys.argv:
    # the rollup-summary twin of tests/engine/test_unmodelled_regions.py
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "t",
        str(
            Path(__file__).resolve().parents[3]
            / "tests"
            / "engine"
            / "test_unmodelled_regions.py"
        ),
    )
    t = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(t)
    model = t.ROLLUP_SUMMARY
if "--optional" in sys.argv:
    # the optional-entity model of tests/engine/test_derived_key_domain.py
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "tdk",
        str(
            Path(__file__).resolve().parents[3]
            / "tests"
            / "engine"
            / "test_derived_key_domain.py"
        ),
    )
    tdk = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tdk)
    model = tdk._OPTIONAL_MATERIALIZED if "--mat" in sys.argv else tdk._OPTIONAL_DERIVED
if "--forked" in sys.argv:
    # the two-family forked model of tests/engine/test_duckdb_partial_key_assembly.py
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "pka",
        str(
            Path(__file__).resolve().parents[3]
            / "tests"
            / "engine"
            / "test_duckdb_partial_key_assembly.py"
        ),
    )
    pka = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pka)
    model = pka._FORKED
ex = p._ex(model)
for q in sys.argv[1:]:
    if q.startswith("--"):
        continue
    print(f"\n##### {q}")
    try:
        print("  rows:", p._rows(ex, q))
    except Exception as e:
        print("  ERROR", type(e).__name__, str(e)[:160])
