"""Where do a plan's merge join keys come from?

Compiles every SELECT in the globbed .preql files with the planner
instrumented, and reports per statement: how many sides the FINAL merge had,
which stage-2 (group graph) and stage-3 (assembly) join-key paths fired, which
keys resolve-time join discovery (`get_node_joins`) actually used, and whether a
declarative pairing rule over stage-2 facts alone would have predicted them.

    python local_scripts/sql_ab/join_census.py <out.json> "tests/modeling/**/*.preql" ...

Run from the root of the tree under test (cwd goes first on sys.path). The
JSON holds the per-statement records; the summary prints to stdout. See
`docs/join_axis_audit_2026_10.md` for what the numbers mean and the rule the
prediction uses.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, os.getcwd())

from trilogy import Environment
from trilogy.core.processing import join_resolution as jr
from trilogy.core.processing.nodes import merge_node as mn
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper import strategy_builder as sb
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import (
    MultiSelectStatement,
    SelectStatement,
)
from trilogy.dialect.enums import Dialects

ALL_ROWS = "__preql_internal.all_rows"

REC: dict = {}
STATE = {"in_final": False, "in_carry": False, "in_bridge": False}
FINAL_NODES: set[int] = set()
CURRENT_MERGE: list = []


def bump(key: str, n: int = 1) -> None:
    REC[key] = REC.get(key, 0) + n


@contextmanager
def flag(name: str):
    prev = STATE[name]
    STATE[name] = True
    try:
        yield
    finally:
        STATE[name] = prev


def where() -> str:
    return "final" if STATE["in_final"] else "group"


def wrap(mod, name: str, fn) -> None:
    orig = getattr(mod, name)

    def inner(*a, **k):
        return fn(orig, *a, **k)

    inner.__name__ = name
    setattr(mod, name, inner)


def nonempty(key: str):
    def f(orig, *a, **k):
        r = orig(*a, **k)
        if r:
            bump(key)
        return r

    return f


# ---- stage 2: group graph ---------------------------------------------------
def p_final_contract(
    orig, group_graph, attrs, mandatory_list, environment, *rest, **kw
):
    r = orig(group_graph, attrs, mandatory_list, environment, *rest, **kw)
    return r


def p_regraft_candidate(orig, *a, **k):
    r = orig(*a, **k)
    if r is not None:
        bump(
            "s2.regraft_candidate" + ("_partial" if k.get("allow_partial") else "_full")
        )
    return r


# ---- stage 3: assembly --------------------------------------------------------
def p_assemble_final(orig, *a, **k):
    with flag("in_final"):
        return orig(*a, **k)


def p_cover(
    orig, group_graph, group_edges, attrs, built, mandatory_list, environment, ownership
):
    r = orig(
        group_graph, group_edges, attrs, built, mandatory_list, environment, ownership
    )
    REC["final.cover_contributors"] = len(r)
    REC["final.cover"] = {
        gid: {
            "derivation": str(attrs[gid].derivation),
            "grain": sorted(attrs[gid].grain_components),
            "emitted_grain": sorted(attrs[gid].emitted_grain),
            "outputs": sorted(attrs[gid].output_concepts),
            "inputs": sorted(attrs[gid].input_concepts),
            "members": sorted(attrs[gid].members),
            "anchor_keys": sorted(attrs[gid].anchor_keys),
            "extent_spans": sorted(attrs[gid].extent_spans),
            "built_outputs": sorted(c.address for c in built[gid].output_concepts),
        }
        for gid in r
        if gid in attrs
    }
    return r


def p_plan(orig, *a, **k):
    plan = orig(*a, **k)
    REC["final.plan_keys"] = sorted(plan.join_keys)
    REC["final.plan_pairings"] = [
        [p.left, p.right, sorted(p.keys), p.kind.value] for p in plan.pairings
    ]
    REC["final.plan_carry"] = {g: sorted(v) for g, v in plan.carry_demands.items()}
    return plan


def p_fresh_root(orig, *a, **k):
    r = orig(*a, **k)
    if r is not None and STATE["in_final"]:
        bump("final.root_resourced_fresh")
    return r


def p_wrap_for_grain(orig, parent_node, *a, **k):
    r = orig(parent_node, *a, **k)
    if any(n is not parent_node for n in r):
        bump(f"{where()}.wrap_for_grain_grouped")
    return r


def p_satisfy(orig, parents, *a, **k):
    r = orig(parents, *a, **k)
    if len(r) != len(parents) or any(x is not y for x, y in zip(parents, r)):
        bump(f"{where()}.satisfy_projection_contract_changed")
    return r


def p_bridge(orig, parents, *a, **k):
    with flag("in_bridge"):
        r = orig(parents, *a, **k)
    if len(r) > len(parents):
        bump(f"{where()}.bridge_added_parent", len(r) - len(parents))
    if any(x is not y for x, y in zip(parents, r)):
        bump(f"{where()}.bridge_deduped_member")
    return r


def p_fold_covered(orig, parents, *a, **k):
    r = orig(parents, *a, **k)
    if len(r) < len(parents):
        bump("final.fold_covered_contributors", len(parents) - len(r))
    return r


def p_carry(orig, *a, **k):
    with flag("in_carry"):
        return orig(*a, **k)


def p_widen(orig, *a, **k):
    r = orig(*a, **k)
    if STATE["in_carry"]:
        bump(f"{where()}.widen_join_key_onto_parent")
    elif STATE["in_bridge"]:
        bump(f"{where()}.widen_member_to_bridge_key")
    return r


def p_pairing_islands(orig, outputs):
    r = orig(outputs)
    if len(r) > 1:
        bump(f"{where()}.islands_seen")
    return r


# ---- resolve: where the keys are really chosen --------------------------------
_orig_merge_init = mn.MergeNode.__init__


def p_merge_init(self, *a, **k):
    _orig_merge_init(self, *a, **k)
    if STATE["in_final"] and k.get("host_stitch"):
        FINAL_NODES.add(id(self))


_orig_generate = mn.MergeNode.generate_joins


def p_generate(self, *a, **k):
    CURRENT_MERGE.append(self)
    try:
        return _orig_generate(self, *a, **k)
    finally:
        CURRENT_MERGE.pop()


def p_get_node_joins(orig, datasources, environment, *a, **k):
    joins = orig(datasources, environment, *a, **k)
    node = CURRENT_MERGE[-1] if CURRENT_MERGE else None
    is_final = node is not None and id(node) in FINAL_NODES
    keys: set[str] = set()
    keyless = 0
    for j in joins:
        if not hasattr(j, "pairs"):
            continue
        if not j.pairs and not j.concepts:
            keyless += 1
        for p in j.pairs:
            keys.add(p.left.address)
            keys.add(p.right.address)
        for c in j.concepts or []:
            keys.add(c.address)
    if not is_final:
        bump("group.merges_resolved")
        if keyless:
            bump("group.keyless_joins", keyless)
        return joins
    REC["final.merge_sides"] = len(datasources)
    REC["final.join_keys"] = sorted(keys)
    REC["final.keyless_joins"] = keyless
    everything = set(keys) | set(REC.get("final.plan_keys") or [])
    for info in (REC.get("final.cover") or {}).values():
        for field, values in info.items():
            if field != "derivation":
                everything |= set(values)
    REC["final.canon"] = environment.address_roots(everything)
    return joins


def p_keyless_guard(orig, *a, **k):
    try:
        return orig(*a, **k)
    except Exception:
        bump("resolve.keyless_guard_raised")
        raise


def install() -> None:
    mn.MergeNode.__init__ = p_merge_init  # type: ignore[method-assign]
    mn.MergeNode.generate_joins = p_generate  # type: ignore[method-assign]
    wrap(gg, "_refresh_final_contract", p_final_contract)
    wrap(gg, "_regraft_candidate", p_regraft_candidate)
    wrap(
        gg,
        "_synthetic_dimension_regraft_parent",
        nonempty("s2.synthetic_dimension_regraft"),
    )
    wrap(
        gg,
        "_terminal_basic_grouping_provider",
        nonempty("s2.terminal_basic_grouping_provider"),
    )
    wrap(gg, "_shared_row_parent_join_keys", nonempty("s2.shared_row_parent_join_keys"))
    wrap(sb, "_assemble_final_node", p_assemble_final)
    wrap(sb, "_cover_groups_for_mandatory", p_cover)
    wrap(sb, "plan_final_join", p_plan)
    wrap(sb, "_fresh_final_root_projection", p_fresh_root)
    wrap(sb, "_wrap_for_grain", p_wrap_for_grain)
    wrap(sb, "_satisfy_parent_projection_contract", p_satisfy)
    wrap(sb, "_bridge_unpaired_parents", p_bridge)
    wrap(sb, "_fold_covered_contributors", p_fold_covered)
    wrap(sb, "_carry_join_keys", p_carry)
    wrap(sb, "widen_projection", p_widen)
    wrap(sb, "_pairing_islands", p_pairing_islands)
    wrap(mn, "get_node_joins", p_get_node_joins)
    wrap(jr, "_raise_if_keyless_row_bearing_join", p_keyless_guard)


# ---- the declarative rule under test -------------------------------------------
def canon(canon_map: dict[str, str], addrs) -> frozenset[str]:
    return frozenset(canon_map.get(a, a) for a in addrs if a != ALL_ROWS)


def predicted_keys(record: dict) -> frozenset[str]:
    """The keys the planner's own FINAL join plan pairs its cover on."""
    return canon(record["final.canon"], record.get("final.plan_keys") or [])


# ---- driver ----------------------------------------------------------------------
def compile_file(path: Path, out: dict) -> None:
    renderer = Dialects.DUCK_DB.default_renderer()
    key = path.as_posix()
    try:
        env, statements = Environment(working_path=path.parent).parse(
            path.read_text(encoding="utf-8")
        )
    except Exception as e:
        out[key] = {"error": f"PARSE {type(e).__name__}"}
        return
    selects = [
        s for s in statements if isinstance(s, (SelectStatement, MultiSelectStatement))
    ]
    for idx, statement in enumerate(selects):
        REC.clear()
        FINAL_NODES.clear()
        try:
            processed = process_query(
                env,
                statement,
                having_alias=renderer.SUPPORTS_ALIAS_IN_HAVING,
                supports_full_join=renderer.SUPPORTS_FULL_JOIN,
            )
            renderer.compile_statement(processed)
        except Exception as e:
            REC["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        out[f"{key}#{idx}"] = dict(REC)


def summarize(out: dict) -> None:
    multi = {k: v for k, v in out.items() if v.get("final.merge_sides", 0) >= 2}
    errors = sum(1 for v in out.values() if "error" in v)
    print(f"{len(out)} statements, {errors} errors")
    print(f"FINAL merges with >=2 sides: {len(multi)}")
    counts: Counter = Counter()
    for v in out.values():
        for k, n in v.items():
            if isinstance(n, int) and "." in k and k not in ("final.merge_sides",):
                counts[k] += 1
    print("\nstatements where each path fired:")
    for k, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4d}  {k}")
    grew = [
        k
        for k, v in multi.items()
        if v.get("final.cover_contributors") != v["final.merge_sides"]
    ]
    print(
        f"\nFINAL merges whose side count differs from the elected cover: {len(grew)}"
    )
    for k in grew:
        print(
            f"     {k}: cover={multi[k].get('final.cover_contributors')} sides={multi[k]['final.merge_sides']}"
        )

    verdicts: Counter = Counter()
    misses: list = []
    for k, v in multi.items():
        if not v.get("final.cover"):
            continue
        actual = canon(v["final.canon"], v["final.join_keys"])
        predicted = predicted_keys(v)
        if predicted == actual:
            verdict = "EXACT"
        elif predicted < actual:
            verdict = "UNDER"
        elif predicted > actual:
            verdict = "OVER"
        else:
            verdict = "DIFFERENT"
        verdicts[verdict] += 1
        if verdict != "EXACT":
            misses.append((verdict, k, sorted(actual), sorted(predicted)))
    total = sum(verdicts.values())
    print(f"\nstage-2 pairing rule vs resolve-time keys over {total} FINAL merges:")
    for verdict in ("EXACT", "UNDER", "OVER", "DIFFERENT"):
        print(f"  {verdicts[verdict]:4d}  {verdict}")
    for verdict, k, actual, predicted in sorted(misses):
        print(f"  {verdict:9s} {k}")
        print(f"      actual    : {actual}")
        print(f"      predicted : {predicted}")


def main() -> None:
    install()
    out_path = Path(sys.argv[1])
    files = sorted({f for g in sys.argv[2:] for f in glob.glob(g, recursive=True)})
    start = time.time()
    out: dict = {}
    for f in files:
        compile_file(Path(f), out)
    out_path.write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(f"{time.time() - start:.0f}s -> {out_path}")
    summarize(out)


if __name__ == "__main__":
    main()
