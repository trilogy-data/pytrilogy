# Handoff: `gen_rowset` plans its body in a fresh scope, consumes no parents, and stays that way

**Status**: RESOLVED on `refactoring` (PR #674, 2026-09-05 to 2026-09-06).
The mechanical and structural cleanups landed; the structural rewrite (plan a
rowset body as a stacked source with parents, the way PR #673 did for unions)
was answered in writing and declined. See `## Why the body keeps its own
scope` for the decision and `## Permanent carve-outs` for what that commits
the caller to.

**Scope**: `trilogy/core/processing/v4_node_generators/rowset.py`, the two
compensating carve-outs in `v4_helper/strategy_builder.build_strategy_node`,
and the boundary node type.

---

## What landed

- The `v4_node_generators` to `v4_helper` import cycle is gone.
  `strategy_builder` imports `build_node` at call time in its two consumers;
  `v4_helper/__init__` is unchanged. The three-way probe (import
  `v4_node_generators` first, `v4_helper` first, `trilogy` first) passes from
  a cold process in every order.
- `gen_rowset` lost `preexisting_conditions` and `g`; `resolve_rowset` lost
  `g`. `parents` stays for dispatch uniformity, and a non-empty list is logged
  at the boundary's depth (loud, not fatal: the suite has 4 live occurrences).
  `dispatch.build_node` no longer claims ROWSET needs `g`, and the ROWSET,
  SUBSELECT and UNION branches are split. SUBSELECT now receives
  `preexisting_conditions` like every other parent-consuming generator (no
  footprint measured: 10 dispatches, 0 with either condition).
- A real `depth` is threaded `_search_concepts` to `_build_from_graph` to
  `build_strategy_node` to `build_node` to `gen_rowset`, so a nested body's
  trace indents under its parent, and the boundary node itself is stamped
  with it.
- The handle loop iterates the rowset's own `derived_concepts`
  (`_rowset_handles`) instead of the whole environment. Demanded handles still
  win ties and a demanded handle of another rowset is still skipped. One trap
  the corpus render and the probe sweep are blind to:
  `environment.concepts.get(addr)` can return the CANONICAL a scoped-merge
  collapse substituted for the handle (a different address), with the
  authored handle surviving only in `alias_origin_lookup`. The helper offers
  both spellings by own address; `tests/test_scoped_derived_rowset_join_matrix.py`
  pins it (3 cells flipped rows under the naive lookup). Add that file to any
  boundary gate.
- The declared-SUBSET predicate has one home: `DomainGraph.declared_subset_pairs`
  is the single scan, and `subset_sources`, `left_anchor_keys` and
  `declared_subset_anchors` derive from it. The edges are scanned once per
  boundary, not once per handle, and `_anchors_all_rowset` is module level.
- The boundary's outputs accumulate through one `_Boundary` record (handles,
  the produced column backing each, hidden addresses, the address set) with a
  single dedup point, in place of five parallel locals and four rebuilds of
  the address set. The presence-probe recovery and the nullability alias
  mapping are named helpers. The two loops that iterated `set[str]` (grain
  components, relation members) now iterate sorted, so handle order no longer
  depends on the hash seed.
- `RowsetNode` is ADOPTED: the boundary is built as one, and its docstring
  says what it is (a typed 1:1 projection with no behavior of its own).
  `MultiSelectMergeNode` is DELETED with its `__all__` entry and the
  `isinstance` clause in `_subtree_pinned_addresses`, which `git log -S` shows
  was added after the only constructor had already been removed. The
  derivation sniff in `MergeNode._resolve` is left as is: it asks whether a
  MERGE's own outputs are rowset handles, a different question from "is this
  node the boundary", so an `isinstance` swap there is not equivalent.
- The 41 `derivation == Derivation.ROWSET` sites were triaged: all are
  concept-level or group-level (`GroupAttrs.derivation`, bucket derivation,
  output-concept derivation). None asks a built node whether it is a
  boundary. Nothing to retype.
- Five test docstrings that referenced `gen_rowset_node`, a name that no
  longer exists, are fixed.

Gates, all on the current tree against a control copy of the committed module,
rendered twice in one process with a no-op leg:

| gate | result |
| --- | --- |
| `ruff check . --fix` (default rules), `mypy trilogy`, `black .` | clean |
| corpus render, tpc_ds + tpc_ds/aggregates + tpc_h + thelook + gcat | 178 / 178 byte-identical |
| `python -m local_scripts.fuzzer` | all cases agree |
| probe sweep (`## Measurement harness`) | every count unchanged |
| full suite `-m "not adventureworks_execution"` | green except `tests/cli/test_cloud_live.py` (API token lacks `workspaces:read`) |

Probe sweep, before and after the cleanup:

| stat | before | after |
| --- | --- | --- |
| `calls` | 787 | 787 |
| `DISCARDED_PARENTS` | 4 | 4 |
| `DROPPED_OTHER_ROWSET_HANDLE` | 10 | 10 |
| `dropped_output` | 88 | 88 |
| `OUTPUT_ABSENT_FROM_STAMPED_ENV` | 61 | 61 |
| built as `RowsetNode` | 0 | 775 |
| a condition wrapper over a `RowsetNode` | 0 | 12 |

775 + 12 = 787, so every boundary is now typed or sits directly under one.

---

## Why the body keeps its own scope

The question was whether a rowset body can be walked under a scope label in
the concept graph, the way PR #673 walks each union arm under `arm:<identity>`,
so that the boundary consumes parents like every other generator. The answer
is no, and the original docstring's reason was the wrong one.

The docstring said the OUTER build environment classifies the body's concepts
under rowset aliasing, so a plain root reads back as `derivation=rowset`. That
does not reproduce on its own: for
`rowset agg <- select cat, sum(val) -> total, id where val > 1;` the outer
build environment holds `local.cat` as ROOT and `local._agg_total` as
AGGREGATE beside the `agg.*` ROWSET handles. The docstring now names the
reasons that do hold.

What holds is that a rowset body is a **statement**, where a union arm is an
**expression**. Measured over the 789 body plans in the sweep:

| body carries | count |
| --- | --- |
| its own WHERE | 259 |
| its own HAVING | 22 |
| its own LIMIT | 2 |
| its own ORDER BY | 3 |
| a multiselect / union TVF body (own arms, align) | 81 |
| its OWN query-scoped joins | 11 |
| nesting inside another body | 187 |

A union arm has none of these: it is a lineage over the SAME environment's
rows, so a scope label is enough to keep its roots from bucketing with a
sibling arm. A rowset body needs three things a label cannot give it:

1. **Its own environment materialization.** The 11 bodies with their own
   `scoped_joins` build through `nested_select.build_nested_select` against a
   different canonical collapse (the body's join key becomes ONE canonical
   column with the authored side as a pseudonym), with fresh `BuildCaches`
   when the join set differs from the outer's. The concept graph and group
   graph consume one `BuildEnvironment` and one `ReferenceGraph`; the body's
   graph also bridges datasources the outer graph never joins.
2. **Statement-level condition hosting.** WHERE, `then where` stages, HAVING,
   LIMIT with its ORDER BY. Hosting these per body scope in the group graph is
   `search_concepts` re-implemented inside the concept graph, not a smaller
   mechanism.
3. **Recursion.** 187 bodies nest inside another body; each level repeats 1
   and 2.

"Why no parents" and "why a fresh environment" are therefore one decision seen
from both ends: a ROWSET concept is a deliberate leaf in the concept graph
(`concept_graph._add_concept`), the walk never descends into the body, and the
ROWSET bucket has no predecessors. The rare parent (4 of 787 calls) is a
non-handle concept bucketed with the handles (`local.overall_avg_sale`, a
TVF-union arm node) and is logged.

## Permanent carve-outs

Because the boundary re-sources within, `build_strategy_node` compensates in
two places, both commented as permanent:

- A boundary group's outputs are ordered so its OWN handles (primary members)
  come first. `resolve_rowset` plans the rowset of the first handle it sees,
  and a boundary group can carry ANOTHER rowset's handles (a deferred WHERE's
  args exposed through a scoped relation); unordered, a foreign handle hijacks
  the boundary.
- ROWSET is excluded from `satisfiable_outputs` pruning alongside ROOT and
  UNNEST. Pruning a boundary by a constraint-edge sibling's outputs would drop
  any handle the sibling happens not to pseudonym-cover.

Two consequences the probe sweep keeps measuring and which are not defects:

- `DROPPED_OTHER_ROWSET_HANDLE` stays 10. Those are foreign handles a boundary
  group carries for a deferred WHERE arg; the FINAL merge sources them from
  their own boundary. Live in the suite at
  `tests/engine/test_duckdb_rowset.py::test_order_by_measure_through_nested_rowset_join_groups`
  and `tests/engine/test_duckdb_subquery.py::test_tuple_membership_grainless_output`.
- The condition injection at the end of `resolve_rowset` stays: a
  consumer-side predicate over the boundary's rows has no other host.

---

## Latent, checked, did NOT materialize. Do not chase blind.

Instrumented across the sweep and measured at **zero** occurrences.

- **Grain-exposure gate ignores `limit`.** Raw grain-key exposure is gated on
  `where_clause is None and having_clause is None` but not on
  `select.limit is None`, while the partial-marking decision in the same
  function treats a limit as row-narrowing. Measured 2 unfiltered-and-limited
  bodies, 0 of which exposed a raw grain key. The unconditioned subset domain
  edge pinned in `tests/test_rowset_body_limit.py` appears to cover it. Worth
  a deliberate decision and a comment, not a fix under pressure.
- **Outer conditions resolved against the inner scope.** The condition
  injection passes the body's environment and graph to
  `resolve_and_inject_condition` for a condition that came from the OUTER
  group graph. Measured 0 of 12 injected conditions had a row argument absent
  from the inner environment.

### One real but currently harmless mismatch

The boundary node is stamped with the body's environment while its output
concepts are looked up from the OUTER environment. Measured: 61 of 787
boundaries emit an output concept absent from their own stamped environment.
In every observed case it is the presence probe (`local._virt_presence_*`)
that the probe obligation pulls from the outer env. It works because the
renderer resolves the probe inline off its member handle. It is a live trap
for any future code doing `node.environment.concepts[output.address]` on a
boundary, which would `KeyError`. Either stamp the outer environment, register
the probe in the inner one, or add a comment naming the exception.

---

## Cleared false leads

- **`where_clause is None` does correctly cover staged `then where`.**
  `BuildSelectLineage.where_clause` is the AND-fold of `where_clauses` and is
  the canonical full row gate; the plural field is a discovery convenience,
  not a second gate.
- **Mutating the inner node (`add_output_concepts`) is safe.**
  `V4History.get_build_history` returns `node.copy()`, so a cached node is not
  aliased into the mutation.
- **The old whole-environment handle scan was not a perf problem** (20898
  total iterations across the sweep). Its replacement is a readability fix.
- **`ruff` does not flag unused generator parameters** because `ARG` is not
  in `lint.extend-select`. Enabling it repo-wide would false-positive across
  every generator, since `dispatch` requires uniform signatures. Do not enable
  it just for this.

---

## Measurement harness

Every number above is reproducible. The probe is a pytest plugin that wraps
`dispatch._GENERATORS[Derivation.ROWSET]`; it touches no repo code.

Put this on `PYTHONPATH` as `rowset_probe.py` and run with `-p rowset_probe`:

```python
"""pytest plugin: what gen_rowset receives and what it returns."""
import json, os, sys, collections

STATS = collections.Counter()
SAMPLES = collections.defaultdict(list)
PLANS = []


def pytest_configure(config):
    import trilogy.core.processing.v4_helper  # noqa: F401
    from trilogy.core.enums import Derivation
    from trilogy.core.processing.v4_node_generators import dispatch

    original = dispatch._GENERATORS[Derivation.ROWSET]
    rowset_mod = sys.modules["trilogy.core.processing.v4_node_generators.rowset"]
    orig_plan = rowset_mod.plan_nested_select

    def plan_probe(select, history, depth, label, exclude_derived=None,
                   hide_from_connectivity=None):
        r = orig_plan(select, history, depth, label, exclude_derived,
                      hide_from_connectivity)
        PLANS.append(r)
        return r

    rowset_mod.plan_nested_select = plan_probe

    # Signature tracks `gen_rowset`; a stale probe raises TypeError on every
    # dispatch, which reads as a mass test failure rather than a bad probe.
    def probe(outputs, parents, environment, conditions=None, *,
              history, depth=0):
        from trilogy.core.models.build import BuildRowsetItem
        mark = len(PLANS)
        result = original(outputs, parents, environment, conditions,
                          history=history, depth=depth)
        STATS["calls"] += 1
        if parents:
            STATS["DISCARDED_PARENTS"] += 1
        if result is None:
            STATS["returned_None"] += 1
            return result
        STATS["boundaries"] += 1
        env = result.environment
        if any(c.address not in env.concepts
               and c.address not in env.alias_origin_lookup
               for c in result.output_concepts):
            STATS["OUTPUT_ABSENT_FROM_STAMPED_ENV"] += 1
        addrs = [c.address for c in result.output_concepts]
        if len(addrs) != len(set(addrs)):
            STATS["DUPLICATE_OUTPUT_CONCEPTS"] += 1

        plan = next((p for p in PLANS[mark:] if p is not None), None)
        rs = [o for o in outputs if isinstance(o.lineage, BuildRowsetItem)]
        if plan is None or not rs:
            return result
        planned = rs[0].lineage.rowset.name
        produced = set()
        for c in result.output_concepts:
            produced.add(c.address)
            produced.update(c.pseudonyms)
        for o in outputs:
            if o.address in produced:
                continue
            if isinstance(o.lineage, BuildRowsetItem) and o.lineage.rowset.name != planned:
                STATS["DROPPED_OTHER_ROWSET_HANDLE"] += 1
                if len(SAMPLES["other_rowset"]) < 5:
                    SAMPLES["other_rowset"].append(
                        {"planned": planned, "dropped": o.address})
            else:
                STATS["dropped_output"] += 1
        return result

    dispatch._GENERATORS[Derivation.ROWSET] = probe


def pytest_sessionfinish(session, exitstatus):
    with open(os.environ["ROWSET_PROBE_OUT"], "w") as f:
        json.dump({"stats": dict(STATS), "samples": dict(SAMPLES)}, f, indent=2)
```

```bash
export ROWSET_PROBE_OUT=/tmp/probe.json
export PYTHONPATH=/dir/containing/rowset_probe.py
.venv/Scripts/python.exe -m pytest \
  tests/complex tests/engine tests/discovery tests/join_matrix tests/core \
  -p rowset_probe -q -m "not adventureworks_execution"
```

`boundaries == calls` and `returned_None == 0` in every sweep: none of
`resolve_rowset`'s `return None` paths fire anywhere in this corpus. They are
reached only by the direct unit tests in
`tests/core/processing/test_v4_node_generators.py`. Treat the docstring's
claim that "a recursive nested-rowset search can hand a bucket of plain roots
here" as unverified by the corpus.

Narrowing the directory set moves `calls` by a few (761 to 792 observed); the
other counts are stable. Any change here must leave every number unchanged
unless it is the structural rewrite this document declines.

---

## Gates

Per AGENTS.md, for any repo-wide change:

```bash
ruff check . --fix
mypy trilogy
black .
```

Run `ruff check . --fix` with default rules. Do **not** narrow it to
`--select E,F,I`; that hides rules CI enforces.

Planner-relevant gates for any change that touches this file or
`merge_node.py`:

- Full suite: `pytest -m "not adventureworks_execution"`. **Never run two pytest
  processes concurrently**; the modeling suites share duckdb files under
  `tests/modeling/*/` and produce phantom failures.
- Corpus render must cover `tpc_ds` **and** `tpc_ds/aggregates` **and**
  `tpc_h` **and** `gcat` **and** `thelook`. A `tpc_ds`-plus-`tpc_h`-only sweep
  has previously come back identical while turning gcat/thelook CI red, and a
  `query*.preql` glob misses `aggregates/`, which is where planner changes
  bite hardest. Render the aggregates with `working_path` at the
  `tpc_ds_duckdb` root (they import `..store_sales`).
- Run the fuzzer: `python -m local_scripts.fuzzer`. A corpus-identical,
  suite-green planner change has previously shipped a silent cross join.
- Gate against the **current tree**: load the committed module as a control
  (`git show HEAD:...rowset.py`, exec'd with
  `__package__ = "trilogy.core.processing.v4_node_generators"`), swap it into
  `dispatch._GENERATORS[Derivation.ROWSET]`, and render the corpus under both
  in one process with a no-op leg. Checked-in goldens go stale mid-session.
- The `tests/modeling/**/zquery_timing_*.log` and perf PNGs are committed by
  design. Expect diffs, do not revert them, and do not commit a log from an
  interrupted run.

Guardrail suites specific to this area:

- `tests/test_rowset_generation_matrix.py` (exists so a refactor of this
  generator can gate on it; run it around every change here)
- `tests/test_scoped_derived_rowset_join_matrix.py`
- `tests/complex/test_rowset.py`
- `tests/engine/test_duckdb_rowset*.py`
- `tests/engine/test_duckdb_rowset_aggregate_filter_leak.py`
- `tests/test_rowset_body_limit.py`
- `tests/core/processing/test_v4_node_generators.py`
- `tests/core/processing/test_v4_nested_select_parity.py`
