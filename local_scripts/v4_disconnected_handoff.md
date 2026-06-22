# Handoff: v4 disconnected-component cross-join semantics (3 remaining)

**Date:** 2026-06-22  **Branch:** v4_syntax_continue
**Status:** 10/13 disconnected tests fixed; 3 remain, tracked in
`tests/v4_known_failing.py` under reason `_DISCONNECTED`.

## Background

When a query's required concepts (outputs + filter args) cannot be welded into
one connected query, discovery must raise `DisconnectedConceptsException`
(subclass of `ValueError`) naming the disconnected subgraphs and asking "Are you
missing a join or merge?". It carries `.subgraphs` — a list of address groups,
partitioned by graph reachability.

- Typed exception + helpers: `trilogy/core/exceptions.py::DisconnectedConceptsException`,
  `trilogy/core/processing/discovery_utility.py::{disconnected_components,
  format_disconnected_subgraphs_error, format_unresolved_concepts_error}`.
- v3 raises it from `discovery_utility.get_priority_concept` (~line 521-534) and
  `concept_strategies_v3.py:645`.
- Tests (all added in #584, all pass under v3):
  `tests/core/processing/test_disconnected_components_e2e.py` (×10 raise + control
  resolves) and `tests/core/processing/test_disconnected_subgraphs.py` (×3).

## What was already fixed (10/13, 2026-06-22)

`trilogy/core/query_processor.py::_get_query_node_v4`: at the dead-end
(`info.strategy_node is None`), instead of always raising the generic
`UnresolvableQueryException`, it now computes
`disconnected_components(build_environment, outputs + WHERE row_args, graph)` and
raises the typed `DisconnectedConceptsException(format_..., subgraphs=...)` when
the required concepts split into >1 subgraph. This handles every case where v4
*already* dead-ended to `None`.

This is the right fix shape for the simple cases. The 3 below are NOT
exception-typing problems — v4 takes a different control-flow path (resolves or
crashes) and never reaches the dead-end, so they need deeper planner work.

## The 3 remaining (each is a distinct v4 semantics gap)

### 1. `test_where_filter_pulls_in_disconnected_model` — should RAISE, v4 cross-joins

Repro (model A `a_id/av`, unrelated model B `b_id/bv`):
```trilogy
select av where bv > 0;
```
Expected: raise, subgraphs `[[local.av], [local.bv]]`.
v4 actual: silently builds `... RIGHT OUTER JOIN "highfalutin" ON 1=1 WHERE bv > 0`
— a cross-join of two unrelated models via the WHERE arg. `search_concepts_v4`
returns a (wrong) node, so the dead-end is never hit.

Root cause hypothesis: v4 treats the WHERE-only concept `bv` as freely
cross-joinable against `av` instead of recognizing they are in different
reference-graph components with no join/merge path. The `ON 1=1` join is the
tell. Fix likely belongs where v4 decides a row-stream parent can be attached
with no shared key (the cross-join / merge-grain logic), or a pre-check that the
output+filter concepts are connected before planning. Compare against the
*control* test `test_scoped_join_bridged_models_resolve`
(`select av, bv inner join a_id = b_id;`) which MUST still resolve — so the gate
is "no join/merge/FK path", not "different models".

### 2. `test_abstract_aggregates_cross_join_resolve` — should RESOLVE, v4 crashes

Repro:
```trilogy
select sum(av) as sa, sum(bv) as sb;
```
Expected: resolve (two ungrouped/single-row aggregates cross-join freely; this is
NOT a disconnection — `_crossjoinable` skips single-row concepts).
v4 actual: `IndexError: list index out of range` at
`discovery_utility.py:71` in `calculate_effective_parent_grain`
(`qds.datasources[0].grain` on a QueryDatasource with empty `datasources`). So v4
builds a degenerate QDS (no datasources) for the two-abstract-aggregate cross
join.

Root cause hypothesis: v4 fails to source/cross-join two independent scalar
aggregates into one node, producing an empty-datasource QDS. This is a
*resolution* bug (the opposite of #1 — here a cross-join is correct but v4 can't
build it). Note: v3 resolves this. Start at whatever v4 node-builder yields the
empty-`datasources` QueryDatasource for grouped-to-single-row aggregates.

### 3. `test_cross_cte_aggregate_grain_only_bridge_raises` — should RAISE (nested)

Repro (a_agg over model A, b_agg over model B, then a `combined` CTE that groups
`by a_agg.a_id` but sums `b_agg.sb`):
```trilogy
with a_agg as select a_id, sum(av) as sa;
with b_agg as select b_id, sum(bv) as sb;
with combined as select a_agg.a_id as id, sum(a_agg.sa) as ta, sum(b_agg.sb) as tb;
select combined.id, combined.ta, combined.tb;
```
Expected: raise, subgraphs containing `{a_agg.a_id}` and `{b_agg.b_id, b_agg.sb}`
(the grain-only `by` edge must NOT bridge the unrelated a/b models — see
`disconnected_components`' `_aggregate_grain_only_parents` edge-dropping).
v4 actual: does not raise at the top-level `_get_query_node_v4` dead-end — the
disconnection is inside building the `combined` CTE, a nested resolution level
whose concepts are `a_agg.*`/`b_agg.*`, not the top-level `combined.*`. The fix
in #fixed only covers the outermost dead-end.

Root cause hypothesis: the typed-exception surfacing needs to also fire at the
nested dead-end where `combined` is resolved (likely a recursive
`_get_query_node_v4` / `search_concepts_v4` call, or inside rowset/CTE building).
Thread the same `disconnected_components` check to that level with the
*nested* concept set.

## How to work / verify

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  tests/core/processing/test_disconnected_components_e2e.py \
  tests/core/processing/test_disconnected_subgraphs.py \
  --runxfail -q -p no:cacheprovider -o addopts=
```
For an isolated repro, set `CONFIG.use_v4_discovery = True` BEFORE importing the
planner (the env var only flips the flag via conftest; a standalone script must
set CONFIG directly — see `tests/conftest.py`).

As each is fixed, remove its entry from `tests/v4_known_failing.py` (the
`_DISCONNECTED` block) and re-run a full v4 sweep
(`-m "not adventureworks_execution"`) to confirm 0 regressions. v3 must stay
untouched (all 20 disconnected tests pass under v3 today).

## Pointers
- v4 entrypoint + the landed fix: `trilogy/core/query_processor.py::_get_query_node_v4`
- partition logic: `trilogy/core/processing/discovery_utility.py::disconnected_components`
  (drops aggregate grain-only `by` edges; skips crossjoinable/single-row)
- the membership-in-having sibling fix (same session, same file) is unrelated but
  shows the v3-mirroring pattern: see
  `evals/tpcds_agent/bug_invalid_reference_codegen_having_membership.md`.
