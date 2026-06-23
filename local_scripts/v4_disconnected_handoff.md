# Handoff: v4 disconnected-component cross-join semantics — COMPLETE (13/13)

**Date:** 2026-06-22  **Branch:** v4_syntax_continue
**Status:** ALL 13 disconnected tests pass under v4 (and still under v3). The
`_DISCONNECTED` block is removed from `tests/v4_known_failing.py`.

## Background

When a query's required concepts (outputs + filter args) cannot be welded into
one connected query, discovery must raise `DisconnectedConceptsException`
(subclass of `ValueError`) naming the disconnected subgraphs and asking "Are you
missing a join or merge?". It carries `.subgraphs` — a list of address groups,
partitioned by graph reachability. Two ungrouped (single-row) aggregates are NOT
a disconnection — they cross-join freely.

- Typed exception + helpers: `trilogy/core/exceptions.py::DisconnectedConceptsException`,
  `trilogy/core/processing/discovery_utility.py::{disconnected_components,
  raise_if_disconnected, format_disconnected_subgraphs_error,
  format_unresolved_concepts_error}`.
- Tests: `tests/core/processing/test_disconnected_components_e2e.py` (×13 raise +
  controls) and `tests/core/processing/test_disconnected_subgraphs.py` (×3).

## The three final fixes (2026-06-22)

### 1. `test_where_filter_pulls_in_disconnected_model` — should RAISE
`select av where bv > 0` over unrelated models silently cross-joined (`ON 1=1`).
v4 `search_concepts_v4` returned a node, so the dead-end was never hit.
**Fix:** `query_processor._get_query_node_v4` now calls a new up-front
`_raise_if_disconnected(outputs + WHERE row args)` BEFORE planning (it delegates
to the shared `discovery_utility.raise_if_disconnected`). Crossjoinable
(single-row/constant) concepts are skipped, so valid cross-joins still resolve.
The control `select av, bv inner join a_id = b_id` stays connected (the scoped
join collapses the keys into one component) and resolves.

### 2. `test_abstract_aggregates_cross_join_resolve` — should RESOLVE
`select sum(av) as sa, sum(bv) as sb` crashed with `IndexError` (an
empty-`datasources` QDS). Root cause: `group_rules.partition_roots`' output-
projection co-sourcing rule unioned the two roots `av`/`bv` (both reach an output
address) into ONE `grp:root:root:∅` root group; that group is disconnected and
cannot be sourced as a single scan, so its build returned None and BOTH aggregate
GroupNodes ended up parentless → degenerate QDS.
**Fix (at source):** gate the output-projection union on concept-graph weak
connectivity — only co-source output-converging roots that lie in the SAME
weakly-connected component. Disconnected roots split into separate scans that the
FINAL node cross-joins (`FULL JOIN ... ON 1=1`). Preserves q04 (attributes share
the customer key → connected) and same-model multi-aggregate scans (connected via
the shared key). The `calculate_effective_parent_grain` IndexError was a symptom,
NOT patched (no belt-and-suspenders).

### 3. `test_cross_cte_aggregate_grain_only_bridge_raises` — should RAISE (nested)
`combined` groups `by a_agg.a_id` but sums `b_agg.sb` — the grain-only `by` edge
must not bridge the unrelated a/b models. The disconnection is INSIDE building the
`combined` CTE (a rowset), a nested resolution level the top-level pre-check
doesn't cover.
**Fix:** `concept_strategies_v4.resolve_rowset` checks the inner select's
connectivity before its recursive `search_concepts`. The check set is built by a
new `_rowset_inner_check_concepts` helper that mirrors v3's dead-end working set:
the group grain keys + each aggregate measure at a DIFFERENT grain (a same-grain
aggregate regroups freely and is elided, so its measure column doesn't spuriously
appear in the subgraph error). Yields the v3-identical partition
`[{a_agg.a_id}, {b_agg.b_id, b_agg.sb}]`.

## Verification (2026-06-22)
- `test_disconnected_components_e2e.py` + `test_disconnected_subgraphs.py`:
  20 passed (with `--runxfail`).
- Full v4 sweep (`TRILOGY_V4_DISCOVERY=1`, `-m "not adventureworks_execution"`):
  4119 passed / 0 failed (82 errors = clickhouse.cloud env; 40 xpass = known
  cross-file pollution, conftest non-strict).
- Full v3 sweep: 4162 passed / 0 failed. v3 untouched.
- `ruff` / `mypy` / `black`: clean.

## Touched files
- `trilogy/core/query_processor.py` (`_raise_if_disconnected` + up-front call)
- `trilogy/core/processing/discovery_utility.py` (`raise_if_disconnected` helper)
- `trilogy/core/processing/concept_strategies_v4.py`
  (`_rowset_inner_check_concepts` + `resolve_rowset` check)
- `trilogy/core/processing/v4_helper/group_rules.py`
  (`partition_roots` connectivity-gated output union)
- `tests/v4_known_failing.py` (removed the `_DISCONNECTED` block + constant)
