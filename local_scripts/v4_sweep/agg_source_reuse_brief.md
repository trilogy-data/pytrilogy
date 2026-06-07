# P1 root cause — v4 never reads a precomputed/aggregated datasource

> **STATUS (2026-06-07): EXACT-GRAIN + address-match ROLLUP landed.**
> `concept_strategies_v4._materialized_root_addresses` + `concept_graph.materialized_roots`
> treat a demanded AGGREGATE/BASIC concept materialized at the *exact* target grain as a
> ROOT scan; an additive aggregate bound (by address) on a *finer* table is also a ROOT
> scan, and `strategy_builder._group_to_grain_if_required` re-aggregates it with
> `rollup_concepts` (`sum(finer.col)`, count→sum). Both filter-gated by `_conditions_supported`.
> **REMAINING (this brief):**
> 1. **Rollup-WITH-filter** — currently gated off (`where is not None` skips rollup): marking
>    the concept a root lets source-planning pick a coarser exact table that can't express the
>    filter, so the precomputed value would ignore it. Needs the chosen source pinned to the
>    filter-compatible finer table (e.g. a dedicated rollup source node, not generic root-marking).
> 2. **Signature-match rollup** — the finer table binds a *different* concept with the same
>    aggregate signature (`total_val` from `total_val_class`); needs aliasing the source column
>    to the demanded address so the SUM has a column to read (`test_combine_grand_total_*`).
> 3. **Source-preference for non-additive** — `count_distinct`/`avg` over a summary table's grain
>    columns should still scan the summary, not base (`test_partial_aggregate_rollup_rejects_unsupported`).

**Bucket:** the single biggest v4 gap. Covers all `_AGG_SOURCE` (37) and `_PERSIST`
(5) entries in `tests/v4_known_failing.py`, plus TPC-H `adhoc06` (stops using the
precomputed cache table). One fix should clear most of them.

## Evidence (minimal repro)
Model `tests/discovery/aggregate_testing.preql` has a base `orders` table **and**
pre-aggregated summary tables that bind the aggregate concepts directly, e.g.
`customer_summary` (grain `customer_id`) binds `customer_revenue <- sum(order_value) by customer_id`.

```python
# SELECT customer_id, customer_revenue;   (named aggregate concept, no inline agg)
# v3:  reads "customer_summary" directly (no GROUP BY)
# v4:  recomputes  sum("orders"."order_value") ... GROUP BY 1   <-- ignores customer_summary
```
Reproduce:
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest tests/discovery -k aggregate --runxfail
```
It is **pure source-selection** — the bug reproduces with the *named* concept, so
it's independent of inline-`sum(order_value)`→concept matching (both fail).

## Root cause
The v4 planner classifies and dispatches every concept by `concept.derivation`
(its lineage-derived kind):
- `v4_node_generators/dispatch.py::build_node` keys generators off `Derivation`.
- `v4_helper/concept_graph.py::_effective_label` / `classify_depth` only route
  `Derivation.ROOT` concepts to the datasource-scan (ROOT) path; an AGGREGATE
  concept always flows to `gen_aggregate` (a GroupNode over base).

There is **no step that asks "is this AGGREGATE/derived concept already
materialized on a datasource at the required grain?"** — i.e. v4 is missing v3's
precomputed/exact-grain datasource short-circuit. In v3 this lives in
`concept_strategies_v3.search_concepts` + `node_generators/select_node.gen_select_node`:
before decomposing by derivation, v3 tries to satisfy the demanded concepts
directly from a datasource (regardless of their lineage), so a datasource that
*binds* `customer_revenue` at grain `customer_id` wins over re-grouping `orders`.

The graph already has the edges (the env graph links `customer_summary` →
`customer_revenue`); the source-planning in `v4_helper/source_planning.py` can
build the SelectNode. What's missing is letting the **grouping derivations**
consider a direct datasource source before/instead of grouping.

## Suggested fix locus
Add the "concept directly materialized on a datasource at the target grain →
source as ROOT (datasource scan), skip the GroupNode" short-circuit to the v4
search. Two candidate insertion points:
1. **Group-graph classification** (`concept_graph.py` / `group_graph.py`): when a
   concept's address is bound by a datasource whose grain ⊆ target grain, treat
   it as ROOT-sourceable for this request instead of by lineage-derivation.
2. **gen_aggregate / strategy_builder grouping** (`strategy_builder.py` ~455,
   ~834, ~1485): before emitting a GroupNode for the aggregate set, ask
   source-planning whether a single datasource (or partial set) provides those
   aggregate concepts at grain; if so, prefer the datasource SelectNode.

Prefer (1) if it generalizes (it also fixes persisted/cache tables, which are
just datasources binding derived concepts — same mechanism). Mirror v3's
partial/complete + grain-satisfaction logic; respect `accept_partial` so a
datasource covering some but not all demanded aggregates still contributes.

## Affected tests (remove from registry as they pass)
- `tests/discovery/test_aggregates_comprehensive.py::*` (10)
- `tests/discovery/test_aggregate_resolution_coverage.py::*` (incl. `test_exact_grain_match_*`, `test_rollup_*`, `test_partial_key_upgrade_*`) (15)
- `tests/discovery/test_aggregate_handling.py::*` (7)
- `tests/discovery/test_primary_source_aggregate_fallback.py::*` (3)
- `tests/discovery/test_year_alias_resolution.py::test_resolve_via_named_concept_uses_aggregate`
- `_PERSIST`: `tests/persistence/*`, `tests/etl/test_duckdb.py::test_partition_persistence`,
  `tests/engine/test_duckdb.py::test_anon_function_resolves_from_precomputed_source`,
  `tests/modeling/tpc_h/test_tpch_queries.py::test_adhoc06`

## Done when
the discovery group passes under v4 (`TRILOGY_V4_DISCOVERY=1 pytest tests/discovery`),
those entries are removed from `tests/v4_known_failing.py`, and a full v4 sweep
shows no new XPASS/fail. `rollup` / `partial_key_upgrade` variants may need the
follow-on "datasource at a coarser/finer grain than target" handling — land the
exact-grain case first, then the rollup/partial cases.
