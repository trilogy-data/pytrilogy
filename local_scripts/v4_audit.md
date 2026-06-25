# v4 compatibility audit (last refreshed 2026-06-22, phase-boundary hardening)

This file is the current handoff for v4 discovery work. The authoritative skip list is
`tests/v4_known_failing.py`; reclassify with `python local_scripts/v4_classify.py`
when changing planner behavior.

## Current tracked state

`tests/v4_known_failing.py` currently tracks 46 non-strict xfail entries:

| bucket | n | meaning |
| --- | ---: | --- |
| INLINE | 11 | SQL/CTE shape differs from v3, rows expected to match |
| RESULT | 1 | known wrong-row regression (test_ninety_seven_two; now xpasses) |
| MODELING | 13 | modeling sweep diffs still needing per-test classification |
| TPCDS_SIZE | 18 | TPC-DS rows match, SQL exceeds v3-tuned size ceilings |
| DISCONNECTED | 3 | v4 cross-join semantics gaps; handoff in `v4_disconnected_handoff.md` |

The four "high-priority unclassified" entries from the prior audit
(`test_composite_rollup_aggregate_keeps_group_by`, both geography
`test_exact_match_resolution` / `test_exact_match_with_parenthetical_extra_filter`,
and `test_sixty_eight`) are now fixed and removed from the list. `test_top_x_by_metric`
is fixed and promoted to `v4_evals/cases/` (see below). Full v4 suite (no
adventureworks, clickhouse ignored), after the three 2026-06-21 fixes below:
**4022 passed, 4 xfailed, 40 xpassed, 0 failed** — but that figure predates #584
and is now stale; see "Correctness parity — corrected" below for an untracked
`membership in having` over-union regression that turns the gate red.

Two of those fixes were genuine wrong-rows bugs the prior "no remaining
wrong-rows" line had missed: `test_select_literal_is_rendered_in_projection`
(mis-bucketed INLINE; the classifier's "ROWS" tag was right) and the untracked,
hash-seed-flaky `test_window_over_rollup_preserves_grouping_rows`. The third was
fallout from the (then-uncommitted) `top_x` bridge fix. See the section above.

### Latest classifier run (2026-06-22, isolation, `--runxfail`)

`python local_scripts/v4_classify.py` over the 43 entries, after the classifier
heuristic was hardened (see below): **37 SHAPE / 5 XPASS / 1 OTHER**. The two
prior false positives are gone — the old run's 2 "ROWS" were SQL-shape asserts
(now SHAPE) and the 2 "CRASH" were a parallel-worker file-lock on the shared
`zquery_timing_*` sidecar (now re-run serially in a second pass). The lone
**OTHER** is `test_having_nested`: a per-row value predicate
(`store_order_count > 1000`) the classifier can't cleanly call SHAPE or ROWS.
In isolation it fails under v4 (got 534) but passes under v3; at file level it
xpasses, so it is cross-test-pollution-dependent, not a full-suite regression.

#### Classifier heuristic fix (2026-06-22)

`v4_classify.py` had two bugs that produced the false positives above:

1. **Parallel `zquery_timing` collision mislabeled CRASH.** The benchmark
   suites read-modify-write a shared `zquery_timing_{fingerprint}.log` per test
   (temp-file + atomic `replace()`); the 8-worker pool races the rename →
   Windows `PermissionError`. Fix: detect `(Permission|FileExists)Error … zquery_timing`,
   flag the node, and re-run those nodes **serially** after the pool drains.
2. **ROWS/SHAPE split scanned the wrong text.** It searched full stderr for
   space-anchored SQL tokens (`select `, ` from `), which miss real
   newline/truncated query text, so SQL-text asserts fell through to ROWS. Fix:
   switch to `--tb=short` and classify on the `E ` assertion-operand lines with
   word-boundary SQL tokens; SQL token ⇒ SHAPE, else row signal (count/`len(`/row
   mismatch/tuple-eq) ⇒ ROWS, else OTHER. NB: dropped `where`/`and` from the SQL
   token set — pytest's own assertion-rewrite explanation lines (` + where 534 = …`)
   use those words and were false-matching SQL (this is what had masked
   `test_having_nested` as SHAPE).

### Correctness parity — corrected (fresh full v4 sweep 2026-06-22)

The prior "v4 is at correctness parity" line was based solely on the
`v4_known_failing.py` skip list, and that list is **stale**: the classifier only
audits tests already on it, so it is blind to regressions in tests added after
the list was last curated. A fresh full v4 sweep
(`TRILOGY_V4_DISCOVERY=1 pytest -m "not adventureworks_execution"`) reports
**4105 passed / 5 xfailed / 40 xpassed / 13 failed** (+82 clickhouse.cloud
connection errors, environmental — ignore). The 13 are untracked, all added in
#584, all pass under v3:

- **`membership in having` over UNION ALL** — 3 tests
  (`test_membership_in_having_{over_window,auto_concept,no_projected_flag}_renders_valid_subselect`).
  **FIXED 2026-06-22** (v4-only). v4 never sourced the existence subselect for
  a HAVING membership, nor for a *projected* membership flag (`--x in set as
  flag`), so both rendered `INVALID_REFERENCE_BUG`. Two changes mirroring v3:
  (1) `_get_query_node_v4` now calls `append_existence_check` after applying the
  HAVING; (2) `strategy_builder._group_existence_concepts` now also collects the
  existence set from a `BuildConceptArgs` (SubselectComparison) lineage, not just
  `BuildFilterItem`. Full sweep confirms 0 regressions. See the eval doc
  `evals/tpcds_agent/bug_invalid_reference_codegen_having_membership.md`.
- **disconnected-component detection** — 13 tests
  (`test_disconnected_components_e2e.py` ×10 + `test_disconnected_subgraphs.py`
  ×3). Pre-existing (fail with the membership fix stashed too — not a
  regression). **10/13 FIXED 2026-06-22**: `_get_query_node_v4`'s dead-end
  (`info.strategy_node is None`) now mirrors the v3 `get_priority_concept`
  dead-end — it runs `disconnected_components` over (outputs + WHERE row args)
  and raises the typed `DisconnectedConceptsException(msg, subgraphs=…)` when
  the required concepts split into >1 subgraph, instead of a generic
  `UnresolvableQueryException`. The remaining **3 are deeper v4 cross-join
  semantics gaps**, not exception-typing — v4 resolves/crashes where it should
  raise (and vice-versa) instead of dead-ending to `None`:
    - `test_where_filter_pulls_in_disconnected_model` — v4 silently builds a
      `RIGHT OUTER JOIN … ON 1=1` cross-join for `select av where bv > 0` over
      unrelated models, so the dead-end is never reached. Should raise.
    - `test_abstract_aggregates_cross_join_resolve` — should *resolve* (two
      single-row `sum`s cross-join), but v4 crashes with `IndexError` in
      `calculate_effective_parent_grain` (`qds.datasources[0]`, empty datasources).
    - `test_cross_cte_aggregate_grain_only_bridge_raises` — nested dead-end
      (inside building the `combined` CTE), deeper than the top-level None site;
      expects inner-namespace subgraphs (`a_agg.a_id`, `b_agg.sb`).

After the membership fix + the 10/13 disconnected fix, those 3 deeper
cross-join-semantics cases are now **tracked** in `v4_known_failing.py`
(`_DISCONNECTED`), so the v4 gate is green again. They are the only known open v4
correctness work; full diagnosis + repros + fix pointers are in
`local_scripts/v4_disconnected_handoff.md`. Parity is **confirmed modulo those 3
tracked cases** (re-run a full v4 sweep after the next planner change to catch
any newly-added untracked tests — the classifier alone cannot).

XPASS-in-isolation, promote candidates (re-confirm in a full-suite run before
removing — the list is non-strict precisely because of cross-test state leakage):

- `tests/modeling/test_complex.py::test_in_select`
- `tests/modeling/geography/test_landmark_updates.py::test_exact_match_merge_preserves_subgraph_filters`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_nine`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_one`
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two`

### Result-regression (`_RESULT`) entries

- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ninety_seven_two` — **now
  passes** (xpasses in isolation and in the full suite); rows correct. Last
  `_RESULT` entry; safe to remove on the next cleanup pass.
- `tests/stdlib/test_report.py::test_top_x_by_metric` — **FIXED** (2026-06-21).
  Promoted to `local_scripts/v4_evals/cases/top_x_by_metric.preql`; removed from
  the known-failing list. Root cause + fix below.

#### `test_top_x_by_metric`: parent merge dropped the bridge join key (FIXED)

`@top_x_by_metric(order, sum(amount), 1, -1)` expands to
`CASE WHEN rank(order) over (order by sum(amount) desc) < 2 THEN order ELSE -1 END`
(a BASIC concept, grain `order`, over a WINDOW over a per-order AGGREGATE). The
outer `sum(amount) by top_orders` aggregate merges two row parents — the fact scan
(`amount`, carries `order`) and the window-derived `top_orders` dimension (carries
`order`). They must join on the shared `order` key; v4 instead cross-joined
(`FULL JOIN ON 1=1`), so the top bucket summed the global 10.0 instead of order 3's
own 6.0.

Two-part fix, decision in **group planning** (the input contract):

1. `group_graph.py` — `_shared_row_parent_join_keys` + `_refresh_input_contracts`:
   when a joining consumer (AGGREGATE / WINDOW / GROUP_TO / BASIC) has ≥2 row-stream
   parents that share a KEY-purpose output, that key is declared in the
   `GroupInputContract.preserve_keys` as the bridge join key. (Parallels the
   7de267f6 ROWSET/`_final_merge_grain` work, for a non-grouping row parent.)
2. `strategy_builder.py` — the materializer honors the contract by pulling the
   *extra* bridge keys (those not already in the group's grain/outputs) into
   `needed` before sourcing parents, so the root slice keeps `order`. The
   "extra-only" guard is essential: adding a key already in the grain re-forces a
   `by rollup` grouping key into the SELECT outside its GROUP BY (regressed
   `aligned-multi-select` before the guard was added).

v4 now emits `... INNER JOIN quizzical ON cheerful.order = quizzical.order`
(matching v3's shape). Both changes are v4-only (`v4_helper/`); v3 untouched.
Locked by `tests/core/processing/test_v4_group_behaviors.py::test_input_contract_declares_shared_row_parent_join_key`
and the promoted parity case.

## Phase boundary contract

The intended v4 path is:

1. Source concept demand: `concept_graph.py` walks mandatory output concepts and
   condition inputs back to root concepts, preserving row-vs-existence dataflow
   with typed edges. This phase should not pick concrete datasources or build
   `StrategyNode`s.
2. Group concepts: `group_graph.py` applies per-derivation grouping rules,
   injects conditions at groups, computes group IO, and appends the FINAL sink.
   This phase should decide which concepts can be sourced together, but should
   not render or materialize query nodes.
3. Materialize groups: `strategy_builder.py` walks the group DAG and dispatches
   each group to `v4_node_generators`. ROOT groups call `source_planning.py` for
   datasource selection, bridge planning, partial completion, and pinned rollup
   sourcing. This phase satisfies declared contracts; it should not infer
   projection grain, join keys, or final contributor contracts from physical
   sibling shape.
4. Zip final query: `_assemble_final_node` merges the minimum built contributors
   that cover the mandatory outputs, applies final-only filters, hides join keys
   that were carried only for assembly, and dedups to the requested output grain.
   Optimization happens after this planner returns a normal strategy node.

## Separation audit notes

The boundary is now enforced in code rather than only described here:

- `FinalAssemblyContract` and `FinalContributorContract` are Stage 2 outputs.
  Stage 3 requires them and no longer refreshes or synthesizes missing final
  contracts.
- `GroupInputContract` is the only source for parent projection grain and
  per-group bridge join keys. `_satisfy_parent_projection_contract` physically
  satisfies that grain, but no longer falls back to group grain or shaped sibling
  outputs.
- `_widen_merge_join_keys` only widens for declared join key addresses. It no
  longer scans sibling outputs for "key-ish" concepts.
- FINAL rowset joins declare the row-stream lineage key (for example
  `local.order_id`) in Stage 2, rather than relying on Stage 3 to infer it from
  nullable rowset aliases.
- `_fold_passthrough_parents` is pinned as a physical redundancy optimization:
  it can absorb row-preserving projections, but cannot dissolve row-shape
  barriers.
- Sole-contributor FINAL hiding is pinned as output hygiene: it hides
  non-mandatory carried grain keys only at the FINAL layer.
- `_group_to_grain_if_required` is pinned to `FinalAssemblyContract`; it may
  skip or perform physical deduping, but does not choose the logical output
  grain.

Still-watch areas:

- `source_planning.py` is the right home for concrete datasource selection. Its
  bridge and partial-completion helpers are sourcing concerns, not grouping
  concerns.
- `_regraft_group_sources` in `group_graph.py` is topology-only: it may add a
  better existing parent edge or a synthetic dimension ROOT bucket, but it must
  not call `plan_source`, inspect datasources, or build `StrategyNode`s.
- Existence edges must stay side-channel-only. They order subselect sources but
  should not be treated as row-stream JOIN parents.
- Rowset, recursive, aggregate, window, and group-to concepts remain row-shape
  barriers. Fixes should preserve them as materialized boundaries rather than
  decomposing their roots into another phase.

## Next cleanup loop

Among *tracked* entries the rest of the list is SHAPE/SIZE plus pollution-flaky
xpasses. The open correctness work is the **untracked** `membership in having`
over-union cluster (see "Correctness parity — corrected"); fix or track it, then
re-run a full v4 sweep to confirm nothing else regressed since #584 (the
classifier only re-checks tests already on the skip list, so a fresh full-suite
run is the only way to find new untracked gaps).

1. Remove the 5 XPASS candidates above (confirmed xpassing in isolation; the
   geography `test_exact_match_merge_preserves_subgraph_filters` is the newest):
   `test_in_select`, `test_exact_match_merge_preserves_subgraph_filters`,
   `test_sixty_nine`, `test_ninety_seven_one`, `test_ninety_seven_two`.
   Re-confirm in a full-suite run first. Removing `test_ninety_seven_two`
   empties the `_RESULT` bucket entirely.
2. Split the 13 `_MODELING` entries into `_INLINE` or `_TPCDS_SIZE` (all are
   SHAPE per the classifier — no rows diffs left).
3. Re-run `local_scripts/v4_classify.py` after any planner change.
4. Keep new Stage 3 heuristics behind contract-driven tests: if materialization
   needs a key or projection grain, Stage 2 should declare it first.
