# v4 compatibility audit (last refreshed 2026-06-12)

Fresh re-classification of every entry in `tests/v4_known_failing.py`, run in
isolation under `TRILOGY_V4_DISCOVERY=1 --runxfail`. Regenerate with
`python local_scripts/v4_classify.py` (writes `v4_classify.json`).

Supersedes the Jun-5 sweep snapshot (reasons/results/triage/SUMMARY + the
root-cause-A / fanout / agg-source briefs), all deleted — those bugs are fixed
and the data was stale.

## Headline (2026-06-12 re-run, post-C3+C6-fix)

52 tracked entries (down from 59 — gcat test_join + gcat2 test_extra_fields_two +
ncaa adhoc02 + C5 array_inclusion_aggregate + C3 test_history fixed/removed; gcat
test_join_discovery_two now XPASSes and was removed):

| bucket | n | meaning |
| --- | --- | --- |
| **SHAPE** | 42 | correct rows, worse/different SQL — cosmetic, not a parity bug |
| **CRASH** | 0  | no genuine crashes remain |
| **ROWS**  | 8  | classifier coarse bucket; ~4 genuine wrong-rows + 2 render + 2 SHAPE-ish |
| **TIMEOUT** | 2 | tpc-ds q21/q22 exceed the classify budget |

Counts are the raw `v4_classify.py` buckets. The `ROWS` bucket is coarse: only
C8 (×2) + C9 (×2) are genuine wrong-rows, C7 (×2) are the date-render misses, and
two (`test_select_literal_is_rendered_in_projection`, `test_aggregate_filter_uses_having`)
are really SHAPE/`_INLINE` that the regex caught. **No genuine crashes remain**
(C3 + C6, the last two, fixed below).

Note: under the *parallel* classify run the file-lock flake can re-surface
`tpc_ds_duckdb::test_seventy_three` as a spurious `PermissionError [WinError 32]`
CRASH — in isolation it's `assert 5489 < 3000` (SHAPE/size), not a real crash.

So ~6 of 52 are genuine parity gaps (~4 genuine wrong-rows + 2 render); the rest
are SQL-shape/verbosity (TPC-DS size ceilings + inlining CTE-shape snapshots).

### FIXED 2026-06-12 — C3 complete-where exact-match filter-column over-requirement
`discovery/test_discovery.py::test_history_e2e_non_materialized_field`
(`UnresolvableQueryException: no complete sources found for {'total_customer_revenue'}`).
Query `where name='Sarah' select customer_id, total_customer_revenue`; the model
has `partial datasource customer_revenue_for_sarah … complete where name='Sarah'`
binding only customer_id/revenue (no `name`). `_plan_complete_where_source`
(source_planning.py) required *every* filter column to be a source output so a
residual predicate could be applied — but here the query condition is *equivalent*
to `non_partial_for`, so there is no residual and `name` need not be bound. Fix:
compute `residual_free = condition_implies(non_partial_for, conditions)` (the
query condition is already implied to be implied the other way) and skip the
filter-column requirement when residual_free. The matched scan then renders with
`partial_is_full=True` (no WHERE), exactly the expected single-table SELECT.
Passes identically under v3 and v4; removed from v4_known_failing. This was the
last genuine crash → **0 crashes**.

### FIXED 2026-06-12 — C6 union-member complete-where crash (tpc_ds q23)
`tpc_ds_duckdb/test_queries.py::test_twenty_three` (`KeyError: 'ds~store_sales_unified'`).
`_plan_complete_where_source` (source_planning.py) iterates `environment.datasources`
and matched `store_sales_unified` — a *member* of a union datasource whose
`non_partial_for` the query's WHERE implies. But a union member has no standalone
node in the concept graph (the graph only carries the union node), so
`create_select_node_candidate`'s `g.datasources[f"ds~{ds.name}"]` KeyErrored. Fix:
the match loop now skips any datasource not present as a standalone scan in
`request.graph.datasources`, leaving union members to the union planner. Crash →
correct rows (now only the pre-existing TPC-DS size-ceiling miss, `9135 < 8500`,
so it stays xfailed under `_TPCDS_SIZE`). Regression sweep
(engine/filter + gcat + persistence + materialized-roots under v4): 88 passed,
6 xfailed, 0 regressions. v4-only path.

### FIXED 2026-06-12 — C5 agg-derivation crash (group-graph existence cycle)
`test_duckdb_filter.py::test_array_inclusion_aggregate`
(`UnresolvableQueryException: Could not resolve connections for … ord_count`).
The comp query `where orid_2 in even_orders select count(orid_2)` has two
independent unnests (`orid`, `orid_2`) that v4 merges into ONE unnest group node.
The top-level existence WHERE creates concept edge `even_orders → orid_2` → lifted
to group edge `filter(even_orders) → unnest` (EXISTENCE), while lineage already
gives `unnest → filter` (even_orders descends from orid in that same unnest) — a
2-cycle, so `_topological_dependency_order` returned None and the strategy build
was abandoned (`group-graph cycle, abandoning strategy build`). An EXISTENCE edge
ONLY orders the subselect source before its consumer; when the source group is a
LINEAGE-descendant of the consumer group, that ordering is already implied and the
edge is a pure cycle-forming back-edge. Fix (`group_graph.py`, after the
concept→group edge lift): drop any EXISTENCE group edge `gu→gv` where `gv` is a
LINEAGE-ancestor of `gu` (`nx.has_path(lineage_sub, gv, gu)`). Same
existence-vs-row principle as the q08 / window_clone fixes; v4-only path. Sweep
(engine+tpc_ds+tpc_h+usa_names+gcat under v4): 524 passed, 8 pre-existing baseline
fails, 0 regressions (test_recursive + composite_rollup + 6 tpc_ds all fail on a
clean `git stash` baseline too). Removed from v4_known_failing.

### FIXED 2026-06-11 — C2 window_clone source-map crash
`test_complex.py::test_window_clone` (`SyntaxError: Missing source map entry for
local.nums`). Two-part fix in the v4 merge-join-key widener:
1. `parent_output_addresses` (projection.py) now excludes **hidden** parent
   outputs — a hidden output is dropped from that parent's CTE SELECT, so it's
   not readable by a consumer and must not count as "available".
2. `_widen_merge_join_keys` (strategy_builder.py) now skips carrying any sibling
   concept that is in the receiving parent's `existence_concepts` — an existence
   concept is consumed via a subselect (`WHERE x in (select …)`), never joined
   as a row column, so carrying it as a widenable output produced a dangling
   un-joined CTE reference (`Referenced table "highfalutin" not found`).
NOT the right axis: gating on `concept.derivation` (WINDOW/UNNEST/…) over-broadly
broke `test_window_over_rollup_preserves_grouping_rows`, which legitimately
carries a WINDOW result. The discriminator is existence-vs-row, not derivation.

### FIXED 2026-06-11 — C3 merge-key partial output (gcat test_join)
`UnresolvableQueryException: no complete sources found for output concepts
{'vehicle.name'}`. `vehicle.name` is bound in `launch_info` only as the
`~vehicle.name` merge key (partial column), so the v4 bridge carried it partially
and the final-output guard (`query_processor.py:532`) rejected it. v3 *also*
picks `launch_info` from the graph search but its outer sourcing loop then
**completes** the partial key against its dimension home (`lv_info`) and joins.
Mirrored that in v4: new `_complete_partial_requested` (source_planning.py) runs
on a strict (non-partial) bridge attempt -- finds requested outputs still bound
partially, sources them with `STRICT_SOURCE_POLICY`, and anchors the complete
(and filter-carrying) dimension via an outer-join merge (v3's
`lv_info LEFT JOIN launch_info` shape). Guards: only when a *complete* source
exists (else unchanged -> genuinely-partial stays for the partial passes); the
WHERE is carried to the completion only when all its columns are among the keys
being completed (so the unfiltered dimension can't re-introduce excluded keys);
and a `SourceRequest.complete_partials=False` flag on the completion sub-call
prevents infinite re-entry when a concept has no complete source (caught a
`test_filter_sector_two` RecursionError regression). Removed from
v4_known_failing. NOTE: the *other* C3 crash (`test_history_e2e_non_materialized_field`)
is a different mechanism (`partial ... complete where customer_id=2` datasource
under a `name='Sarah'` filter -- needs implies-reasoning) and is still open.

### FIXED 2026-06-12 — C4 derived-condition-arg not sourced (gcat2 test_extra_fields_two)
`ValueError: Invalid reference string` at render. Query: `where date_part(launch_date,
year)=2010 select vehicle.stage.engine.fuel`. `launch_date <- launch_jd` is a
`launch_info` column; launch_info is joined only for the `vehicle.name` key, so the
WHERE's derived row-arg `launch_date` decomposes to `launch_jd` which the
launch_info scan never carried → final WHERE rendered `INVALID_REFERENCE_BUG` for
`launch_jd`. Root cause: the v4 bridge's `filter_downstream` Steiner pass drops a
BASIC condition arg (`launch_date`), so its ROOT lineage (`launch_jd`) was never
added to the launch_info scan. Fix: `_search_concepts_for_bridge`
(source_planning.py) now also includes `_condition_arg_lineage_roots` — the ROOT
`.sources` of any *derived* condition row-arg. Narrowly scoped: raw/ROOT condition
args have `lineage is None` and are unaffected. NOTE: a dead-end first attempt
(letting BASIC groups host a WHERE on their own output via `_reachable_input`)
*dropped* the filter+joins entirely — the condition-placement layer was a red
herring; the bug is purely in bridge sourcing. Sweeps: filter/condition suites
301 passed/0 failed; tpc-ds/tpc-h/modeling sweep's 8 fails all pre-existing on a
clean baseline (`git stash`). Removed from v4_known_failing.

## Genuine gaps, clustered by root cause

### C1 — merge / rowset fan-out (wrong rows) — FIXED (was 4 → 0)
~~Big row blowups; v4's merge/multiselect dedup misses on the demo models.~~
Re-verified 2026-06-12: the whole cluster passes under v4. None of these were
ever in `v4_known_failing.py`, so the classifier never re-checked them and this
section was carried over stale from a pre-fix snapshot. The fix landed earlier
(see memory `project_v4_merge_into_key_fanout.md` + the `test_demo_e2e`
persist-of-unnest fix in `project_v4_persist_unnest_reuse.md`).
- ~~`engine/demo/test_demo_duckdb.py::test_merge`~~ passes
- ~~`engine/demo/test_demo_duckdb.py::test_merge_basic`~~ passes
- ~~`engine/demo/test_demo_duckdb_import.py::test_demo_merge_rowset_e2e`~~ passes
- ~~`persistence/test_complex_persistence.py::test_complex`~~ passes
- ~~`engine/demo/test_demo_duckdb.py::test_demo_e2e`~~ passes
Full `demo` + `persistence` suites: 26 passed / 0 failed under v4.

### C2 — recursive-CTE pseudonym source-map crash — FIXED (was 4 → 0)
`SyntaxError: Missing source map entry for root_parent.id with pseudonyms
{'local.recursive_parent'}` at render — the recursive-CTE alias path.

FIXED & removed: `test_window_clone` (existence-concept carried as a row output,
see headline), `test_refresh` (gcat, fixed earlier), and the
`merge recurse_edge(...) into root_parent.id` cluster (hackernews adhoc02/03).

The recurse-merge fix was NOT the brief's concept-graph substitution (red herring)
— the bridge/`_derived_connector_nodes` machinery already routes `root_parent.id`
through its recursive origin. Two bugs in `source_planning.py` (v4-only):
1. `_local_concept_nodes_for_datasource` attached the non-BASIC merge key to its
   INPUT scan (reachable via reverse-lineage) → missing-source-map crash. Now
   skipped unless the datasource binds it as a column (new
   `_concept_has_non_basic_merge_origin` + `_datasource_can_output`).
2. `_derived_connector_nodes` dropped the connector's grain/JOIN key when "covered"
   → `FULL JOIN ... on 1=1` when the recursion's `id` is also the post scan's `id`.
   Now always carries grain keys (the join column).
adhoc02/03 assert WITH RECURSIVE + `"1=1" not in`; both removed from v4_known_failing.

### C3 — partial-source / namespaced-property-without-key crash — FIXED (was 3 → 0)
`UnresolvableQueryException: … could only be resolved from partial sources` /
`NoDatasourceException`. A namespaced property is bound but its key has no
datasource. `cases/namespaced_property_without_key_datasource.preql` exists and
passes, so the *base* shape is fixed.
- ~~`modeling/gcat/test_gcat.py::test_join` (vehicle.name)~~ FIXED — merge-key
  partial-output completion (see FIXED note above).
- ~~`discovery/test_discovery.py::test_history_e2e_non_materialized_field`~~ FIXED.
  The audit's prior "needs Sarah⇒id=2 implies-reasoning" framing was WRONG: the
  query `where name='Sarah'` is served directly by the `customer_revenue_for_sarah`
  source declared `complete where name='Sarah'` — an *exact* condition match, no
  id-inference needed. The bug was in `_plan_complete_where_source`: it demanded
  every filter column (`name`) be an output of the source, but that source only
  binds customer_id/revenue. Fix: skip the filter-column requirement when
  `non_partial_for` *also* implies the query condition (the two are equivalent), so
  there is no residual WHERE to apply — the scan is already pre-filtered to exactly
  the requested rows (`create_datasource_node` sets `partial_is_full=True` and emits
  no WHERE, so `name` is never referenced). Removed from v4_known_failing (now
  passes identically under v3 and v4). See FIXED note below.

### UNTRACKED pre-existing v4 baseline failures (found in 2026-06-11 sweep)
Not in `v4_known_failing.py`, but fail on a clean baseline too (verified via
`git stash`) — so they are real v4 gaps the list is missing, NOT regressions:
- `tpc_ds_duckdb/test_queries.py`: test_twenty_nine (ValueError), test_forty_six,
  test_fifty_four, test_sixty_eight, test_seventy_seven, test_ninety_seven_two
- `engine/test_duckdb.py::test_composite_rollup_aggregate_keeps_group_by`
  (the NULL-CASE-over-rollup-key dim baseline fail noted earlier)
- `engine/test_duckdb.py::test_recursive` (`assert 2 == 4` — wrong rows; verified
  pre-existing on clean baseline, NOT a regression of the C5 cycle fix)
- `modeling/geography/test_landmark_updates.py::test_exact_match_resolution`,
  `…::test_exact_match_with_parenthetical_extra_filter`
These should be added to the tracking list or fixed.

### C4 — "Invalid reference string" crash (ValueError) — 1 (was 2)
- ~~`modeling/gcat/gcat2/test_gcat_two.py::test_extra_fields_two`~~ FIXED —
  derived-condition-arg lineage not sourced through the bridge (see FIXED note above).
- `modeling/ncaa/test_ncaa.py::test_adhoc02`

### C5 — aggregate-derivation unresolvable crash — FIXED (was 1 → 0)
- ~~`engine/test_duckdb_filter.py::test_array_inclusion_aggregate`~~ FIXED —
  group-graph existence/lineage cycle (two merged unnests + top-level existence
  WHERE). See FIXED note in the headline section.

### C6 — KeyError `ds~store_sales_unified` crash — FIXED (was 1 → 0)
- ~~`modeling/tpc_ds_duckdb/test_queries.py::test_twenty_three`~~ FIXED —
  `_plan_complete_where_source` matched a union *member* with no standalone graph
  node. Now skips datasources absent from `request.graph.datasources`. See FIXED
  note above. Remaining miss is the TPC-DS size ceiling (SHAPE), not a crash.

### C7 — date arithmetic dropped from render — 2
Interval subtraction (`DATE_ADD(..., INTERVAL -30 day)` / `datetime(...)`) missing.
- `engine/test_bigquery.py::test_date_diff_rendering`
- `engine/test_sqlite.py::test_date_diff_rendering`

### C8 — ambiguous forced-join off-by-one rows — 2
- `modeling/join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join` (4 vs 3)
- `…::test_ambiguous_error_with_forced_join_order` (6 vs 5)

### C9 — misc wrong rows — 2
- `modeling/rides_example/test_ride_example.py::test_example_model` (1 vs 4)
- `modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate` (0 vs 2)

## Coverage-gap conclusion

Only `top_x_by_metric` is distilled in `v4_evals/failing_cases/`. **All crash
clusters are now fixed** (C2, C3, C4-gcat, C5, C6) and C1 (merge fan-out) turned
out already-fixed/stale. The remaining genuine gaps are wrong-rows + render only.
Remaining work, in priority order:
**C8/C9 (wrong rows) → C7 (date render) → SHAPE/verbosity backlog.**

The 42 SHAPE entries need no eval case; they're tracked verbosity/inlining
regressions to close at the source-selection / render layer, gated on
`CONFIG.use_v4_discovery` in their SQL assertions.
