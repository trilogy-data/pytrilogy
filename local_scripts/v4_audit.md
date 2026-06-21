# v4 compatibility audit (last refreshed 2026-06-20)

### FIXED 2026-06-20 — q76 crash: aggregate-over-aggregate dimension projection adds a non-output row key to the group grain
`tpc_ds_duckdb/test_queries.py::test_seventy_six` was mis-bucketed `_TPCDS_SIZE`
(SHAPE) but actually **CRASHED** under v4 in isolation (`ValueError: Invalid
input concepts to node! ['ss.date.id'] are missing non-hidden parent nodes`) —
so the headline "no crashes remain" was stale. Distilled q76: an
aggregate-over-aggregate (`sum(ss_row_flag) by date.year/quarter/category`, where
`ss_row_flag <- sum(case .. ss.store.id is null ..) by ticket_number, item.id`)
with a `ss.date.id is not null` WHERE. The C9 bridge path
(`_project_dimension_parents_to_group_grain`, strategy_builder.py) built a
`GroupNode` whose grain = `fd_needed | join_keys`. `fd_needed` is derived from
`outputs_by_parent` = the addresses available to the parent *from its own
parents* (grandparents), NOT the parent's own outputs; `ss.date.id` is
FD-determined by the row grain `{item.id, ticket_number}` (so it passes
`_fd_at_grain`) and is available upstream, but the dimension parent drops it (it
lives only as a scan-level WHERE filter, never a column here). Grouping on a
concept the parent doesn't output fails `GroupNode.validate_inputs`. Fix
(strategy_builder.py): intersect the projected grain with the parent's actual
`usable_outputs` before building the GroupNode — `(fd_needed | join_keys) &
parent_outputs`. q76 now builds + returns correct rows (stays `_TPCDS_SIZE`:
`13369 < 10000` size ceiling). 0 regressions: rides C9 (`test_example_model`),
both C8 `test_ambiguous_error_with_forced_join(_order)`, and core/processing +
optimization + complex + engine (929 passed) all green under v4. Distilled
regression lock: `tests/core/processing/test_v4_dimension_projection_group.py`
(fails with the exact crash when the fix is reverted). **No crashes remain
(re-confirmed).**

### FIXED 2026-06-20 — BASIC-over-GROUP not folded into the GROUP SELECT (extra CTE)
`tpc_ds_duckdb/test_non_benchmark_queries.py::test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate`
(the audit's #1 "genuine wrong-rows" — actually **correct rows, extra CTE**).
`select week_seq, @weekday_sum(0)/@weekday_sum(1)` (a BASIC division over two
aggregates): v4 emitted the GROUP (`cooperative`: week_seq + the two sums) and
then a SEPARATE projection CTE for the division, where v3 inlines
`sum(..)/sum(..)` directly in the GROUP BY select (one CTE). Rows identical;
pure SHAPE/verbosity. Root cause: `CollapseSingleParent.parent_is_ineligible`
blocked a BASIC child from folding into a GROUP parent (blanket `SourceType.GROUP`
/ `group_to_grain` block). Fix (`collapse_single_parent.py`): allow the fold,
gated by a new `basic_fold_into_group_is_safe` — every output the child derives
*locally* must be a scalar; aggregate/window columns merely *passed through* from
the GROUP parent are fine (a dimension-join projection carries the parent's
aggregates straight through). GROUP BY renders solely from `parent.group_concepts`
(base.py), which the BASIC merge path never touches, so the fold can't change the
grouping. DEAD ENDS: (a) a `child.grain == parent.grain` gate — a BASIC node
inflates its own grain with the derived column (`{category, category_label}` vs
`{category}`), so equality never holds; (b) rejecting any aggregate-lineage child
output — those are passthroughs from the group, legitimately present. The fold is
a pure restriction of the un-gated version, so it only ever folds a subset.
Clears 4 known-failing entries (`test_inline_filter_basic`, gcat
`test_equals_comparison`, stocks `test_calculated_field`, `def_wrapped`) and
nudges TPC-DS CTE counts down (q97_1 now under its size ceiling in isolation).
Unit + e2e lock: `tests/optimization/test_collapse_basic_into_group.py`. v3 + v4
opt/complex/engine/core/persistence + modeling sweeps 0 regressions. The two q97
xpasses are pre-existing cross-file pollution (pass in isolation with OR without
this change) — left tracked.

# v4 compatibility audit (previously refreshed 2026-06-13)

Fresh re-classification of every entry in `tests/v4_known_failing.py`, run in
isolation under `TRILOGY_V4_DISCOVERY=1 --runxfail`. Regenerate with
`python local_scripts/v4_classify.py` (writes `v4_classify.json`).

Supersedes the Jun-5 sweep snapshot (reasons/results/triage/SUMMARY + the
root-cause-A / fanout / agg-source briefs), all deleted — those bugs are fixed
and the data was stale.

## Headline (2026-06-13 re-run, post C7 + filter-only-RECURSIVE + C8 fixes)

53 tracked entries. **No crashes, no runaways remain** — the q29 crash (C10), the
q2.1/q2.2 fan-out runaway (C11), and the q54/q77 wrong-rows all landed earlier,
C7 (the "date-render" misses), `test_recursive` (filter-only RECURSIVE),
**C8 (ambiguous forced-join off-by-one, ×2)**, and now **C9 rides
(aggregate-over-aggregate cross-join)** landed this round (see "FIXED" notes +
memory):

| bucket | n | meaning |
| --- | --- | --- |
| **SHAPE** | 47 | correct rows, worse/different SQL — cosmetic, not a parity bug |
| **ROWS**  | 4  | classifier coarse bucket; ~2 genuine wrong-rows + 2 SHAPE-ish |
| **XPASS** | 2  | q97_1/q97_2 — pass in isolation; pre-existing cross-file pollution, left in |

Counts are the raw `v4_classify.py` buckets. The `ROWS` bucket is coarse: only
C9 def_wrapped and
`composite_rollup` are genuine wrong-rows; the two
(`test_select_literal_is_rendered_in_projection`, `test_aggregate_filter_uses_having`)
are really SHAPE/`_INLINE` that the regex caught.

Note: under the *parallel* classify run the buckets read 46 SHAPE / 7 ROWS / 3
XPASS — `modeling/test_complex.py::test_in_select` flips to XPASS from cross-file
pollution but is a genuine SHAPE entry in isolation (hence 47 SHAPE / 2 XPASS
above). The file-lock flake can likewise re-surface
`tpc_ds_duckdb::test_seventy_three` as a spurious `PermissionError [WinError 32]`
CRASH — in isolation it's `assert 5489 < 3000` (SHAPE/size), not a real crash.

So ~2 of 53 are genuine parity gaps (all wrong-rows); the rest are SQL-shape/
verbosity (TPC-DS size ceilings + inlining CTE-shape snapshots).

### FIXED 2026-06-13 — C8 ambiguous forced-join off-by-one (group-property + unrelated key)
`join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join`
(was 4 vs 3) + `::test_ambiguous_error_with_forced_join_order` (was 6 vs 5).
`select store_by_warehouse, product_id` where `store_by_warehouse <- group(store_id)
by wh_id` (a group-property used as a forced-join disambiguator) alongside an
unrelated key. Two stacked bugs in the v4 FINAL-node assembly:
1. **Cross-join (the spurious extra row, e.g. (store2,product1)).** `_wrap_for_grain`
   bucketed the re-sourced ROOT scan's own grain keys (`product_id`/`store_id`/
   `wh_id`) into per-key singleton GroupNodes; sharing no join key, the FINAL merge
   cross-joined them `ON 1=1`. Fix: when a needed concept is NOT FD-determined by
   the merge grain (a finer/orthogonal row key), keep the parent whole at its row
   grain (`strategy_builder._wrap_for_grain`). Caller at line 724 only ever passes
   FD concepts, so it's unaffected.
2. **Missing dedup (duplicate rows).** With the scan kept whole, the multi-
   contributor FINAL merge sat at `{store,wh,product}` and leaked dups when `wh`
   dropped out of the output grain `(store_by_warehouse, product)`. The multi-
   contributor path never applied `group_if_required` (unlike single-contributor).
   Fix: `_assemble_final_node` now passes its assembled MergeNode through
   `_group_to_grain_if_required`. That set `force_group=True` but was **silently
   defeated** by two single-source passthrough shortcuts in `merge_node.py` that
   return the lone datasource ignoring force_group — guarded both with
   `and not self.force_group` (SHARED v3+v4 code; a merge explicitly told to group
   must not short-circuit). DEAD ENDS (don't repeat): (a) splitting only the finer
   concept and emitting a union-grain bucket — `from_concepts` re-expands the
   group-property's grain back to `(store,wh)`, re-introducing `wh`; the dedup must
   GROUP BY the output concepts as atoms (v3's `GROUP BY 1,2`); (b) preventing the
   product source's harmless 4-table over-join — its `(store,product)` projection
   is already correct, the wrong rows came purely from the cross-join + dedup gap.
   Validation: join_resolution 3/3; v3 (shared merge_node) optimization/complex/
   engine/processing/persistence 942 + modeling 333, 0 fail; v4 same suites 1012 +
   modeling 291, 0 fail/0 regression (only the 2 C8 tests flip to pass). See memory
   `project_v4_c8_forced_join_dedup.md`.

### FIXED 2026-06-13 — filter-only RECURSIVE inlined as a one-step CASE (test_recursive)
`engine/test_duckdb.py::test_recursive`. `where first_parent = 1 select id, label`
with `first_parent <- recurse_edge(id, parent)` returned 2 rows {1,2} instead of 4
{1,2,3,4}: v4 inlined the RECURSIVE arg as `CASE WHEN parent IS NULL THEN id ELSE
parent END = 1` instead of a `WITH RECURSIVE` CTE. FILTER-ONLY bug (selecting
first_parent built the CTE fine). Root cause: `_condition_arg_lineage_roots`
(source_planning.py) pulled the ROOT `.sources` of EVERY derived condition arg into
the bridge so the renderer could recompute it — correct for a BASIC arg
(gcat2 launch_date, the case it was added for), wrong for a row-shape barrier (the
recursion collapses to one step). Fix: skip args whose `derivation in
ROW_SHAPE_BARRIER_DERIVATIONS`. The bridge then can't source the barrier arg, the
strict `plan_source` attempt fails for it, and `gen_root`'s existing
`_resolve_root_condition_sources` fallback builds the recursive node via
`search_concepts` and merges it on the grain key with a terminal `first_parent=1`
filter — exactly v3's shape. DEAD ENDS (don't repeat): (a) making the recursive
GROUP host the filter — `gen_recursive` ignores the passed conditions (v3 too); the
CONSUMER applies the filter, so this dropped it entirely; (b) the
`_fold_passthrough_parents` guard — the recursive wrapper isn't a SelectNode/
MergeNode so the fold never touched it. Validation: engine+gcat 384 passed,
processing/optimization/complex/persistence/discovery 666 passed, modeling 291
passed, all 0 fail; v3 unchanged. Removed from v4_known_failing. See memory
`project_v4_recursive_filter_only.md`.

### FIXED 2026-06-12 — C7 constant-only WHERE dropped from render (the "date" misses)
`engine/test_sqlite.py::test_date_diff_rendering` +
`engine/test_bigquery.py::test_date_diff_rendering`. NOT a date-render bug: a
**constant-only WHERE was dropped entirely**. Query `select today where
date_add(current_date(), day, -30) < today` (today = const) emitted
`SELECT date('now')` with NO WHERE. The const `today` builds as two concept-graph
nodes — a d* SELECT-phase node and a d1 `@condition`-phase node — and the default
partition rule (`partition_by_depth_and_grain`, keyed on depth) split them into
two constant groups. Condition placement chose the upstream-most (d1) group, but
`gen_constant` ignores parents, so the downstream d* output ConstantNode just
rebuilt the constant and dropped the d1 group's WHERE. Fix: new
`partition_constants` rule (`group_rules.py` GROUPING_RULES) keyed on
`(scope, grain, grouping_mode)` — never depth, and on scope so the `@condition`
suffix doesn't re-split — merging the phases into one STAR group that carries the
WHERE (exact mirror of `partition_rowsets`). Removed both from v4_known_failing
(`_RENDER` reason now unused, deleted). Unit locks:
`test_partition_constants_*` in test_v4_group_behaviors.py. Validation: engine+
processing+optimization+complex 918 passed/0 fail; modeling 291 passed/0 fail; v3
unchanged. See memory `project_v4_c7_constant_condition_dropped.md`.

### FIXED earlier this round — q29 crash, q54/q77 rows, q2.1/q2.2 runaway
- **q29** (`test_twenty_nine`) — the LAST crash. `_get_query_node_v4` forked a
  fresh `V4History` that dropped the outer `build_caches.scoped_joins` (in-query
  JOIN merges), so the rowset's cross-fact inner select couldn't resolve →
  INVALID_REFERENCE_BUG. Fix: thread `build_caches=history.build_caches`. Same fix
  cleared **q54/q77** wrong-rows. See `project_v4_scoped_joins_rowset_threading`.
- **q2.1/q2.2** (`test_two_one`/`test_two_two`, C11 runaway) — `_aggregate_input_grain`
  skipped inline-expression args, so two per-fact aggregates collapsed to one
  bucket and joined RAW facts → soft cross-join. Fix: descend inline args to row
  identity (skip row-shape-barrier refs). Reclassified `_TPCDS_SIZE` (correct
  rows, verbose). See `project_v4_c11_multifact_aggregate_input_grain`.

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

### C10 — folded-in baseline fails (were untracked; all pass v3, fail v4) — 10
Folded into `v4_known_failing.py` 2026-06-12 (previously untracked but failing on
a clean baseline). All confirmed v4-only (the 10 pass under v3). Diagnoses for the
two highest-priority failures investigated in depth — both are deep planner gaps,
not quick wins:

- **CRASH** `tpc_ds q29 test_twenty_nine` (`ValueError: Invalid reference string`,
  `INVALID_REFERENCE_BUG` in the final SELECT). The `correlated` rowset's
  row-level source — the cross-fact join `physical_sales ⋈ returns ⋈
  catalog_sales` on (customer, item), pinned by the query's explicit `inner join`
  hints — fails to source: the v4 ROOT node `grp:root:root:∅` builds with
  `parents=[] -> None`, cascading to an empty rowset node and parent-less
  aggregate GroupNodes whose columns all render as the invalid sentinel. Root
  cause is v4 multi-fact correlated-grain ROOT sourcing returning None. Deep.
- ~~**ROWS** `engine test_recursive`~~ FIXED 2026-06-13 (see headline). The
  inlining was in the source_planning bridge: `_condition_arg_lineage_roots`
  pulled the RECURSIVE arg's ROOT lineage so the renderer recomputed it as the
  one-step CASE. Skipping ROW_SHAPE_BARRIER args there routes it through
  `gen_root`'s recursive-node fallback. (The earlier "carried into the final merge
  path graph" framing was a symptom, not the cause.)
- **ROWS** `tpc_ds q46` (different rows — genuine wrong-rows; the classifier
  mis-buckets it SHAPE because the captured planner log contains "datasource").
  q54/q77 landed earlier (scoped_joins threading). `q97_1`/`q97_2` now **PASS in
  isolation** under the current planner (re-confirmed 2026-06-20 via
  `v4_classify.py` + direct run) — the XPASS in full-suite runs is cross-file
  pollution; they remain tracked non-strict. Not yet diagnosed: q46.
- **SHAPE/source** `geography exact_match` ×2 (picks `full_tree_info` over the
  partial source), `q68` (tie-break ordering — numeric values match, only the
  LIMIT-tie city name differs), `composite_rollup` (NULL CASE over rollup key).
  Not yet diagnosed; lower priority.

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

### C7 — date arithmetic dropped from render — FIXED (was 2 → 0)
Was mis-named: not a date-render miss but a **constant-only WHERE dropped**. See
the FIXED note in the headline (`partition_constants`).
- ~~`engine/test_bigquery.py::test_date_diff_rendering`~~ FIXED
- ~~`engine/test_sqlite.py::test_date_diff_rendering`~~ FIXED

### C8 — ambiguous forced-join off-by-one rows — FIXED (was 2 → 0)
- ~~`modeling/join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join` (4 vs 3)~~ FIXED
- ~~`…::test_ambiguous_error_with_forced_join_order` (6 vs 5)~~ FIXED
Both: `_wrap_for_grain` per-key split → FINAL `ON 1=1` cross-join, plus a missing
multi-contributor FINAL dedup defeated by `merge_node.py`'s force_group-ignoring
single-source shortcuts. See the FIXED note in the headline.

### C9 — misc wrong rows — 1 (was 2)
- ~~`modeling/rides_example/test_ride_example.py::test_example_model`~~ FIXED 2026-06-13.
  `avg(daily_rides) by start_station.id` (daily_rides = `count(ride_id) by ride_date`)
  cross-joined ON 1=1: the outer aggregate's grouping dimension (start_station.id)
  was row-sourced from the standalone `stations` table, decoupled from the inner
  aggregate's grain key `ride_date`, so every station got `avg(all daily_rides)`.
  TWO graph-layer fixes (NO node-build re-sourcing): (1) concept graph —
  `_upstream_aggregate` now adds an inner-AGGREGATE arg's output grain (ride_date)
  as a row-input edge, mirroring `_aggregate_input_grain` (the edges and the
  computed input grain were inconsistent); this wires ride_date into the dimension's
  source so start_station.id is sourced from rides. (2) projection —
  `_project_dimension_parents_to_group_grain` keeps that bridge key when projecting
  the dimension to the group grain (else it strips ride_date and the post-projection
  merge has nothing to join on), GUARDED on `fd_needed.isdisjoint(other_outputs)` so
  it only fires in the genuine no-shared-key cross-join case (the_look multi-key
  aggregate is untouched). See memory `project_v4_c9_aggregate_over_aggregate_crossjoin`.
- ~~`modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate`~~ FIXED 2026-06-20 (see top FIXED note). NOT wrong rows — a `@weekday_sum(0)/@weekday_sum(1)` BASIC division over two aggregates that v4 emitted as a separate projection CTE instead of folding into the GROUP BY select. `CollapseSingleParent` now folds a BASIC child into a GROUP parent (gated by `basic_fold_into_group_is_safe`).

### C11 — fan-out RUNAWAY (the "timeout" cases, mis-labeled) — FIXED (was 2 → 0)
FIXED via aggregate input-grain derivation over inline-expression args (see the
headline FIXED note + `project_v4_c11_multifact_aggregate_input_grain`). The brief
below is retained for the original diagnosis; the actual fix was NOT the
fact-based split it proposed — it was `_aggregate_input_grain` ignoring inline
`BuildFunction` args (`sum(case ... web.price ...)`), which collapsed the two
per-fact aggregates into one bucket. Reclassified `_TPCDS_SIZE` (correct rows,
verbose SQL).

`tpc_ds_duckdb/test_queries.py::test_two_one` / `test_two_two` run the q2 variants
`query02-one.preql` / `query02-two.preql` (labels 2.1/2.2) — NOT q21/q22. The
classifier bucketed them TIMEOUT/OTHER; investigation shows they are a genuine v4
**fan-out runaway**, the soft-cross-join class flagged by the user (no literal
`1=1`). The v4 SQL joins **raw, un-aggregated** `web_sales` and `catalog_sales`
scans on `date_id` (= sold_date_sk, a non-unique key) BEFORE summing each to the
date grain:
```
wakeful (raw web_sales rows)  FULL JOIN  quizzical (raw catalog_sales rows)
    on web.date_id is not distinct from catalog.date_id
```
At sf=1 (web_sales ~7.2M, catalog_sales ~14.4M rows over ~1823 dates) this is a
per-date many-to-many product — billions of intermediate rows — so EXEC takes
~70s (q2.1) / ~103s (q2.2) for 53 correct output rows. The reference/v3 plan
aggregates `sum(ext_sales_price)` by `date_id` WITHIN each fact first, then joins
1-row-per-date. **Fix direction:** push the per-fact date-grain aggregation below
the union/join so the cross-fact join is 1:1 on the date key. NOTE: generation is
fast (~0.5s) and the SIZE/ROWS asserts pass — only EXEC is pathological — so this
never surfaced as a normal failure; it only shows as a timeout under the parallel
classifier (and bloats the suite's wall-clock). Reclassified in
`v4_known_failing.py` from `_TPCDS_SIZE` to `_RUNAWAY`. Mirrors the q29 (C10)
multi-fact-sourcing family: v4 under-aggregates before a cross-fact join. Full
handoff brief (root cause, fix direction in `partition_aggregates`, regression
guards, repro recipes): `local_scripts/v4_c11_multifact_aggregate_runaway_brief.md`.

## Coverage-gap conclusion

Only `top_x_by_metric` is distilled in `v4_evals/failing_cases/`. **All crash and
runaway clusters are now fixed** (C2–C6, C8, C9 rides, plus q29/C10, the C11
runaway, C7, the filter-only-RECURSIVE wrong-rows, and the q76 hidden crash
surfaced + fixed 2026-06-20). No crashes, no runaways remain (re-confirmed by a
fresh `v4_classify.py` run: 43 SHAPE / 3 ROWS / 2 XPASS / 0 genuine CRASH).
C9 def_wrapped turned out to be SHAPE (extra CTE), FIXED 2026-06-20. q97_1/q97_2
now pass in isolation. Remaining genuine wrong-rows work, in priority order:
**q46 wrong-rows → composite_rollup (NULL CASE over rollup key) →
geography exact_match source-selection (×2) → q68 tie-break → SHAPE/verbosity
backlog (the BASIC-into-GROUP fold trims CTE counts across this backlog).**

The 47 SHAPE entries need no eval case; they're tracked verbosity/inlining regressions to
close at the source-selection / render layer, gated on `CONFIG.use_v4_discovery`
in their SQL assertions.
