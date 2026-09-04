# Query-generation simplification: remaining work

What is left of the 2026-08 simplification audit. Everything that landed has
been removed from this file; the git history is the record of that. Three items
remain, plus a set of verdicts that exist to stop them being re-opened.

Pipeline stages as used below:

- LOGICAL: v4 discovery (`concept_strategies_v4.search_concepts`,
  `v4_helper/*`, `v4_node_generators/*`) builds the StrategyNode tree.
- PHYSICAL: `StrategyNode.resolve()` (`processing/nodes/*`,
  `join_resolution.py`) produces `QueryDatasource`s; `query_processor`
  turns them into CTEs.
- OPTIMIZED: `core/optimization.py` rule phases over CTEs.
- RENDER: `dialect/base.py` and per-backend subclasses.

House rules that shaped the verdicts: a provably-omittable join is a PLANNER
fix; optimizer rules are for post-multi-node state only; a missing join key is
fixed at discovery, never re-injected late; no `getattr`; never call code dead
on inspection or coverage alone.

## Gate recipe

Every item below is gated the same way: render SQL only (no DB) for every
`query*.preql` under `tpc_ds_duckdb`, `tpc_ds_duckdb/aggregates` (working_path
stays `tpc_ds_duckdb`) and `tpc_h`, and diff per statement against a
same-process control leg. The ~80-line harness is described in
`docs/handoff_invisible_contributor_joins.md` ("Reproduce"). Run the control
leg from a cwd outside the repo with `PYTHONPATH=<worktree>`. A corpus-only
gate is not sufficient on its own: it missed two keyless-join-guard raises in
this stack that the full suite caught, so run `-m "not adventureworks_execution"`
before landing anything here.

Also run `python -m local_scripts.fuzzer` (fixed seed, deterministic, a few
minutes). Neither corpus nor unit suite covers a union-TVF arm whose key has an
optional (`?`) binder elsewhere in the model; the fuzzer does, and one item in
this stack shipped a silent cross join through a byte-identical corpus and a
green suite. Any change to what a group's consumers see as their parent needs
this gate.

## 1. 3.6 One truth for partial/nullable (LANDED)

Decision taken: a node's partial and nullable stamps are its empirical inputs
(datasource columns, or the resolved parents' stamps) narrowed by its own
proofs, computed at resolve so a widened projection is restamped; the
QueryDatasource takes the stamp verbatim and nothing unions raw column flags
back in. Non-projected columns are not join-typing input.

- `select_node_v2.scan_stamps` is the scan rule (columns over projected
  outputs, minus `partial_is_full` / membership-complete / condition non-null
  proofs stored on the SelectNode); construction and resolve both call it.
- `StrategyNode._resolve` and `GroupNode` inherit partials from the resolved
  parents by address. `UnionNode` keeps only column-level `~` bindings (a
  covering union completes table-level partiality).
- `MergeNode` stamps outputs by `join_resolution.merge_partial_addresses`:
  a fully preserved side (`preserved_sources`, left-deep over the resolved
  join types) binding the address complete makes it complete; otherwise any
  partial side keeps it partial.
- `_collect_deep_partial_addresses` is deleted; join typing and the merge's
  branch proofs read the sides' own stamps.

Three consumers relied on the stale (empty) merge stamps and were tightened
to their real requirement: `deduplicate_nodes` (a redundant parent needs
every address it exposes bound by the survivor, complete ones complete, not
"no partial anywhere"); the merge folds sibling parents whose `shape`
(extent-agnostic identity plus resolved joins, recursively) matches, since
extent ownership that changed no join is not a distinct relation; and
`_is_filter_population` only defers to an extent-free branch's partner when
that partner binds the axis complete. Do NOT sync merge-level partials back
onto the node (discovery reads the node stamp for source completeness and
starts refusing complete sources).

Pins: `tests/core/processing/test_partial_nullable_stamps.py`,
`tests/join_matrix`, `tests/engine/test_duckdb_return_only_anchor_elision.py`,
`tests/modeling/test_nullability.py`, the modeling row suites. Corpus: 7
statements change, all row-verified (FULL/RIGHT narrowed to LEFT where the
fact side already carries every dimension member, a redundant order_items
self-join dropped in thelook adhoc04, a join-order flip, CTE renames).

## 2. 2.3(b) Filter-virtual wrapping (NOT LANDED)

RENDER decision that should be a planner decision. 2.3(a), the ORDER BY
`min(leaf)` wrapping, landed; the planner emits it now and
`query_processor._scalar_order_leaves` is the surviving owner. 2.3(c), the
passthrough-group gate, landed: `MergeNode._resolve` clears `force_group`
when any output passes a ROLLUP/CUBE/GROUPING SETS row through, and the
renderer's `_all_grouped_outputs_are_passthrough` / `_has_local_aggregate`
pair is deleted (0 firings across the full suite, fuzzer and corpus after the
planner change).

Filter virtuals are wrapped in `MAX(...)` at render (`dialect/base.py`,
`render_concept_sql`) in coordination with `execute.CTE.filter_collapses_to_grain`
and the GROUP BY exclusion in `CTE.group_concepts`, plus
`_aggregate_over_collapsed_filter` (`dialect/base.py`). This is live and
load-bearing for the `count(<key>)` double-count over a filter virtual whose
keys sit inside the grain but whose predicate reads non-grain columns; two files
coordinate through `filter_collapses_to_grain` to keep `group_concepts` and the
rendered column in agreement.

- Change: build such a filter virtual with a `max(case ...)` aggregate lineage
  at the hosting GroupNode, so the two agree by construction. The grain that
  decides it is only known at node resolve, not at concept build, so the
  substitution changes a concept's identity mid-plan.

Delete after: ~110 LOC in `base.py`, ~30 in `execute.py`. Pins: q16/q95/q05
rows, `tests/test_filtered_count_at_regroup_grain.py`,
`tests/test_filter_cte_grouped_metric_projection.py`. Risk MEDIUM-HIGH
(semantics), confidence MEDIUM.

## 3. 2.1 step 4: the residual optimizer LEFT/RIGHT to INNER branches (decision)

Steps 1-3 landed: the proof harvest is shared and
`grain_utility.downgrade_join_for_proofs:462` narrows LEFT/RIGHT from the
planner's own proofs. Optimizer flips fell from 436 to 45, and the statements
depending on the rule from 110 to 55.

The remaining 45 all rest on proofs that only exist after planning: conditions
the optimizer itself pushed, and cross-CTE consumer proofs
(`_external_forced_map`, `join_upgrade.py:607`). So `_downgrade_base_join`
(`join_upgrade.py:419`), the LEFT/RIGHT arms of `_downgrade`
(`join_upgrade.py:325`) and the BaseJoin loop in `UpgradeJoinOnGuards.optimize`
(`join_upgrade.py:759`, `:839`) stay unless pushed conditions become visible to
the planner. That visibility is the decision; it is the same prerequisite as
2.6's remaining sites.

`SimplifyNullSafeJoins` (`null_safe_join.py:189`) exists only because join types
change after planning and leave stale NULLABLE modifiers. Re-measure it if the
above ever moves; delete if it reaches 0.

## 4. 2.7 symmetric coverage (optional, 0 firings today)

`outputs_with_scoped_join_mates` (`v4_node_generators/aggregate.py:102`) is
generator-agnostic but wired only into `gen_aggregate` (`:155`). Wire it into
`gen_basic` / `gen_filter` / `gen_window` only if a test shape needs it. Nothing
fires today.

## Closed: do not re-chase

Verified during the audit, kept here only so the next pass does not re-open
them.

- **The review's three "known gaps" are fixed**, each pinned by a DuckDB row
  test: ORDER BY CASE/comparison over an unprojected leaf
  (`_scalar_order_leaves` walks CASE arms and comparisons;
  `tests/engine/test_duckdb_order_by_case_unprojected_leaf.py`);
  `narrow_keyless_joins` ignoring the explicit left after a keyed or unnest
  join; scoped joins on expression keys between rowsets consumed through
  projection wrappers (`_widen_merge_join_keys` second pass carries the
  unprojected expression mates;
  `tests/engine/test_duckdb_scoped_join_expression_keys_through_wrappers.py`).
  That mate rule must stay restricted to BASIC-over-rowset-handle members:
  environment `merge` declarations fold into the same key groups, and
  widening those re-plans hackernews adhoc05 and breaks the canonical
  collision merge test.

- **2.6 site B** (`source_planning` bridge merge, 57 relocations) is not a
  construction-time decision: `InlineDatasource` runs before pushdown and
  refuses a filtered root scan unless sole-consumer and all-INNER, so
  planner-hosted scan atoms split scans that are inlined today (75 statements
  changed, 6 new errors in the prototype). The other 30 are union-arm
  pushes/prunes that belong in the optimizer.
- **Not deletable, pinned by a named test**:
  `strategy_builder._hide_final_only_grain_keys` (multiselect align NULL
  padding, `test_v4_nested_select_parity::test_multiselect_arm_limit_applies`;
  `HideUnusedConcepts` does not cover it); the `_elide_passthrough_tree` pass
  (`test_non_benchmark_queries::test_or_membership_with_projected_aggregate`,
  `::test_membership_in_having_auto_concept_renders_valid_subselect`) AND the
  per-group `_elide_single_parent_passthrough` call in the build loop, which is
  not redundant with that pass: consumers copy a group's node as it is built, so
  a passthrough the tree pass would later collapse is already the shared parent
  two consumers regroup over
  (`test_duckdb_fuzzer_regressions::test_union_arm_partition_with_optional_fk_sibling`).
  Nor do the two collapse into one call: the publish seam needs one level, the
  tree pass is recursive and separately pinned, and calling the recursive form
  at the seam renders identical SQL for 2.6x the walk (9282 vs 3558 helper
  calls on the corpus, same 6 collapses). Also the
  second existence-attach loop (new errors on ds:query08, adhoc01,
  adhoc01_imports); `value_set_join_upgrade._upgrade_to_inner` (live under 46
  tests).
- **Corpus-silent but pinned elsewhere**, so a coverage- or corpus-only argument
  for deleting them is void: LOGICAL `resolve_alternatives` and hub helpers,
  `_fold_rollup_key_dims`, `_bound_column_components`, `group_rules._lineage_layers`,
  `partition_roots` single-row arms, `_post_aggregation_producers`, the
  `source_planning` `_plan_*` family, `then where` stage labels, `_seed_cover`,
  `_fold_covered_contributors`, `_carry_order_by_concepts`, `_bridge_pseudonyms`;
  PHYSICAL `gen_multiselect`, `gen_union`, `gen_subselect`, `gen_recursive`,
  `_interpose_limit_node`, the `condition_utility` predicates, `validate_stack`,
  `select_merge_node`'s `covered_conditions` retry; OPTIMIZED `JoinHoist`,
  `_push_having_into_group_parent`, `UnionDimPushdown`, `_narrow_directionally`,
  `_external_forced_map`, `OrderInnerJoinsFirst`, `StripRedundantNotNull`,
  `_optimization_visit_order`, `lower_full_joins` (MySQL only); RENDER
  `_canonical_render_siblings`, `_aggregate_collapse_safe`,
  `_render_expression_membership_exists`, `_constant_output_group_by_fallback`,
  the `base_alias` / `source_address` fallback chains.
- **Wrong stage but live**, for the record: outer-join key COALESCE
  (`dialect/base.py`), multi-source COALESCE, join-key COALESCE
  (`dialect/common.py`), COUNT to `coalesce(..., 0)`. The natural owner of the
  last one is nullability stamping in `merge_node.py`, which is item 1's
  territory.
- **Design-level note, not a handoff**: 448 planner FULLs "because partial" are
  narrowed by `_narrow_directionally` in 59 statements. That is the documented
  preserving-render contract (`docs/subset_union_join_design.md`) and conflicts
  with planner-owns-joins only in letter, since the planner has the same inputs.
  Moving `_pair_side_fully_matches` / `_complete_values` to node level is
  unmeasured.
