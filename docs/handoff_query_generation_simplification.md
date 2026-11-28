# Query-generation simplification: remaining work

What is left of the 2026-08 simplification audit. Everything that landed has
been removed from this file; the git history is the record of that. Two items
remain open (2.3(b), and 2.7, which is optional); 2.1 step 4 landed but for the
key-set join rule and the grain-pin render it lists, and the grouping-set TVF (4) is done but for
a short list of pinned readers. A set of verdicts exists to stop them being re-opened.

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
base worktree with `local_scripts/sql_ab` (`corpus_sql.py` + `sqldiff.py`; see
its README). Glob `tpc_ds_duckdb/aggregates` too: they are the
`partial ... complete where` summary-table shapes a source-selection change
re-routes first, and a per-directory glob skips them. A corpus-only
gate is not sufficient on its own: it missed two keyless-join-guard raises in
this stack that the full suite caught, so run `-m "not adventureworks_execution"`
before landing anything here.

Also run `python -m local_scripts.fuzzer` (fixed seed, deterministic, a few
minutes). Neither corpus nor unit suite covers a union-TVF arm whose key has an
optional (`?`) binder elsewhere in the model; the fuzzer does, and one item in
this stack shipped a silent cross join through a byte-identical corpus and a
green suite. Any change to what a group's consumers see as their parent needs
this gate.

## 1. 2.3(b) Filter-virtual wrapping (first slice landed)

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
`_aggregate_over_collapsed_filter` (`dialect/base.py`) and the HAVING
placement in `CTE.condition_placement`. This is live and
load-bearing for the `count(<key>)` double-count over a filter virtual whose
keys sit inside the grain but whose predicate reads non-grain columns.

**First slice (landed): one owner for the elided CASE.** Whether a filter's
per-row CASE renders at all is `execute.filter_case_elided`, read by the
renderer, the collapse and `_has_local_aggregate`. Before, only the renderer
knew a parent can guarantee the predicate, so the CTE wrapped an elided bare
column in MAX and grouped by it too
(`tests/test_filter_guaranteed_by_parent_stays_group_key.py`). The predicate
arm of `_has_local_aggregate` is deleted: 0 firings over the suite, since
the planner materializes a filter's predicate inputs in a parent. Each other
site was switched off alone against the 357 tests the collapse fires in (and
the 3,524 the elision fires in); every one fails tests, so all are pinned.
Detail: seam 11 of `docs/query_gen_simplification_audit_2026_10.md`.

- **Measured, not landed: a stamp from the plan.** The handoff's idea of
  deciding the collapse once at the hosting node does not hold as stated.
  The collapse is a fact about a (concept, CTE) pair, and most firings are a
  concept read only as an aggregate's argument in a CTE that does not output
  it, often after `CollapseSingleParent` folds the per-row CTE into the
  group: a birth-time stamp of each CTE's outputs disagreed with the live
  answer in every firing test file sampled.
- Still open: build such a filter virtual with a `max(case ...)` aggregate
  lineage at the hosting GroupNode, so the two agree by construction. The
  grain that decides it is only known at node resolve, not at concept build,
  so the substitution changes a concept's identity mid-plan. Pins: q16/q95/q78
  rows, `tests/test_filtered_count_at_regroup_grain.py`,
  `tests/test_filter_concept_is_a_value.py`, the filters and windows oracle
  laws. Risk MEDIUM-HIGH (semantics), confidence MEDIUM.

## 2. 2.1 step 4: the planner owns WHERE-driven join narrowing (landed)

`UpgradeJoinOnGuards` is deleted: seam 29 of
`docs/query_gen_simplification_audit_2026_10.md` took its consumer channel
and early pass, seam 30 its last pass (on a CTE's own WHERE once pushdown
has run); seams 14 to 28 are the path there. The planner narrows every outer
join a WHERE, HAVING or consumer join rejects the padding of
(`grain_utility.downgrade_join_for_proofs`,
`consumer_proofs.narrow_for_consumers`). With the last pass went the pushdown
refire after it, the `after_join_upgrades` deferral of atom removal and the
`upgrade_condition_joins` flag. The one plan only the last pass narrowed,
`test_window_expression_join`, is planned by a value-domain proof (seam 31):
a consumer key held to the concept's own values (`ConsumerProofs.keys`,
`join_rejections`) lets the merge force a side that carries every value of
the concept (`grain_utility.force_domain_sides`). Nothing else narrows less but
`test_fixpoint_contract_laws::test_ungated_pushdown_cycles`, which ungates
pushdown on purpose.

What is left of 2.1:

- `UpgradeOuterFromKeySetEquivalence` (seam 2 of the audit): its proof
  branches were ablated one by one (eleventh slice), and each branch kept
  names the plans or the test that move without it, so do not re-ablate them
  without a new premise. Its vetoes were not ablated, since turning one off
  makes the rule narrow more. Re-measured after seam 31 (seam 33): 82
  firings in 72 tests outside the tpc-ds and tpc-h files (which need the
  DuckDB extensions), 61 of them the equivalence upgrade. Seam 33 plans 8
  (twin aggregates of one stream through an INNER join on the key). Seam 34
  plans 15 more (a key the shared stream computes or keeps, and a join onto
  lefts an earlier INNER equated): 60 firings in 54 tests, 39 of them
  equivalence. Seam 35 plans 11 more keys (a split-key gate beside the
  totals it gates, INNER under the merge's own WHERE): 28 equivalence
  firings in 26 tests, 34 keys. Seam 36 plans 11 more keys of the
  different-streams class (twins of one stream each beside the one-row
  total over it, or beside the set a subselect of its filter reads): 22
  equivalence firings in 20 tests, 25 keys. Seam 37 plans 3 of the 4
  proved-but-not-narrowed keys (a side covered on the keys the right's
  grain keeps of the ON, which the walk now states): 20 firings in 19 tests,
  23 keys, counting the tpc-ds file's one firing. Seam 38 plans 3 more: a
  relation rollup's joins left to the walk (its forced FULL had overwritten
  668 joins the walk typed LEFT, and one INNER), and a split-key gate whose
  witnessing INNER is walked after the join it proves. 17 firings in 16
  tests, 20 keys. 9 are joins no planned merge holds (5 hand-built unit
  CTEs, 4 an optimizer rule built or moved), which is the rule's own
  post-multi-node work. The planner's debt is the other 8 firings (9 keys),
  none of them a modeling-suite shape: 6 different-streams keys in single
  fuzz shapes, which need a proof over two joins together; 2 split-key keys
  that need the padding NULL told apart from the value NULL (the grain-pin
  question below); and the pair whose addresses differ. See seam 38's
  "What is left".
- `SimplifyNullSafeJoins` (`null_safe_join.py`) is closed as a
  post-multi-node rule (seam 32). Its premise, stale `NULLABLE` modifiers
  from join types that change after planning, does not hold: of the 112 keys
  it strips (82 tests), 2 sit on a join planned FULL; on 7 the planned
  merge's own WHERE held the proof. The rest read a WHERE that predicate
  pushdown or a parent collapse put on a CTE after planning. Do not move it
  into the planner.
- The grain-pin render (seam 28): a pin renders as a bare `(expr)`, so a row
  whose anchor is NULL shows the computed value where the semantics say
  NULL. Guarding it with its anchors (`case when anchor is not null then
  expr end`) rewrites every pinned value's SQL and needs its own A/B. The
  guard as written is also wrong for an anchor that is NULL as a value (a
  `?` key's NULL group), which it would blank too: it needs the padding
  NULL told apart from the value NULL, which a CASE on the anchor cannot do.
  Decide the semantics first.

## 3. 2.7 symmetric coverage (optional, 0 firings today)

`outputs_with_scoped_join_mates` (`v4_node_generators/aggregate.py`) is
generator-agnostic and wired into `gen_aggregate` and the ROOT generator
(`v4_node_generators/root.py`). Wire it into `gen_basic` / `gen_filter` /
`gen_window` only if a test shape needs it. Nothing fires today.

## 4. Grouping sets as a relational TVF (every keyed read landed)

Multi-level grouping is written as a call, `rollup((select ...), keys)`
(`cube`, `grouping_sets`), and lowers to a one-arm TVF (`RollupSelectLineage`,
`gen_rollup_select`): named (`with r as rollup(...)`), inline
(`from rollup(...) -> (names) select ...`), or spread into a query
(`select *rollup(...)`), which hydrates to a select over an anonymous rowset's
handles, shown under the arm's names through `SelectStatement.labels` exactly
as a signed spread is (no authored/planned statement pair). The `by rollup|cube|grouping sets`
select clause is gone: it still parses, to raise a migration error. Every read
of a keyed rollup is of new key and property concepts, so nothing above the arm
tracks grouping-set padding. Below the planner the pass is the group's:
`GroupNode` decides its `BuildGroupingSets` once and the datasource, CTE,
renderer and optimizer read that, never an aggregate's mode. The arm is
pure: windows, scalars over measures, HAVING, ORDER BY and LIMIT are lifted
into a select over its relation (`rollup_lift.py`), and the arm is planned
as a standard select: no aggregate carries a grouping mode. The pass is the
select's, `BuildEnvironment.grouping` on the arm's build environment, and
every reader asks `BuildEnvironment.pass_of`. The readers were ablated one by
one (seam 10, seventh slice, of `docs/query_gen_simplification_audit_2026_10.md`):
14 of 28 toggles are deleted. Each of the 12 sites kept names a test or a set of
plans that moves without it: 7 for rows, 5 for plan size (the other two toggles
were unions of kept sites). The plan-size half is the
only open lead: `_fold_rollup_key_dims` both saves and costs plans, so
narrowing it (fold only a dim no later read can compute above the pass) could
delete its remainder. Do not re-ablate the others without a new premise.

Gate any change here with `tests/engine/test_duckdb_rollup_tvf_laws.py`
(rowset reads against the statement) and
`tests/engine/test_duckdb_rollup_statement_laws.py` (the statement against
DuckDB): the suite's own rollup-in-rowset coverage is about twenty tests and
passed two render defects the laws caught.

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
  `OrderInnerJoinsFirst`, `StripRedundantNotNull`,
  `_optimization_visit_order`, `lower_full_joins` (MySQL only); RENDER
  `_canonical_render_siblings`, `_aggregate_collapse_safe`,
  `_render_expression_membership_exists`, `_constant_output_group_by_fallback`,
  the `base_alias` / `source_address` fallback chains.
- **Wrong stage, now one owner each.** COUNT to `coalesce(..., 0)`: the
  source decides it (`QueryDatasource.zero_filled`, seam 12 of the audit)
  and the CTE, renderer and pushdown read it. Do not move the decision back
  into the CTE: an optimizer that changes a CTE's grouping re-decides it
  through `CTE.regroup`, and that is the only place the CTE recomputes it.
  Outer-join key, multi-source and join-key COALESCE: `CTE.column_reads`
  decides which parent columns a key reads and `coalesced_key_groups` which
  ON pairs coalesce (seam 13); the renderer only formats them. These stay
  CTE-level on purpose: the optimizer changes join types after planning,
  and an outer join's key class is a fact about the final joins.
- **Design-level note, not a handoff**: 448 planner FULLs "because partial" are
  narrowed by `_narrow_directionally` in 59 statements. That is the documented
  preserving-render contract (`docs/subset_union_join_design.md`) and conflicts
  with planner-owns-joins only in letter, since the planner has the same inputs.
  Moving `_pair_side_fully_matches` / `_complete_values` to node level is
  unmeasured.
