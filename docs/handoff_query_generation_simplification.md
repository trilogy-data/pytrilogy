# Query-generation simplification: remaining work

What is left of the 2026-08 simplification audit. Everything that landed has
been removed from this file; the git history is the record of that. Three items
remain (2.3(b), 2.1 step 4, 2.7); the grouping-set TVF (4) is done but for a
short list of pinned readers. A set of verdicts exists to stop them being re-opened.

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

## 2. 2.1 step 4: the residual optimizer LEFT/RIGHT to INNER branches (decision)

Steps 1-3 landed: the proof harvest is shared and
`grain_utility.downgrade_join_for_proofs` narrows LEFT/RIGHT from the
planner's own proofs. Optimizer flips fell from 436 to 45, and the statements
depending on the rule from 110 to 55.

`UpgradeJoinOnGuards` (CTE-to-CTE and base joins alike) was censused by
proof channel (`local_scripts/sql_ab/guard_census_plugin.py`, seam 14 of the
audit): of 1,539 flips over the suite, 475 needed only the CTE's own planned
WHERE. The planner now makes those itself: a FULL reads `IS NOT NULL` on a
column exclusive to one side, a RIGHT/FULL's padded side is the whole
left-deep stream, and a later INNER equality on a padded side's key proves
it present. 1,080 flips remain, 12 of them planned (5 are `union join` FULLs
the planner may not re-side once the merge has ordered its sources).

The others rested on conditions the optimizer itself pushed and on
cross-CTE consumer proofs (`join_upgrade._external_forced_map`). Both are a
WHERE or HAVING the planner placed above the merge, so the planner now
reads them too (seam 15): `consumer_proofs.narrow_for_consumers` hands each
merge what all of its consumers reject on columns read off it alone, through
plain selects, merges and non-grouping-set groups (keys only). Flips fell
from 1,059 to 608 (pushed 433, consumer 157, planned 12).

A consumer's join equality now reads too (seam 16): a NULL never matches
`=`, so `consumer_proofs.join_rejections` hands a merge the columns its
consumers join on, on a side the join drops unmatched rows of, and the merge
forces a side through any column it alone renders, partial or not. A
consumer's join type can widen when it is re-resolved over the narrowed
merge, so the channel is checked against the plan it built and dropped if a
consumer stopped rejecting (tpc_h adhoc07). Flips fell from 597 to 529
(pushed 431, consumer 77, planned 12, new CTE 9).

A join key forces its side too (seam 17), wherever the merge reads it off
that side alone: a `~` binding beside a complete one is never read, so the
key is NULL on the complete side's padded rows. Consumer flips fell from 77
to 49 (dimension chain 46 to 22, tpc_h adhoc07 6 to 0); 504 flips in all
on this run's base of 515 (distinct flips 304 to 284).

The 332 flips of one shape in `test_relation_rollup_laws` were discovery's
(seam 18). A WHERE on a key an unnest is merged into (`ecoregion_id`) made
every aggregate read the unnest as a row parent
(`concept_graph._constrain_aggregates_by_merged_expansions`), even one
grouped by that key, whose rows already reach it through the same unnest.
The planner joined a second unnest of the merge's rows back on (key, row
id) to read the key again, and put the WHERE above that self-join, where
no proof reaches the merge's LEFT. An aggregate that reads the key itself
now takes no such edge, so the WHERE lands on the merge. Flips fell from
486 to 157 on one machine (pushed 416 to 87, distinct 284 to 142).

Three gaps in the consumer walk itself were next (seam 19). A FilterNode
stopped every proof, though its WHERE reads its parent's rows as a select's
does: the filter laws' HAVING on a `_wscope` aggregate and the rowsets'
`n > 0` both sit on one. The walk read each consumer once, before the merges
under it narrowed, so a key a narrowing un-padded (a null-safe pair become
plain `=`) never handed its rejection down: it now repeats to a fixpoint.
And a merge that had ordered its sources could not narrow a FULL to the LEFT
a later INNER on its stream proves (only the RIGHT that barred is unsound).
Flips fell from 157 to 104 (pushed 87 to 55, consumer 49 to 30, planned 12
to 10; distinct 142 to 80).

Seam 20 opened the two places the walk still stopped. A proof drawn through
a computed flag (`returned_orders <- order_number ? is_returned`, `is_returned
<- _returned_order_number is not null`) names a column the node computing
the flag reads but does not output; `computed_inputs` lets it through to the
merge that pads it (tpc_ds q16, q94, q95). And a node reading another of its
own relation (a joinless merge over the regroup it passes through) no longer
stops the walk: both share what the relation's consumers reject (q58, q83,
the return-date pins, filters seed 5331, the union-key HAVING semijoin, the
subset join key sets). The case that backed this out before (dimension-chain
seeds 258 and 280) is now caught: a narrowed merge stamps an extent-free key
partial, and the joinless merge above re-resolves into a LEFT from the
aircraft lookup, returning aircraft with no flight. `_widened` backs the
join channel out when a re-resolved node's join types keep a parent's rows
it dropped before and did not all match (`preserved_sources`). Flips fell
from 101 to 82 on this run's base (pushed 52 to 36, consumer 30 to 28;
distinct 80 to 64).

Seam 21 took the filtered-aggregate rewrite and the dimension-chain
fallback. A group whose every aggregate reads one filter (`count(x ? p)`,
`sum(y ? p)`) passes `p`'s proofs to its parent as a WHERE where its
consumers reject a group that counted none (`consumer_proofs.group_filter`;
a COUNT is 0 there, so only a comparison 0 fails rejects it,
`condition_utility.zero_rejections`). A grouped merge reads an aggregate
off its parent as a key, so a HAVING on one reaches the group under it.
The key channel now bars only the merges that fail `_failing_keys` or sit
under a node that `_widened`, not the whole plan, and a narrowed merge's
extent-free partials are masked to what it held before, as its joined
partials are. Full suite census (`-n 4`): 125 flips to 92 (external 80 to
51, pushed 26 to 22; distinct 116 to 83).

Seam 22 found the cluster-key flips were an ordering gap in the planner's
own narrowing. `narrow_join_types` calls `downgrade_join_for_proofs` twice
per join: on the merge's and its consumers' proofs, then on its branches'.
It acted on a FULL only, so a consumer key that forced one side first
(FULL to RIGHT) left the branch proof forcing the other side nothing to
narrow; the RIGHT kept the lookup's rows, `_widened` barred the merge and
the optimizer flipped the LEFT. It now narrows any outer join
(`unpadded_join_type` already maps a LEFT/RIGHT whose padded side is forced
to INNER). Census, both trees on one machine: 92 flips to 59 (external 51
to 24, planned 10 to 5, pushed 22 to 21; distinct 44 to 38).

Seam 23 took the dimension chain. Its flips were not join generation
reading partial marks, as seam 22 guessed, but a reader re-typing itself
off the padding a narrowing dropped. Once the key channel narrows the
`max(seats)` side, its `carrier_name` is no longer padded, so the merge
pairing it with the count (never narrowed itself) re-resolves with the key
nullable on one side alone and `_nullable_join` keeps that side: INNER to
RIGHT. It stops rejecting `airport_name`, and `_failing_keys` / `_widened`
barred every keyed merge under it. `narrow_for_consumers` now holds a merge
whose joins grew to the types it planned (`MergeNode.planned_join_types`,
`utility.keep_planned_join_types`) and narrows again, barring only where no
merge can be held: the narrowing dropped only rows those types reject.
Census, both trees on one machine: 59 flips to 53 (external 24 to 18;
distinct 46 to 40); dimension chain 6 to 0. What is left:

- 21 pushed flips. tpc_ds q72's `inv.quantity_on_hand < cs.quantity`
  (1), the laws (windows 3, expressions 3, compositions 1), keyed-class
  trees 4, root partition 2, unmodelled regions 2, demo import 2, scoped
  join expression keys 2; one to two elsewhere. Each needs its rewrite
  planned first.
- 18 external: `test_semi_join_pushdown` 10 (CTEs the optimizer builds),
  `test_join_upgrade` 2 (hand-built CTEs that test the rule itself),
  filters seed 133525 2 (one consumer's LEFT stays for the optimizer; SQL
  byte-identical to `main`; a pin does not reach it), two-namespace import
  2, gcat 1, spellings 1. The last three are the next leads: small plans,
  not yet traced.
- 5 planned and 9 new-CTE flips remain, unchanged.

So `_downgrade_base_join`, the LEFT/RIGHT arms of `_downgrade` and the
BaseJoin loop in `UpgradeJoinOnGuards.optimize` (all `join_upgrade.py`) stay
until those are planned. Re-run the census
(`local_scripts/sql_ab/guard_census_plugin.py`) before deleting any of them.

The same holds for `UpgradeOuterFromKeySetEquivalence` (seam 2 of
`docs/query_gen_simplification_audit_2026_10.md`): 87 firings over the
whole suite, and its proof branches were ablated one by one (eleventh
slice). The nine the planner's `complete_key_domain` already proves are
deleted. Each branch kept names the plans or the test that move without it,
so do not re-ablate them without a new premise. Its vetoes were not
ablated, since turning one off makes the rule narrow more.

`SimplifyNullSafeJoins` (`null_safe_join.py`) exists only because join types
change after planning and leave stale NULLABLE modifiers. Re-measure it if the
above ever moves; delete if it reaches 0.

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
  `_external_forced_map`, `OrderInnerJoinsFirst`, `StripRedundantNotNull`,
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
