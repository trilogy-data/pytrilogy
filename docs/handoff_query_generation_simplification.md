# Handoff: query-generation simplification audit (2026-08-23)

## Status 2026-08-23: LANDED through wave 3 (commits aecae9837 .. see git log)

Landing was done in three waves of parallel agents with disjoint file
ownership, a serialized pytest lock, and a per-wave corpus baseline. Every
commit message records its corpus A/B result. Item status:

| Item | Status | Notes |
|---|---|---|
| 0.1, 3.1, 3.2 | LANDED 5c5547b2f | one SQL template; persist renders on every backend; new all-dialect persist test |
| 0.2 | LANDED d1de7b573 | q14 renders with merge_aggregate off |
| 0.3, 0.4 | LANDED aecae9837 | q64/q80/q84 lose a redundant relocated predicate |
| 1.1, 1.2, 1.3, 1.12-1.16, 1.18 (physical), 3.8, 2.8 | LANDED c2897f784 | `_padding_sources` kept (live import); `_union_key_siblings` is called (moved, not deleted) |
| 1.5, 1.6, 1.7 (per-node), 1.8 (helper fold), 1.18 (builder), 3.9, 3.10 | LANDED d1de7b573 | |
| 1.4 | NOT DELETABLE | pins `test_v4_nested_select_parity::test_multiselect_arm_limit_applies` (multiselect align NULL padding); HideUnusedConcepts does not cover it |
| 1.7 tree pass, 1.8 loop | NOT DELETABLE | tree pass: `test_non_benchmark_queries::test_or_membership_with_projected_aggregate`, `::test_membership_in_having_auto_concept_renders_valid_subselect`; loop: NEW ERROR on ds:query08/adhoc01/adhoc01_imports. The corpus-only ablation missed both |
| 1.9, 1.10, 1.18 (logical), 3.7, 3.10 (group_rules), 3.8 tail | LANDED e4c5f4719 | |
| 1.11, 1.18 (render/execute), 3.4 | LANDED 5c5547b2f | `CTE.inlined_alias_map` kept (reachable from the live `str` arm) |
| 1.17, 1.19, 1.20, 1.21, 3.11, 3.12, 3.14 | LANDED aecae9837 | 3.12 landed as enforcement (`validate_optimization_rule_plan`), not deletion; 1.21(d) had a direct test (renamed, realign asserts replaced) |
| 3.13 | PARTIAL aecae9837 | the two completeness helpers deleted; `_upgrade_to_inner` is live under tests (46 true) and stays |
| 2.1, 3.5, 2.5 | LANDED c4cc2a05f, 81ce16e8c | optimizer LEFT->INNER flips 436 -> 45, all residual need post-planning proofs (pushed conditions, external consumers); statements still depending on the rule 110 -> 55; NarrowKeylessFullJoins deleted |
| 2.2 | LANDED c4cc2a05f, 81ce16e8c | `_clear_identity_group` deleted after GroupNode honours is_identity_group and CollapseSingleParent treats `sum(x) by k` over the grouping parent as a rename |
| 2.3(a) | LANDED f3c846fbb | the planner emits `min(leaf)` in the ordering of a grouped final; a plain reference is impossible for that shape (GROUP BY lower(ch) vs ORDER BY ch), so this is one owner, not zero wrapping |
| 2.4 | LANDED fc533ce3f | direct return deleted; the 16 residual roots were conditioned projections, not ORDER BY/LIMIT shapes |
| 2.6 | BLOCKED (wave 2), retried in wave 3 | the 142 pushes originate at gen_root's existence wrapper (34) and source_planning's bridge merge (57), not the strategy_builder sites; a conditioned COPY of a history-cached node diverges (q64) and a node condition refines nullability-derived join rendering (q80) |
| 2.7 | 3 of 4 LANDED c2897f784; 4th in wave 3 | `_inject_scoped_join_key_exposure` fires on derived-key union joins under join_matrix |
| 3.6 | BLOCKED, design decision needed | the divergence is real but not where the spec pointed: node-level stamps are output-restricted while `_collect_deep_partial_addresses` reads NON-projected `~` columns for join typing (gcat:inline29 flips INNER->LEFT/FULL when the QDS honours the stamps), and post-construction widening (`projection.widen_projection`, `set_output_concepts`) never restamps (gcat:inline32 loses its date-spine FULL). Decide whether non-projected partials are join-typing input; then either widen the stamp or make join typing read datasource columns, and restamp on widening. Also: dropping the construction-time `_refine_nullable_for_conditions` loses q64's membership pushdown (`semi_join_pushdown.nullable_in` reads `cte.nullable_concepts`) |
| 2.3(b)(c), 3.3 | wave 3 | see the per-item sections |

### Resume here (paused 2026-08-23 during wave 3)

Landed: 11 commits, aecae9837 .. 737bb0e37 on `more_eval_tuning`, each
gated by a same-process corpus A/B and the TPC-DS/TPC-H row batteries.

In flight when paused (two background agents, not committed):

- Agent R (2.3b, 2.3c, 3.3): owns dialect/*, execute.py, query_processor.py,
  optimization.py, optimizations/*, having_normalization.py, plus
  group_graph.py / group_node.py / discovery_utility.py for the planner side
  of 2.3. Had written nothing to the tree at pause time.
- Agent Q (2.6 retry at gen_root / source_planning): owns
  v4_node_generators/root.py, v4_helper/{source_planning,strategy_builder,
  condition_injection,condition_placement}.py. `root.py` was modified and
  uncommitted at pause time.

If the session resumes, their completion reports arrive as task
notifications and each is committed separately after review. If it does not,
`git diff` on those files is their partial work: verify with the gate below
before keeping any of it, or discard it (only those files; the timing
artifacts are separate, see next point).

Also uncommitted, deliberately: the regenerated tests/modeling timing
artifacts (`*.png`, `*-summary.md`, `zquery*.log`) and
`crates/trilogy-io/Cargo.lock` (version sync to 0.3.338). They are committed
by design with the diffs that produced them; commit them once on the final
tree after the last wave, not per wave.

Remaining work, in order:

1. Collect R and Q; commit what passes the gate; mark the table rows.
2. Regenerate and commit the timing artifacts on the final tree.
3. Full suite (`.venv/Scripts/python.exe -m pytest tests -m "not
   adventureworks_execution" --ignore=tests/cli/test_cloud_live.py`; takes
   ~22 minutes, run it detached, the Bash tool kills at 10 minutes); expected:
   only `tests/modeling/gcat/test_gcat.py::test_environment`, pre-existing.
4. Decide 3.6 (see the table row) and 2.1 step 4 (the 45 residual optimizer
   LEFT->INNER flips all need post-planning proofs, so the optimizer branches
   stay unless pushed conditions are visible to the planner).
5. Optional symmetric coverage for 2.7: `outputs_with_scoped_join_mates` is
   generator-agnostic; wire into gen_basic/gen_filter/gen_window only if a
   test shape needs it (0 firings today).
6. `docs/v4_network_discovery_design.md:928` still names
   `_inject_scoped_join_key_exposure`.

Gate tooling lives in this session's scratchpad `land/` directory
(`corpus_render.py`, `corpus_diff.py`, `locked_pytest.py`, `AGENT_RULES.md`,
`base3_<dialect>.json` = render of commit 81ce16e8c). The scratchpad is
ephemeral; the render harness is the ~80-line recipe from
`docs/handoff_invisible_contributor_joins.md`, and `base3` is reproducible by
rendering that commit from a worktree.

Verification of the final tree: full suite (`-m "not adventureworks_execution"`,
cloud live tests ignored) 8556 passed after wave 1 with one failure,
`tests/modeling/gcat/test_gcat.py::test_environment`, which fails on the
pre-wave commit c8eab06fe as well (DatasourceColumnBindingError, unrelated).

The per-item sections below are the original audit text and keep their
pre-landing line numbers.

Pipeline stages as used below:

- LOGICAL: v4 discovery (`concept_strategies_v4.search_concepts`,
  `v4_helper/*`, `v4_node_generators/*`) builds the StrategyNode tree.
- PHYSICAL: `StrategyNode.resolve()` (`processing/nodes/*`,
  `join_resolution.py`) produces `QueryDatasource`s; `query_processor`
  turns them into CTEs.
- OPTIMIZED: `core/optimization.py` rule phases over CTEs.
- RENDER: `dialect/base.py` and per-backend subclasses.

House rules that shaped the verdicts: a provably-omittable join is a PLANNER
fix; optimizer rules are for post-multi-node state only; a missing join key
is fixed at discovery, never re-injected late; no `getattr`; never call code
dead on inspection or coverage alone.

## Method and evidence

Six independent sweeps, each rendering SQL only (no DB), each with its own
monkeypatched counters, and for every deletion candidate an A/B leg (site
disabled) byte-diffed per statement against a same-process control leg
(control = 0 changed in every run). Corpora:

- Core: tpc_ds_duckdb 109 `query*.preql` + `aggregates/` (working_path =
  `tpc_ds_duckdb`, text from `aggregates/queryNN.preql`) + tpc_h 23 = 134.
- Extended (cross-stage and node sweeps): + `adhoc*`, thelook_duckdb,
  the_look, gcat (36 inline queries from `test_gcat.py`), faa, ncaa,
  hackernews, stocks, tpc_ds = 235 statements (10 are deliberate-error test
  shapes and are excluded from diffs).
- Render sweep additionally rendered the 134 core queries through all nine
  dialect generators and 590 self-contained model+query strings harvested
  from `tests/**/test_*.py`.

The harnesses live in this session's scratchpad
(`...\Temp\claude\C--Users-ethan-coding-projects-pytrilogy-two\ec8799c0-...\scratchpad\`:
`xstage_audit.py` + `run_legs.py` + `xstage_out/`, `audit_sweep.py` +
`ab_legs.py`, `nodes_probe*.py` + `nodesweep.py`, `rules_ab_setup.py` +
`rules_render.py` + `rules_ab_diff.py`, `sweep*.py` + `cov_*.json`). They are
ephemeral; the recipe is ~80 lines and is described in
`docs/handoff_invisible_contributor_joins.md` ("Reproduce"). Re-create it
rather than trusting stale outputs.

**Gate for every item unless stated otherwise:** render the 235-statement
corpus twice in ONE process (change on / change off), per-statement
byte-diff, expected changed set exactly as listed for the item (usually
empty); then the item's named pin tests; then `ruff check . --fix`, `mypy
trilogy`, `black .`. Run pytest suites one at a time.

---

## Tier 0: defects found along the way (fix first, they gate other items)

### 0.1 Persist rendering is broken on postgres, sql_server, clickhouse
- RENDER. `trilogy/dialect/{postgres,sql_server,clickhouse}.py` SQL_TEMPLATE.
- `persist out into out_table from select id, name;` renders: postgres raises
  `jinja2 UndefinedError: 'str object' has no attribute 'address'`
  (template expects `output.address.location`); sql_server and clickhouse
  templates have no `output` block, so the `INSERT INTO` prefix is silently
  dropped and a bare `SELECT` is emitted. No test pins persist on those
  three backends.
- Fix is item 3.1 (one template). If 3.1 is deferred, at minimum add the
  `output` block to the three templates and a persist render test
  parametrized over every dialect asserting the INSERT prefix survives.

### 0.2 tpc_h q14 only renders because `CollapseSingleParent` repairs a planner contract
- LOGICAL. `v4_helper/strategy_builder.py` `_project_basic_aggregate_inputs`
  (~L1277-1336), the `keep` computation (~L1322-1329).
- With `CONFIG.optimizations.merge_aggregate=False`, h:query14 fails with
  `Missing source reference to part.type`: the pre-optimization CTE
  projects `[order.id, line_no, revenue]` while the group's `needed` set
  contains `part.type` (the WHERE arg of a filtered aggregate). The
  AGGREGATE-mode fold of scan into group hides the gap.
- Change: when a direct aggregate arg's lineage is a `BuildFilterItem`, keep
  `lineage.where.row_arguments` (and content args) in `keep`; equivalently
  walk `_row_lineage_closure` one level deeper for FILTER virtuals.
- Gate: corpus render with `merge_aggregate=False` has 0 errors (today 1)
  AND baseline SQL byte-identical with it on. Pins:
  `tests/modeling/tpc_h` q14 rows, `tests/optimization/test_merge_aggregate.py`.
- This is a prerequisite for any change to CollapseSingleParent (items 2.4).

### 0.3 `narrow_keyless_full_joins` is registered twice
- OPTIMIZED. `core/optimization.py` L773-790 and L827-844 are verbatim
  copies (same deps, same reason); provenance is two commits (#602, #609)
  each adding one. Only `push_filtered_count_into_join` and
  `push_filtered_aggregate_input` run between them and neither creates a
  FULL join.
- Delete the second block (18 LOC). Gate: corpus byte-identical; the rule's
  3 corpus firings (gcat:inline31 x2, h:adhoc05, ds:query-1) must still
  occur in the first phase; `tests/optimization/test_keyless_full_join.py`.
  Measured directly: the second copy ran 212 times and changed 0; it also
  overwrites `phase_actions["narrow_keyless_full_joins"]`, losing the first
  run's `changed` flag.

### 0.4 `PredicatePushdownRemove(after_join_upgrades=True)` is never constructed
- OPTIMIZED. `core/optimization.py` L718-731 registers the phase named
  `predicate_pushdown.remove.after_join_upgrades` with a bare
  `rule_factory=PredicatePushdownRemove`, so the post-upgrade remove pass
  runs with deferral still on (`predicate_pushdown.py` L945-948, L986-1005
  `_defers_to_join_upgrade`). Born unwired in a6161b981; grep
  `after_join_upgrades` finds only the definition, its own check, and the
  phase name string. In that phase `_defers_to_join_upgrade` returned True
  6 times / 3 queries.
- Fix: `rule_factory=lambda: PredicatePushdownRemove(after_join_upgrades=True)`
  (what the docstring at L986-995 promises). A/B with it wired: 3/138
  queries (ds:64, 80, 84) drop a now-redundant `... is not null` atom from
  the final WHERE, 0 new errors. Alternative: delete the parameter and the
  early exit (6 LOC) if the deferral is judged unnecessary.
- Gate: tpc-ds q64/q80/q84 execution tests,
  `tests/optimization/test_pushdown_optimization.py`, `test_join_upgrade.py`.
  Confidence HIGH that it is unwired; MEDIUM on wire-vs-delete.

---

## Tier 1: pure deletions, A/B-proven byte-identical on the corpus

Ordered by confidence then LOC. "0 changed / N" = ablation leg vs control.

### 1.1 `_expose_downstream_referenced_columns` (query_processor) is render-inert
- PHYSICAL. `trilogy/core/query_processor.py` L1141-1193 + call at L1570.
- Un-hides 13 columns in 7/134 queries (ds:24, 31, 41, 59, 65, 78, h:15);
  0 changed / 160 and 0 / 34 gcat. Mechanism: the only consumer that would
  render the column is the stale `root_cte` object the optimizer already
  collapsed into its parent; the parent becomes `ctes[-1]` and
  `sort_select_output_processed` (`processing/utility.py` ~L299-303)
  recomputes its hidden set from `query.output_columns` at render time,
  re-hiding it. Its motivating shape
  (`tests/test_filter_cte_grouped_metric_projection.py`, q23 binder error)
  was rebuilt with the function removed: SQL byte-identical, rows identical.
- Owner: RENDER already owns final-CTE visibility via `sort_select_output`;
  OPTIMIZED `HideUnusedConcepts` owns intermediate demand.
- Delete ~55 LOC. Gate: that test file, `tests/optimization/*`, corpus.
  If the test ever fails without it, the fix is in
  `sort_select_output_processed` (do not hide a root column a consumer's
  rendered output references), not a second collector.

### 1.2 `process_query` root-CTE hidden overwrite is a no-op and contradicts `_plan_query_node`
- PHYSICAL. `query_processor.py` L1521
  (`root_cte.hidden_concepts = statement.hidden_components`).
- Drops planner-hidden keys in 25/134 queries (+3 gcat) and adds one in q66;
  two A/B legs (planner-union and planner-only) both 0 changed / 160 because
  the last CTE's hidden set is recomputed at render. `_plan_query_node`
  L850-852 and its docstring say hidden sets are merged, not overwritten.
- Delete the line; fix the docstring. Note for a separate look:
  `ProcessedQuery.base` is the stale pre-collapse root in these cases
  (q24 `base=young`, `ctes[-1]=concerned`) and `_validate_persist_projection`
  reads `select.base`.

### 1.3 `_collect_unreachable_union_arms` adds CTEs that never render
- PHYSICAL. `query_processor.py` L1196-1215 + call L1505 (and `flatten_ctes`
  L1134-1138 stays).
- Returns non-empty 107 times / 27 queries; 0 changed / 160, 0 / 34 gcat,
  0 / 26 thelook. `render_cte(UnionCTE)` renders arms inline from
  `internal_ctes` and `datasource_to_cte` (~L399) already folds every arm's
  `parent_ctes` into the union's, so arm ancestors are already reachable.
- Delete ~22 LOC. If a case is ever found, extend `flatten_ctes` to walk
  `internal_ctes` rather than re-adding a collector. Gate:
  `tests/engine/test_duckdb_rowset.py`, union TVF tests, corpus.

### 1.4 `_hide_final_only_grain_keys` duplicates `HideUnusedConcepts`
- LOGICAL. `v4_helper/strategy_builder.py` L3328-3345 + call site.
  **Touches active work.**
- Hides 104 columns / 38 queries; ablation 0 changed / 235 (HideUnusedConcepts
  hides the same columns: 839 -> 841). Pinned by
  `tests/core/processing/test_v4_group_behaviors.py` (2 refs; retarget).
- Delete ~18 LOC. Owner: OPTIMIZED (demand is only knowable after
  inlining/collapse move columns).

### 1.5 `_validate_not_pushed_past_independent_barrier` cannot raise
- LOGICAL. `v4_helper/condition_placement.py` L525-543 (def), L1338-1344
  (call), comment L1287-1292. **Touches active work** (pure deletion).
- L1293-1305 already filters `restricted` by the identical criterion
  (`(d0_group_ids & ancestors(gid)) - consumed_barriers` over the same
  `_producer_groups` input); `chosen_groups` and the `_nested_scope_swallows_atom`
  re-election both pick from subsets of `restricted`, so `offending` is
  always empty. Raise line never executed (134/134); no test references it.
  A/B 134/134 byte-identical.
- Delete ~27 LOC incl. the second `_producer_groups(row_inputs, buckets)`
  call it forces. Gate: `tests/core/processing/test_v4_condition_placement.py`.

### 1.6 Second `_compute_concept_sets` call in `build_group_graph` is redundant
- LOGICAL. `v4_helper/group_graph.py` L3035-3046. **Touches active work.**
- Call 3 (L3086-3109) runs unconditionally under the same
  `mandatory_list is not None` guard and recomputes every IO field. Nothing
  between call 2 and call 3 reads `output_concepts`/`input_concepts`
  (`_inject_conditions` takes `buckets`; `_propagate_raw_filters_to_d1_roots`
  reads depth/derivation/atoms/members; `_color_phases` reads edges); regraft
  side effects persist and feed call 3. The block does execute on the corpus
  (real work removed, not a dead branch). A/B 134/134 byte-identical.
- Delete 12 LOC (+ the now-unread `merged`/`changed` locals). Also removes
  roughly a third of the concept-set pass, the heaviest pass in group_graph.
  Gate: `test_v4_concept_sets.py`, `test_v4_window_basic_merge.py`,
  `test_v4_join_stream_reuse.py`, gcat/thelook CI.

### 1.7 `_elide_passthrough_tree` (planner) duplicates `CollapseSingleParent` PASSTHROUGH
- LOGICAL. `strategy_builder.py` L1114-1125 + call L4463 (tree pass);
  L1077-1111 + call L4428 (per-node). **Touches active work.**
- Tree pass collapses 1/134; ablation 0 changed / 160. Per-node collapses
  5/134 (+1 gcat); ablation 3 changed (ds:44, h:15, h:22) totalling -7 bytes,
  which after CTE-name normalization is only column order in q44; the
  optimizer collapses the same nodes (561 -> 568 CollapseSingleParent fires).
- Delete the tree pass now (12 LOC; retarget
  `test_v4_group_behaviors.py::test_elide_passthrough_tree_collapses_linear_identity_projection`).
  Delete the per-node pass too if golden churn on those three queries is
  acceptable (~37 LOC more). Owner: OPTIMIZED, see 2.4.

### 1.8 Second existence-attach pass and a near-duplicate helper are inert
- LOGICAL. `strategy_builder.py` L4474-4477 (post-assembly loop calling
  `_attach_existence_to_node` over the whole tree after
  `_attach_existence_sources` at L4451 already walked every root);
  `_filter_lineage_existence_arg_groups` L412-427 (near-copy of
  `_lineage_existence_arg_groups` L224-251, called only at L447).
  **Touches active work.**
- Phase-B loop changes a node 1/134, 0 in gcat/thelook; both ablation legs
  0 changed / 160, 0 / 34.
- Delete the loop; fold the helper into `_node_existence_arg_groups` via
  `_lineage_existence_arg_groups(list(node.output_concepts))` (~25 LOC).
  Gate: `test_v4_node_generators.py` existence cases, `test_v4_subselect.py`,
  q08/q16/q17 in the battery.

### 1.9 `ConditionFit` has six values; the search reads two bits
- LOGICAL. `v4_helper/network_model.py` L47-81, `network_build.py`
  `_condition_fit` L127-151.
- Only `.disqualifying` and `.partial_is_full` are read. `APPLIES`,
  `UNAFFECTED`, `DEFERRED` are produced (105 / 9869 / 1030 labels per corpus)
  and read by nothing but one assertion in
  `tests/core/processing/test_v4_network_search.py:345`. A/B mapping all
  three to NEUTRAL: 0 changed / 160, 0 / 34. The docstring and design doc
  0.10 item (6) both say they were never wired in.
- Keep `NEUTRAL`/`IMPLIED_EXACT`/`SENSITIVE`; delete the rest and L144-151
  (~25 LOC, one test assertion). Side benefit: coarser `signature()` means
  more `search_cache` hits.

### 1.10 `BuildInfo.copy` deep-copies graphs nobody reads
- LOGICAL. `v4_helper/models.py` L243-300 (`copy`, `_copy_attrs`,
  `_copy_concept_attrs`); called from `history.py:73` and
  `concept_strategies_v4.py:567`, twice per search.
- Production reads only `.strategy_node`; graph/attr/edge readers are
  read-only tests (`test_extent_ownership.py`, `test_v4_fd_grain_preservation.py`)
  and `local_scripts/discovery_v4.py`. 248 copies / 5,318 graph nodes per
  corpus run. A/B shallow-share: 0 changed / 160, 0 / 34.
- Copy only `strategy_node`; delete the two helpers (~50 LOC).

### 1.11 `_grain_key_membership_redirect` (render-time INVALID-sentinel rescue) no longer fires
- RENDER. `dialect/base.py` L1295-1336, L301-316, call L1414-1417; test
  `tests/dialect/test_collect_subselect_comparisons.py` pins the helper only.
- Entered only when `BASE_INVALID in result`: 0 hits across 9 dialects x
  134, 17 extra corpora, 590 test queries. Provenance #593 (q64); q64 now
  renders with 0 sentinels on every dialect. The upstream dead-ended
  grain-key projection is fixed, so the mask is dead weight.
- Delete ~60 LOC + the 65-line test. Keep the sentinel guard in
  `_render_query`. Gate: q64 battery, join_matrix, corpus on DuckDB+BigQuery.

### 1.12 Dead v3-era `History` API
- PHYSICAL. `processing/nodes/__init__.py` L47-54 (fields
  `local_base_concepts`, `history`, `rowset_history`, `started`,
  `merge_in_progress_keys`), L77-182 (`search_to_history`, `get_history`,
  `log_start`, `log_end`, `check_started`, `merge_in_progress`).
- Zero references outside the definitions; all six methods never called on
  the corpus; last touched by "Drop V3 Engine (#632)". `V4History` uses none
  of them.
- Delete ~112 LOC (+ unused `contextmanager`, `UnresolvableQueryException`
  imports). Gate: mypy, `tests/core/processing`, `tests/generators`.

### 1.13 Write-only / zero-caller node API
- PHYSICAL. `nodes/base_node.py`: `WhereSafetyNode` L739-764 (never
  constructed; two stale comments in `test_non_benchmark_queries.py`
  L837/865), `set_preexisting_conditions` L296-301, `add_condition`
  L303-318, `set_visible_concepts` L419-423, `all_used_concepts` L483-485,
  `tainted` L234/629 and `log` L255 (write-only), `add_output_concept`
  L446-447 (one caller, `merge_node.py` L533; fold to
  `add_output_concepts([c], rebuild=False)`), `NodeJoin.unique_id` L726-729
  (tests pin `BaseJoin.unique_id`, not this), `NodeJoin.filter_to_mutual`
  never passed True (L692-724 collapse to "must exist on both sides else
  raise"). `virtual_output_concepts` (base_node L202/237/670, merge_node
  L290/312/1071, loop in `discovery_validation.py` L295-299): never non-empty
  in 4184 constructions; only a test stub sets it. `MergeNode.join_concepts`
  (L279/316/1073) never passed in 840 constructions, never read.
  `GroupNode.required_outputs` (group_node L50/69-71/309-311) never passed
  in 680, never read. `SelectNode.accept_partial` (select_node_v2
  L45/77/259/294 and the `accept_partial` param of `create_datasource_node`,
  `select_helpers/datasource_nodes.py` L256/444) write-only.
  `base_node.py` L610 `getattr(self, "set_operator", ...)` violates the
  no-getattr rule: declare `set_operator: SetOperator = SetOperator.UNION_ALL`
  on `StrategyNode`.
- Delete ~110 LOC. Gate: `tests/nodes/test_base_node.py`,
  `tests/generators/test_merge_node.py`,
  `tests/core/processing/test_discovery_validation.py` (drop the stub attr).

### 1.14 `whole_grain` plumbed through nine node classes for one MergeNode branch
- PHYSICAL. `whole_grain` param/forward/copy in base_node L190/213/658 and
  filter/window/unnest/union/subselect/recursive/group/select(x2) nodes;
  sole reader `merge_node.py` L864; sole writer `strategy_builder.py` L4030.
- `whole_grain=True` 0/4184 constructions and 0/902 MergeNode resolves on
  all corpora; no test references it (`deduplicate_to_grain=False` is pinned
  only at contract level in `test_v4_group_behaviors.py` L1598).
- Make it a MergeNode-only kwarg; remove from base + 8 subclasses (~26 LOC).
  Same files: `WindowNode/SubselectNode/RecursiveNode/UnionNode._resolve`
  are literal `return super()._resolve()` (14 LOC); `RowsetNode.copy`
  duplicates `SelectNode.copy` verbatim (22 LOC; use `type(self)(...)` as
  `MergeNode.copy` does); `RecursiveNode` carries the UnionNode docstring.
  Total ~60 LOC.

### 1.15 `island_rowsets_for_weak_merge` is unreachable
- PHYSICAL. `processing/rowset_islanding.py` L57-96, plus `extract_address`
  L46-47 (duplicate of `select_helpers/datasource_nodes.py` L58) and
  `ROWSET_HUB_PREFIX` L42; module docstring L22-26.
- Only reference is its own docstring; `discovery_utility.py` imports only
  the connectivity pair. Survived "Drop V3 Engine (#632)" after the
  weak-merge search graph was removed.
- Delete ~50 LOC; rewrite the docstring. Note
  `island_rowsets_for_connectivity` also fired 0/266 (every production
  caller passes `island_rowsets=False`; reached only from the post-failure
  diagnostics path): keep, corpus-silent.

### 1.16 `_gate_nullable_by_host` has zero SQL effect
- PHYSICAL. `join_resolution.py` L1318-1366 + ~20 lines of call-site
  arguments at L1580-1607.
- Strips NULLABLE 3 times, all tl:adhoc04; ablation 0 changed / 235.
- Delete ~70 LOC. Gate: corpus, join_matrix, `tests/modeling/thelook_duckdb`.

### 1.17 `_prune_unused_single_row_parents` (predicate_pushdown) never fires
- OPTIMIZED. `optimizations/predicate_pushdown.py` L1007-1039.
- 0 fires / 235. It repairs a planner shape (`INNER JOIN <scalar> ON 1=1`
  nothing reads) that `_drop_constant_only_parents` /
  `_fold_constant_parents` already prevent. Pinned only by one reference in
  `tests/optimization/test_optimization.py`.
- Delete ~33 LOC; retarget or drop the test cell. Owner: LOGICAL.

### 1.18 Zero-caller functions and dead branches (bundle)
All grep-verified, all HIGH confidence, low individual value:
- `strategy_builder.py` L2348-2366 `_satisfiable_outputs` (the caller uses
  `projection.satisfiable_outputs` directly). 19 LOC. Active work.
- `network_model.py` L419-423 `SourceNetwork.join_partners` /
  `functional_partners` (callers read `_partners()`). 6 LOC.
- `concept_strategies_v4.py` L345 `_build_from_graph(... depth ...)`:
  parameter never read.
- `source_planning.py` L133-150 `_concepts_in_graph`: 0 callers. 18 LOC.
- `aggregate_rollup.py` L174-184 `filter_is_group_level`: 0 callers;
  L14-17 `_concept_lookup` is `dict.get`. ~15 LOC.
- `group_node.py` L73-83 `GroupNode.check_if_required`: classmethod
  indirection with a local import and one caller; call
  `check_if_group_required` directly. ~10 LOC.
- `processing/utility.py` L308-311 `sort_select_output` is a one-line alias
  of `sort_select_output_processed` (2 callers). 4 LOC.
- `select_merge_node.py` L561-572 `_conditions_deferrable_to_merge` (alias
  of `_conditions_can_be_sourced_by_components`, whose `concepts` param
  L520 is unused), L624-630 and L694-698 (aliases of
  `_condition_can_apply_after_node_merge`). ~25 LOC.
- `select_helpers/datasource_nodes.py` L537-553 `create_union_datasource`:
  zero production callers; only `tests/modeling/ncaa/test_ncaa.py:36` via
  the `__all__` re-export in `select_merge_node.py` L81-92. Repoint the test
  to `create_union_datasource_candidate`, drop wrapper + re-export. ~25 LOC.
- `node_generators/common.py` L116 and L140 define `LOGGER_PREFIX` twice.
- `query_processor.py` L365-371 `except IndexError` around `next(generator)`
  (raises `StopIteration`; unreachable): use `next(..., None)` + explicit
  raise. L383 `counts[vx] = counts[vx]` no-op. L359 `if joins and
  len(joins) > 0` redundant. L240-245 unreachable branch (loop at L238-239
  already sets `source_map[qdk] = []` when any `UnnestJoin` is present).
  L514-515 `if cte.grain != query_datasource.grain: raise` cannot fire
  (grain passed straight through; `CTE.__post_init__` does not touch it).
  L740-744 second `_raise_if_disconnected` call with identical arguments to
  the pre-gate at L710 (`_component_map` is pure in its inputs; 135 calls /
  134 statements). ~30 LOC.
- `strategy_builder.py` L927-934 `_same_relation` and L1850-1854
  `_resolved_grain` swallow `Exception` from `resolve()`: exception path
  0/194. `_deep_copy_node` fallback (L254-259, L397-398) 0/194 top-level
  calls. `_drop_constant_only_parents` L878-892 duplicates `_is_constant_only`
  L921-924 and never drops (0/194) while `_fold_constant_parents` drops
  12/8q; unify `_pre_merge_parents` on `_fold_constant_parents` (MEDIUM: no
  corpus query exercises the drop). Active work.
- `dialect/base.py`: `UnnestMode.CROSS_JOIN` / `CROSS_JOIN_ALIAS` (enums.py
  L22/24, common.py L41-48/96-101, base.py L2935-2940/2990-2997) no dialect
  uses (`UNNEST_MODE =` grep: CROSS_APPLY, CROSS_JOIN_UNNEST, DIRECT, PRESTO,
  SNOWFLAKE only); `BigqueryDialect.render_simple_case` method L450-483 has
  0 callers (the module-level function of the same name is what
  `FUNCTION_MAP` references; if BigQuery truly rejects simple CASE, wire the
  method in instead of deleting). ~50 LOC.
- `core/models/execute.py`: `CTE.base` L123 (never set, never read),
  `CTE.sourced_concepts` L742-744 (0 callers), `UnnestJoin.rendering_required`
  L949 (0 readers), `CTE.source_key_for` BuildDatasource/QueryDatasource arms
  L800-813 and `UnionCTE.source_key_for` L1838-1841 (all 17 call sites pass
  `str` or `CTE|UnionCTE`; 0 hits), `UnionCTE.inlined_alias_map` /
  `resolve_render_alias` L1786-1791 (only reachable from the dead arms),
  `QueryDatasource.get_alias(use_raw_name, force_alias)` params L1474-1484
  (sole caller passes only `source=`; both overwritten inside the loop),
  `Join.quote` L1926 (side channel set by `common.render_join:360`; pass
  `quote_character` to `reference_for` instead), `CTE.__add__` merges
  `partial_concepts` twice (L311-313 and L327-329), `FunctionType.NOOP`
  entry `base.py:589` (parser never produces it), `duckdb.zip_vals`
  L113-114, `snowflake.ENV_SNOWFLAKE_*` L10-12. ~55 LOC. Gate: mypy,
  `tests/test_execute_models.py`, `tests/test_coalesce_duplicate_cte_joins.py`.
- `dialect/base.py` `safe_get_cte_value` L803-877: make it a method; delete
  the `_format` RawColumnExpr/Function arms L815-818 (0 hits; `_render_concept_sql`
  L1647-1652 handles them first), the `isinstance(raw, str)` arm L856-859
  (dead by type: `CTE.source_map: dict[str, list[str]]`), the
  `len(unique_renders)==1` arm L843-844 (0 hits), and the pre-computation at
  L1642-1646. ~30 LOC. `tests/test_execute_models.py` L813-830 pins the
  6-arg signature (update the two direct callers).
- Stale docs: `processing/VIRTUAL_UNNEST.md` describes a mechanism that does
  not exist (the real handling is `join_resolution._row_independent` +
  `discovery_utility._literal_derived`); `processing/nodes/README.md` ends
  mid-sentence. `model_ambiguity.py` L240-259 `AmbiguousModelPair` /
  `sweep_model` are test-only (keep only if a CLI is planned).

### 1.19 Two optimizer refire phases that never change anything
- OPTIMIZED. `core/optimization.py` L749-772
  (`inline_datasource.after_join_upgrades`, reason cites tpc-h q3) and
  L550-559 (`merge_irrelevant_group_by.after_join_hoist`).
- Over 212 `optimize_ctes` runs: the first ran 134 times and changed 0,
  the second ran 2 times and changed 0; A/B dropping the first: 0/168 SQL
  diffs. tpc-h q3 now folds in the first inline pass
  (`_can_inline_filtered_parent` true 79x / 19 queries). A phase that takes
  no action cannot alter SQL (the post-phase
  `reorder_ctes(filter_irrelevant_ctes(...))` was measured idempotent).
- Delete both plan entries and the `upgrade_phases` tuple (~40 LOC). Only
  pin: the phase-name list in
  `tests/optimization/test_optimization_pipeline.py:147`. Gate: that test,
  tpc_h query03 golden, `tests/optimization/test_join_hoist.py`.
- Refire phases that DO change things and must stay:
  `collapse_single_parent.after_pushdown` (44 runs / 20 changed; dropping
  it changes 19 queries), `predicate_pushdown.after_final_upgrade` (100/6),
  `predicate_pushdown.after_union_dim` (16/6),
  `merge_irrelevant_group_by.after_predicate_remove` (44/2),
  `predicate_pushdown.remove.after_join_upgrades` (6/2; see 0.4).

### 1.20 `JoinHoist._lock_in_guarded_upgrades` is inert
- OPTIMIZED. `optimizations/join_hoist.py` L319-335 and L609-621, plus the
  `UpgradeJoinOnGuards` import at L58-61 (keep `_gather_proofs`).
- 6 calls on the corpus, 0 join-type changes (instrumented snapshot of
  `cte.joins` / `cte.source.joins` before and after); A/B 0/168 diffs; no
  test in `test_join_hoist.py` references lock-in or `UpgradeJoinOnGuards`.
  The regression it guarded (a filter-only RIGHT_OUTER to the hoist parent)
  is handled by `upgrade_join_on_guards.final`, which reads the hoisted
  predicate on the parent.
- Delete the method, the `locked_in_upgrades` flag and the re-plan
  (~25 LOC). Gate: `test_join_hoist.py`, tpc-ds q35/q69 (the only hoist
  firings), q73.

### 1.21 Dead driver code in `optimization.py` and rule state never read
- OPTIMIZED, each measured independently over 4586 `canonicalize_graph` /
  `filter_irrelevant_ctes` calls:
  - (a) commented-out `is_locally_irrelevant` L54-68 and L271-288 (33 lines
    of comments, no callers).
  - (b) `filter_irrelevant_ctes` recursion L329-331: reachability from the
    root does not change after removing unreachable nodes; the second pass
    removed more 0/4586. Replace with `return final`.
  - (c) `_grains_equivalent` pseudonym fallback L376-403: true 0/245 calls
    (the q39 case it cites now passes strict equality; q39 is still in the
    direct-return set). `test_optimization.py:564` asserts only the
    REJECTION. Keep `return direct_parent.grain == cte.grain`. MEDIUM
    (corpus-silent only).
  - (d) `canonicalize_graph` union re-alignment L153-190: triggered 0/4586;
    no test references re-alignment
    (`test_union_branch_projection_collision.py` pins the L139-142 collision
    guard, which stays). Its own comment calls it a fallback for an
    over-pruning rule bug. MEDIUM.
  - (e) `CollapseSingleParent.completed` (`collapse_single_parent.py` L451,
    L466-467) never populated (no `.add`); `PredicatePushdownRemove.complete`
    (`predicate_pushdown.py` L947, L1143) written, never read.
- ~110 LOC. Gate: `tests/optimization/test_optimization.py`,
  `test_missing_cte_reference.py`, `test_union_branch_projection_collision.py`,
  `test_union_dim_pushdown.py`, corpus.

---

## Tier 2: move ownership upstream, then delete the downstream repair

These are the findings that matter structurally. Each names the owning
stage, the upstream change, and what becomes deletable after it.

### 2.1 LEFT/RIGHT -> INNER narrowing belongs in the planner's existing proof path
- Today: LOGICAL `get_join_type` emits 703 LEFTs / 110 queries, 687 of
  them from the `nullable_one_side` branch (a `?`/nullable FK). OPTIMIZED
  `UpgradeJoinOnGuards` then flips 430 of them to INNER in 99 queries
  (BaseJoin 226, CTE Join 210), using a WHERE the planner itself placed
  (`UPSTREAM_MOST` on the same node in 302 cases). The planner already owns
  the proof machinery (`grain_utility.non_null_proofs` L89-116,
  `downgrade_join_for_proofs` L356-387, called from `merge_node._resolve`
  L769-806) but applies it only to FULL.
- Prototype (scratchpad, ~30 LOC in `downgrade_join_for_proofs`, same
  partial/opaque guards as the optimizer): v1 (node's own condition via
  `non_null_proofs`) removes 214/430 optimizer firings with 7 statements
  changed, all benign (ds:02 drops a now-redundant WHERE; ds:24/54/81 swap
  column source to the INNER side; ds:64 GROUP BY coalesce collapses; ds:50,
  ds:66). v3 (adds `_gather_proofs`/`_gather_or_groups`: IS NOT NULL,
  BETWEEN, OR-of-ANDs as in q13/q47) removes 386/430 (rule total 264 -> 83)
  with 29 changed and 2 regressions (ds:23, ds:35: FULL narrowed to RIGHT
  instead of INNER because the constant-bound `sales_channel` is opaque on
  the left). Conclusion: FULL keeps the existing planner logic; only
  LEFT/RIGHT take the full harvest.
- What the optimizer keeps (legitimately post-multi-node): cross-CTE
  consumer proofs `_external_forced_map` (38 firings / 22 queries),
  `_narrow_directionally` (the documented preserving-render contract,
  docs/subset_union_join_design.md), `OrderInnerJoinsFirst`.
- Delete after: `join_upgrade._downgrade_base_join` L572-642 (71 LOC), the
  LEFT/RIGHT branches of `_downgrade` L557-568, the BaseJoin loop in
  `UpgradeJoinOnGuards.optimize` L947-955/982-1004 (~30 LOC); then measure
  `SimplifyNullSafeJoins` (`null_safe_join.py` L195-253, 11 fires / 9
  queries, exists only because join types change after planning and leave
  stale NULLABLE modifiers) and delete it if it reaches 0.
- Steps: (1) move `_partial_addresses`, `_opaque_binding_addresses`,
  `_gather_proofs`, `_gather_or_groups` from `join_upgrade.py` into
  `condition_utility.py` so both stages share one harvest; (2) extend
  `downgrade_join_for_proofs` to LEFT_OUTER (right side forced -> INNER) and
  RIGHT_OUTER (left side forced -> INNER) using side-only address sets minus
  partial/opaque; (3) widen the harvest for LEFT/RIGHT only; (4) delete the
  optimizer branches; (5) re-measure null_safe_join.
- Pins: `tests/optimization/test_join_upgrade.py`,
  `test_not_like_partial_join.py`, `tests/core/processing/test_grain_utility.py`,
  `test_join_padding_provenance.py`, `tests/join_matrix/*`,
  `tests/engine/test_duckdb_partial_fk_field_report.py` (q78 `is_returned`
  opaque case). Gate: corpus A/B changed set exactly the 7 above for v1;
  `UpgradeJoinOnGuards` LEFT->INNER count <= 216 (v1) then <= 44 (v3);
  TPC-DS 107 + TPC-H 29 row checks; fuzzer regressions file. Risk MEDIUM:
  the `?` value-null key under LEFT must not narrow (the
  `right_keys <= proofs` gate handles it). Confidence HIGH for step 2,
  MEDIUM for step 3.

### 2.2 Identity GROUP BY decided at the planner
- Today: OPTIMIZED `merge_irrelevant_group_by._clear_identity_group` L44-79
  (+ `_unique_at_declared_grain` L135-174, `_join_preserves_left_rows`
  L120-132, `_stacks_duplicate_rows`) fires 6 times / 5 queries (ds:18 x2,
  ds:02-one, ds:02-two, ds:50, tl:adhoc04); ablation 4 changed (+346 B of
  redundant `GROUP BY`). LOGICAL `discovery_utility.check_if_group_required`
  (L115+) and `merge_node._resolve` force_group (L848-878 via
  `grain_utility.grain_satisfied_by_pregrain` L410-435) lack the "source
  already unique at its declared grain" test.
- Change: move the three predicates into `grain_utility`; have
  `check_if_group_required` return required=False when `comp_grain ==
  target_grain` and every row-feeding parent is unique at its declared
  grain, and have `MergeNode._resolve` consult it before `force_group =
  True`. Then delete `_clear_identity_group` and its call (L273-278), ~90
  LOC net of the move.
- Pins: `tests/optimization/test_merge_irrelevant_group_by.py` (5 refs).
  Gate: `_clear_identity_group` count 0 on corpus with byte-identical SQL.
  Risk LOW-MEDIUM (UNION-stack parents must stay grouped). Confidence HIGH.

### 2.3 Render-time aggregate wrapping is a planner demand
- Today: RENDER `dialect/base.py` L1204-1205 + `_order_expr_needs_group_wrap`
  L1246-1270 + `_scalar_order_leaves` L1208-1244 wrap ORDER BY leaves in
  `MIN(...)` when the final is grouped (0 corpus hits; unit-pinned by
  `tests/engine/test_duckdb_orderby_derived_expr.py:86`, q49 shape);
  L1418-1426 wraps filter virtuals in `MAX(...)` coordinated with
  `execute.py` L559-590 `filter_collapses_to_grain` and L683 GROUP BY
  exclusion and `_aggregate_over_collapsed_filter` L267-280 (205 hits; live
  and load-bearing for q16 double-count; the L1561 branch has 0 hits but
  guards a real q95 nesting bug). Also `_all_grouped_outputs_are_passthrough`
  / `_has_local_aggregate` L2646-2716 (gates L1257-1260, L2842-2845) decide
  "this grouping node should not group" at render: 0 hits on every corpus
  (1106 evaluations all False), pinned by
  `tests/engine/test_duckdb_rollup_passthrough.py` (q05 family).
- Owner: LOGICAL. (a) ORDER BY leaves should be demanded from the group
  node; `query_processor._carry_order_by_concepts` L540-586 already exists
  for that. (b) A filter virtual whose keys are within the grain but whose
  predicate reads non-grain columns should be built with a
  `max(case ...)` aggregate lineage at build time, so `group_concepts` and
  the rendered column agree by construction instead of two files
  coordinating via `filter_collapses_to_grain`. (c) A passthrough group
  should not be emitted as `group_to_grain` (`QueryDatasource.force_group` /
  `group_required`).
- Delete after: ~110 LOC in base.py, ~30 in execute.py, ~70 for the
  passthrough gate. Pins: q16/q49/q95/q05 rows,
  `tests/test_filtered_count_at_regroup_grain.py`,
  `tests/test_filter_cte_grouped_metric_projection.py`,
  `tests/test_rollup_multi_window.py`. Risk MEDIUM-HIGH (semantics).
  Confidence MEDIUM (design-level; counts verified). Do (a) first, it is
  small and fully pinned.

### 2.4 One passthrough-elision mechanism
- Today: three. LOGICAL `_elide_single_parent_passthrough` (+ tree, item
  1.7, 7 fires / 7 queries); OPTIMIZED `optimization.is_direct_return_eligible`
  L406-476 + `_grains_equivalent` L367-403 + `pass_up_metadata` L479-501 +
  loop L965-972 (36 fires / 36 queries; `CollapseSingleParent` absorbs 20 of
  them when it is off: 561 -> 586); OPTIMIZED `CollapseSingleParent`
  PASSTHROUGH (42 / 27 queries).
- Owner: `CollapseSingleParent` (it sees post-pushdown passthroughs; phase
  `collapse_single_parent.after_pushdown` L645-664).
- Change for the 131 direct-return LOC: let CollapseSingleParent accept the
  root CTE as child when its only novelty is ORDER BY/LIMIT
  (`apply_child_merge` L366-370 already carries them); investigate why the
  16 residual roots (ds:21,31,39,47,49,51,57,59,73,75,78 ncaa:adhoc03,08
  gcat:inline13,29 the_look:inline01) are declined (`base_alias !=
  parent.safe_identifier` or `passthrough_renders_from_parent`).
- Pins: `tests/optimization/test_collapse_basic_into_group.py`,
  `test_merge_basic.py`, `tests/complex/test_rowset.py`; direct return is
  pinned implicitly by goldens (`tests/modeling/*/zquery*.log`). Gate: 0
  changed after the move. Prerequisite: 0.2. Confidence MEDIUM.

### 2.5 Keyless FULL narrowing at the planner
- Today: OPTIMIZED `keyless_full_join.py` (131 LOC, flag
  `narrow_keyless_full_joins`, registered twice, item 0.3) fires 3 times
  (gcat:inline31 x2 FULL->INNER, h:adhoc05 FULL->INNER, ds:query-1
  FULL->LEFT). LOGICAL already knows single-row sides
  (`join_resolution._row_independent` L1144-1171, `resolve_join_order_v2`
  solo path L799-834, `merge_node.create_full_joins` L382-401).
- Change: when emitting a keyless join where one side is an ungrouped
  aggregate (abstract grain, no HAVING/limit), emit INNER if both are
  single-row, LEFT/RIGHT toward the single-row side otherwise; keep FULL only
  when both may be empty. The `_raise_if_keyless_row_bearing_join` guard
  stays. Then delete the rule, flag and both registrations.
- Pins: `tests/optimization/test_keyless_full_join.py` (6 refs, by flag;
  repoint at planner output). Gate: corpus byte-identical with the rule
  off. Risk LOW. Confidence MEDIUM.

### 2.6 Host WHERE atoms on the lowest node of their group
- Today: LOGICAL hosts atoms on a group's top SELECT/GROUP node;
  OPTIMIZED `PredicatePushdown._check_parent` (L568-752) then moves 101 of
  its 142 pushes into that group's own merge (71 / 32 queries: ds:64 x19,
  hackernews:adhoc07 x7, ds:10, 69, 77 x6 ...) or root scan (30 / 11
  queries). `PredicatePushdownRemove` (L944-1144, 201 LOC) then dedups 79
  copies in 46 queries, 39 of which are pushdown's own copies and the rest
  the planner's deliberate ones (conjunction_recompute 59, stage_precondition
  27, final_* 21).
- Change: generalize `strategy_builder._push_row_condition_before_group`
  L3366-3396 (today FINAL-only) via `condition_injection.inject_condition_at_node`:
  when an atom's row args are all outputs of the group's parent scan/merge
  and the group is a GroupNode/SelectNode wrapper, inject on the parent.
  Mirror the `_predicate_safe_past_windows` / `_predicate_safe_past_grouping`
  gates. `_nested_scope_swallows_atom` semantics are unchanged (the group
  does not change, only the node within it).
- Keep in the optimizer: `_push_having_into_group_parent` (42 / 25 queries),
  union branch push/prune (15 + 7; the prune is knowable at union source
  selection, low priority), semi-join mirror, filtered aggregate/count
  rewrites.
- Pins: `tests/optimization/test_optimization.py` (18 refs),
  `test_pushdown_optimization.py`, `test_existence_feeder_pushdown.py`,
  `test_having_below_window_pushdown.py`, `tests/modeling/usa_names`. Gate:
  corpus byte-identical; `_check_parent into:{merge,root_scan}` count drop;
  `predicate_pushdown` off changed-count drops from 71. Risk MEDIUM (no
  prototype run). Confidence MEDIUM.

### 2.7 Resolve-time repair machinery that never mutates anything
- PHYSICAL, lens "late re-injection". Counts over 902 MergeNode / 1053
  StrategyNode / 750 GroupNode resolves on base+extended corpora:
  `merge_node._inject_scoped_join_key_exposure` L492-536 (mutates PARENT
  nodes at resolve) changed a parent 0/902, helpers `_feeds_only_existence`
  0/795 True, `_renders_nonstandard_grouping` 0/179 True,
  `_splits_aggregate_groups` L95-123 never called but referenced by
  `tests/join_matrix/test_global_aggregate_broadcast_matrix.py` (q23/q59
  family: corpus-silent, unit-pinned); `base_node._repoint_feeder_only_rows`
  L496-544 0/1053, no test reference; `merge_node` `existence_only_rows`
  drop L920-933 0/902; `group_node` non-scalar-condition wrapper + existence
  relocation L232-289 0/750.
- Owner: LOGICAL obligations already exist for these
  (`v4_node_generators/rowset.py` L273-362 key/probe/derived-member
  obligations, `condition_sources.py` L108-119 feeder output slicing,
  `strategy_builder.py` L3155 `unhide_output_concepts` at assembly).
- Steps: (1) run the same counters under `tests/join_matrix` +
  `test_non_benchmark_queries.py` (the corpora lack scoped `union join` over
  rowsets, the q59 shape); (2) delete `_repoint_feeder_only_rows`,
  `existence_only_rows`, and the GroupNode existence relocation if still 0;
  (3) if `_inject_scoped_join_key_exposure` fires under the suite, move
  that case into the rowset/strategy_builder obligation pass and delete the
  resolve-time mutation. ~130 LOC. Keep `deduplicate_nodes` (7/806) and the
  MergeNode self-drop (40/902). Confidence MEDIUM.

### 2.8 Authored-pair dominated-source pruning at selection time duplicates discovery
- PHYSICAL. `node_generators/select_merge_node.py` L95-152
  (`_authored_key_scope`, `_authored_join_pairs_enforceable`) and
  `select_helpers/source_scoring.py` L256-348 `prune_dominated_datasources`
  (41 stmts), gated on `relevant_authored_join_pairs(...)` at L257 which was
  non-empty 0/77 on base+extended; the helpers never called; no unit test
  references `prune_dominated_datasources`. The same concern is handled at
  LOGICAL by `source_planning.inject_authored_join_key_terminals` (2/423)
  and `network_build.relevant_authored_join_pairs` (7/472).
- Steps: instrument under the unit suite (authored joins live in
  gcat/thelook shapes: `tests/discovery/test_authored_join_terminal_injection.py`,
  `tests/generators/test_select_node_generator.py`, join_matrix); delete
  (~110 LOC) if 0. Confidence MEDIUM (corpus-silent, no unit pin).

---

## Tier 3: consolidation (one owner, same behaviour)

### 3.1 One SQL_TEMPLATE instead of ten
- RENDER. `dialect/base.py` L764-788 GENERIC_SQL_TEMPLATE and the nine
  per-dialect templates (duckdb L330-365, bigquery L176-205, sqlite
  L165-195, mysql L135-164, postgres L42-72, presto L51-80, snowflake
  L58-88, sql_server L33-62, clickhouse L183-208); consumers `render_cte`
  L3106-3129 and `_render_query` L3791-3799.
- Differences are four flags the class already carries or can:
  `SUPPORTS_QUALIFY`, `LIMIT_STYLE` (LIMIT/TOP), `QUOTE_CTE_NAMES`
  (snowflake), `RECURSIVE_KEYWORD` (postgres/presto/sql_server omit it).
  The `grain=` kwarg is referenced by no template; `comment` should be
  passed only under `CONFIG.show_comments`. Fixes 0.1 by construction.
- ~230 of ~300 template lines. Gate: per-dialect corpus byte-diff (134 x 9;
  whitespace-only differences acceptable, goldens may need regeneration);
  `tests/dialect/`, `tests/persistence/`, `tests/engine/test_bigquery.py`,
  `tests/engine/test_mysql.py`; new all-dialect persist render test.
  Confidence HIGH.

### 3.2 Byte-identical per-dialect FUNCTION_MAP overrides and nine hand-merged FUNCTION_GRAIN_MATCH_MAPs
- RENDER. duckdb.py 14 entries (CAST, COUNT, SUM, AVG, LENGTH, INDEX_ACCESS,
  ARRAY_AGG, CONCAT_STRICT, DATE_LITERAL, DATETIME_LITERAL, GEO_*), sqlite 8,
  bigquery 5, presto 5, snowflake 4, sql_server 4, clickhouse 1: output
  proven equal to base on 6 sample arg shapes. Every dialect writes
  `FUNCTION_GRAIN_MATCH_MAP = {**FUNCTION_MAP, **AGGREGATE_GRAIN_MATCH_MAP}`
  plus a class-level re-merge; all nine equal that formula.
- Delete the identical entries; compute `FUNCTION_GRAIN_MATCH_MAP` once in
  `BaseDialect.__init_subclass__`. ~75 LOC. Pins:
  `tests/dialect/test_grain_match_map.py` (checks the grain-match entry is
  not the same object as the FUNCTION_MAP one; must keep passing),
  `tests/test_dialect_function_maps.py`. Confidence HIGH.

### 3.3 Render-time WHERE/HAVING/QUALIFY classification and GROUP BY text-dedup
- RENDER. `base.py` L3040-3080 decomposes `cte.condition` with
  `is_scalar_condition`/`contains_window` (slow path 126 hits, HAVING
  placement 196); L2871-2879 drops group keys whose rendered SQL is
  identical (289 hits, q39 `isk1/isk2`). The same classification already
  exists at OPTIMIZED (`join_hoist.py` L204-223, `graph_models.py` L276-279,
  `predicate_pushdown.py` L410-413) and the HAVING-alias decision is already
  threaded into the plan (`query_processor.py` L1485, `optimization.py`
  L509); the `rollup_addresses` special case at L3045-3069 is a symptom of
  two sources of truth.
- Change: give `CTE` explicit `where`/`having`/`qualify` populated by the
  optimizer; dedupe `CTE.group_concepts` (execute.py L593-724) by source
  binding, not by rendered text. ~45 LOC leave base.py; net repo-wide ~0;
  the gain is one owner. Gate: nine-dialect corpus byte-diff (MySQL and
  DuckDB exercise it most), `tests/test_having_*.py`,
  `tests/test_membership_having_*.py`, `tests/test_rollup_multi_window.py`.
  Confidence MEDIUM.

### 3.4 `common._render_left_concept` stale-alias re-pins never fire
- RENDER. `dialect/common.py` L126-151 (self-reference pin) and L152-197
  (dangling-node pin): 0 hits everywhere incl. 590 test queries; provenance
  #577 and #604; both describe an optimizer (inline/merge/hoist) leaving
  `Join.joinkey_pairs[].cte` pointing at a node no longer in FROM scope.
  `left_is_local` handling L198-218 fires 6x and stays.
- Replace both blocks with one assertion (`node.name in
  consumer.from_scope_aliases()`) raising a clear error; run the gate; delete
  the assertion after a release. If any test fires it, fix the emitting rule
  (`CTE.replace_dependency`, `inline_parent_datasource`, `join_hoist.py`,
  `union_dim_pushdown.py`), do not re-pin. ~70 LOC. Gate: nine-dialect
  corpus, `tests/join_matrix/`, `tests/test_scoped_join.py`,
  `tests/optimization/test_union_dim_pushdown.py`,
  `tests/optimization/test_join_upgrade.py` (join_matrix fixtures were not
  harvested by the sweep). Confidence MEDIUM.

### 3.5 Four resolve-time join re-typers between `get_join_type` and the optimizer
- PHYSICAL. `join_resolution.get_join_type` L307-487 decides; then
  `ensure_content_preservation` L504-564 (changed 134/841),
  `reduce_join_types` L490-501 (11/1515, 2 queries),
  `merge_node.downgrade_join_for_condition` L769-770 (121/1689, 19 queries),
  `downgrade_join_for_proofs` branch-proof pass L804-806 (31/87),
  `_tighten_joins_for_filtered_branches` L538-608 (37/753, 16 queries); then
  the optimizer re-decides again (2.1). `get_join_type[non-INNER]` is
  1102/1693, i.e. most typing is re-evaluated by up to three passes each
  taking a different input (condition, branch proofs, preexisting atoms)
  the first pass could have been given.
- Change: compute `branch_proofs` / `filtered_ids` before `get_node_joins`
  and pass them into `get_join_type`; fold `_tighten_joins_for_filtered_branches`
  and both `downgrade_join_for_*` into it; keep `ensure_content_preservation`
  as the only ordering-dependent fixup. Net ~0-40 LOC; value is one decision
  site and a clean place for 2.1 to land. Gate: corpus byte-identical,
  gcat/thelook CI, join_matrix, `tests/optimization/test_join_upgrade.py`.
  Risk HIGH sensitivity. Confidence MEDIUM. Do 2.1 first or together.

### 3.6 Two truths for partial/nullable on the same scan
- PHYSICAL. `select_node_v2.resolve_from_provided_datasource` L140-153
  re-adds ALL column partials/nullables (QDS partials > node partials
  403/2133 resolves in 44 queries; nullables 684/2133 in 113) while
  `select_helpers/datasource_nodes.py` L417-443 stamps a filtered node-level
  list (`partial_is_full` / `membership_complete` / `proven_non_null`).
  Consumers reading `node.partial_concepts` (validate_stack, discovery) and
  `qds.partial_concepts` (join typing) see different sets for the same scan.
  Condition non-null stripping is done at four sites (base_node L227/320-347
  construction, L598-601 resolve, group_node L145-150, datasource_nodes
  L379-381) and `resolve()` syncs back 1278/6465 times.
  `membership_complete_grain_keys` returned non-empty 0/2872 at both sites
  (pinned by `tests/engine/test_duckdb_return_only_anchor_elision.py`).
- Change: make `resolve_from_provided_datasource` honour the node-level
  stamps (same columns, already computed), then drop the construction-time
  `_refine_nullable_for_conditions` (370 strips / 90 queries; ablation 1
  changed: ds:64 -86 B) once the resolve-time strip is the only one. ~40
  LOC. Gate: join_matrix, that anchor-elision test, corpus,
  `tests/modeling/test_nullability.py`; q78/q51/q86 notes in the comments.
  Confidence MEDIUM.

### 3.7 Union-find five times; `_uf_find/_uf_union` is a byte-copy
- LOGICAL. `source_planning.py` L1491-1502 duplicates `network_model.py`
  L606-619 (`find`/`union`, same dict-parent semantics, same tie-break);
  `group_rules.py` L422-426, L568-577, L926-935 are three inline int-array
  closures.
- Import `find, union` in source_planning; add one
  `_components(n, related) -> list[list[int]]` helper in group_rules used by
  `_partition_grouped_aggregates`, `_cosource_component_groups`,
  `_partition_by_signature_and_grain`. ~37 LOC. Bucket ORDER must stay
  deterministic (group ids are embedded in CTE names, so any slip shows as
  a byte diff). Confidence HIGH / MEDIUM (group_rules).

### 3.8 `node_generators/common.py` helpers with one consumer; v4-only helpers in the legacy package
- PHYSICAL. `prune_and_merge` L119-137, `is_ds_node`, `build_ds_column_index`,
  `iter_unique_ds_pairs`, `get_concept_node_cached`, `existing_join_addresses`,
  `injectable_concepts` L148-231 each have exactly one caller,
  `reinject_common_join_keys_v2` L447-510 (fires 42/77 from
  `select_merge_node`, 0/379 from `source_planning.py` L564: the v4 Steiner
  bridge never needs it). `_walk_aggregate_grain_inputs` L54-113 and
  `_union_key_siblings` L30-51 are imported only by
  `v4_helper/concept_graph.py` (the latter never called, no test).
- Inline the helpers; move the two v4 helpers into `concept_graph.py`;
  instrument the `source_planning` L564 call under the unit suite before
  removing it. ~60 LOC + ~90 relocated. Pin:
  `tests/core/processing/test_join_concept_injection.py`.

### 3.9 `parent_for_consumer` plans a sliced ROOT it discards 86% of the time
- LOGICAL. `strategy_builder.py` L769-836. **Touches active work.**
- 131 speculative `build_node`/`plan_source` calls across 99/134 queries,
  18 adopted in 14 queries (gcat 25 -> 4, thelook 13 -> 2); 1.83 s of a 45 s
  corpus render. Disabling leaf-set adoption changes 6 queries (ds:02-one,
  02-two, 18, 24, tl:query19, adhoc04), so the adopted slices are real.
- Compute `carries_wrong_side` before building and only plan when it is
  false AND the slice's outputs are bindable by a strict subset of the
  node's leaf datasources (`_leaf_datasource_ids(node)` vs datasource
  columns); otherwise `node.copy()`. Alternatively route the discarded
  result through `history.build_history` (23 hits / 225 misses today).
  Gate: 0 changed; q94/q02/q18/q24.

### 3.10 `environment` / `mandatory_list` optionality threaded through ~27 guard sites for one caller
- LOGICAL. `group_graph.py` (21 sites) and `condition_placement.py` (8
  sites); `build_group_graph` has one production caller
  (`concept_strategies_v4._build_from_graph`) that always passes both; every
  `environment is None` arm is never executed. Tests call inner functions
  without an environment (12 call sites across `test_v4_concept_sets.py`,
  `test_v4_group_behaviors.py`, `test_v4_condition_placement.py`).
  **Touches active work heavily; schedule after the branch lands.**
- Make both required on `build_group_graph`, `_compute_concept_sets`,
  `_refresh_final_contract`, `_final_merge_grain`,
  `_group_final_grain_contribution`, `_lineage_pinned_grain`,
  `_rollup_padded_addresses`, `_scoped_axis_mates`,
  `_split_strands_condition_scan`, `_widen_mixed_scalar_basic_to_final_spine`,
  `plan_condition_placements`; give tests a shared empty `BuildEnvironment()`
  fixture. Keep `if mandatory_list` where the list can legitimately be
  empty; drop only the `is not None` forms. ~45 LOC. Related small items
  (same files): `return_merged_graph` overloads on `build_group_graph`
  (L2887-2915, L3124-3126; single caller always True; ~33 LOC);
  `GroupIOPlan.hidden` / `GroupAttrs.hidden_concepts` never written
  (group_graph L2169/2177/2883, models.py L147/273, strategy_builder
  L4117-4128 reads it, `test_v4_concept_sets.py` L406-435 asserts it is
  `()`; ~12 src + 30 test LOC); ROOT `Behavior` entry
  (`group_behaviors.py` L72-80, L156-167, L268-272) unreachable by
  construction because `_compute_concept_sets` L2580 routes ROOT through the
  scan path first, `GroupFacts.behavior: Behavior | None` only None for
  FINAL which is `continue`d at L2576, `Behavior.derivation` read only by a
  test (~35 src + 20 test LOC); `_relation_side_partitions` (group_rules
  L486-504) duplicates `_cosource_component_groups` (L608-622) property-key
  adjacency (~15 LOC; corpus-silent, pinned by join_matrix computed-member
  cells and `tests/test_scoped_join_expression_keys.py`).

### 3.11 InlineDatasource's one-candidate-per-visit early return multiplies driver loops
- OPTIMIZED. `optimizations/inline_datasource.py` L206-211 (`return True,
  None` after registering the first candidate); `constant_inline_cutoff`
  check L212-219.
- 1065 fires for 771 real inlines (294 are no-op "candidate registration"
  returns); phase loop histogram {1:22, 3:18, 4:48, 5:61, 6:31, 7:17, 8:8,
  9:2, 10:4, 11:1} = 1011 loops, each a `gen_inverse_map` + full CTE visit,
  while every other phase converges in <= 3. A/B registering all candidates
  per visit: 0/168 SQL diffs, fires 1065 -> 592, loops {1:22, 3:186, 5:4}.
  `constant_inline_cutoff` never trips on the corpus but is unit-pinned
  (`test_inlining.py::test_inline_datasource_respects_cutoff`).
- Change ~5 lines (`continue` + a `registered` flag, or a one-pass
  reference count over `inverse_map`). Also lower `MAX_OPTIMIZATION_LOOPS`
  (L39) from 100 to ~10 and log if hit (never approached). Gate:
  `test_inlining.py`, corpus.

### 3.12 `depends_on` / `reason` / `_enabled_dependencies` are documentation, not control flow
- OPTIMIZED. `optimization.py` L504-506 and every `depends_on=` /
  `reason=` kwarg (94 + 53 LOC across 27 plan entries). `optimize_ctes`
  reads only `refires_after` (L989); `.depends_on` is consumed only by
  `log_optimization_rule_plan` (L934, L938).
  `tests/optimization/test_optimization_pipeline.py` asserts the declared
  tuples (L70, L90-94, L117-132, L172-175).
- Maintainer call: ~150 LOC of declared-but-unenforced ordering metadata.
  Either delete the fields and fold each `reason` into a one-line comment
  (update the pipeline test to assert order only), or make the driver
  actually enforce `depends_on` (assert each dependency's phase index is
  lower) so the metadata earns its keep.

### 3.13 `value_set_join_upgrade`: the EQUAL-equivalence path is (nearly) dead
- OPTIMIZED. `_upgrade_to_inner` L1014-1100, `_pair_key_sets_equivalent`
  L375-391, `_filters_equivalent`, `_equal_intersection_complete`
  L471-511, `_complete_via_preserved_base` L514-553, `_pair_equal_declared`,
  `allow_scan_evidence` plumbing, flag `narrow_equal_domain_joins` (~200
  LOC). The module docstring L1-38 presents `_upgrade_to_inner` as the
  rule's purpose; 78 of 79 upgrades come from `_narrow_directionally`.
- Two independent sweeps: 0 true / 235 statements with
  `narrow_equal_domain_joins` off changing 0 statements (cross-stage
  sweep); 1 true / 286 calls (ds:89 key-set case) in the optimizer sweep
  over a slightly different corpus. `_equal_intersection_complete` 1 call /
  0 true, `_complete_via_preserved_base` 8 / 0. The planner's
  `nullable_both -> INNER` branch (83 fires) already produces the
  twin-rollup INNER the docstring cites for q12/q20/q63/q89/q98.
- CAUTION: the EQUAL-declared exemption is a settled contract (see the
  two-fact extent contract memory and docs/subset_union_join_design.md);
  `tests/join_matrix/test_narrowing_matrix.py:64,72` toggles the flag and
  is the only path reaching `_equal_intersection_complete`; tests encode
  shapes the corpus lacks. Before deleting anything, instrument
  `_upgrade_to_inner` under `tests/join_matrix/`,
  `tests/optimization/test_value_set_join_upgrade.py` (16 refs),
  `test_join_grain_contract.py`, `tests/core/processing/test_join_padding_provenance.py`,
  `tests/engine/test_duckdb.py`, `tests/nodes/utility/test_joins.py`, and
  confirm each firing cell's join type is produced by the planner. Minimum
  safe step: delete the two completeness helpers (their call sites at L430
  and L1041-1047 reduce to `_complete_values`, ~85 LOC) and rewrite the
  docstring around directional narrowing. Confidence MEDIUM.

### 3.14 Duplicated helpers across optimizer rules
- OPTIMIZED, ~80 LOC. "is grouped" x4 (`merge_irrelevant_group_by.py`
  L40-41, `collapse_single_parent.py` L268-269, `join_hoist.py` L92-93,
  `semi_join_pushdown.py` L64-65); `{WINDOW, UNNEST, RECURSIVE}` x4
  (`optimization.py` L360-364, `collapse_single_parent.py` L34-38,
  `merge_irrelevant_group_by.py` L33-37, `join_upgrade.py` L75);
  `_OUTER_JOIN_TYPES` x3 (`join_upgrade.py` L70, `value_set_join_upgrade.py`
  L57, `predicate_pushdown.py` L40) duplicating `join_resolution.py` L54
  `OUTER_JOIN_TYPES`; reachability walkers x3 (`predicate_pushdown.py`
  L47-59, `union_dim_pushdown.py` L195-209, `semi_join_pushdown.py`
  L113-124); "which parent does this CTE's outer join NULL-pad" x4
  (`predicate_pushdown.py` L219-242, L122-139, L62-104; `null_safe_join.py`
  L45-91; `value_set_join_upgrade.py` L298-325 positional variant);
  `_equivalent_addrs` / `_equivalent_addresses` (`null_safe_join.py` L38-42,
  `strip_redundant_not_null.py` L50-54); `_base_datasource` (`join_upgrade.py`
  L78-83, `union_dim_pushdown.py` L73-78); existence self-reference guard and
  nullable/existence-map carry duplicated between `collapse_single_parent.py`
  L337-348 / L546-557 and `merge_irrelevant_group_by.py` L314-325 / L376-382;
  one-line wrappers (`join_hoist.py` L96-104, `predicate_pushdown.py` L43-44,
  L983-984, `utils.py` L31-33, L94-95, `value_set_join_upgrade.py` L340-341).
- Consolidate into `optimizations/utils.py` (and import `OUTER_JOIN_TYPES`
  from join_resolution). Gate: full `tests/optimization` + join_matrix +
  corpus.

---

## Not deletable (verified; do not re-chase)

Corpus-silent but pinned by unit tests or extra corpora:

- LOGICAL: `concept_graph.resolve_alternatives` + hub helpers
  (`test_v4_alternative_resolution.py`, `test_v4_pseudonym_multi_origin.py`);
  `group_graph._fold_rollup_key_dims` (`test_v4_rollup_key_dim_fold.py`);
  `_bound_column_components` / `_split_strands_condition_scan` (gcat);
  `group_rules._lineage_layers` (`tests/engine/test_duckdb_filter.py:905`);
  `partition_roots` single_row/multi arms (gcat/faa);
  `condition_placement._post_aggregation_producers` global-gate pin
  (`tests/discovery/test_outer_where_pushes_into_global_agg.py`,
  `test_global_avg_filter_group_fanout.py`); `source_planning`
  `_plan_coalescing_axis`, `_plan_complete_where_source`,
  `_plan_finer_filter_rollup`, `_cross_component_source`,
  `_datasource_rolls_up_to`, `_derived_connector_nodes`, gap-fill L636-650
  (`test_v4_agg_source_planning.py`, `test_v4_source_planning.py`,
  join_matrix, hackernews recursive); `_complete_partial_requested` success
  path (gcat); `then where` stage labels (`tests/test_then_where*.py`);
  RELATION-edge paths (join_matrix computed-member cells); `_seed_cover`
  (`test_non_benchmark_queries.py:1118`, `test_v4_network_search.py:903`);
  `_fold_covered_contributors` (tl:adhoc04 changes when off); `_decomposable`
  (0 drops / 194, no unit pin: deletion would rest on the full suite only);
  `_carry_order_by_concepts` + the ORDER BY carry/unhide block in
  `_plan_query_node` (pinned by `tests/engine/test_duckdb_rowset.py:697,1127`
  and `test_duckdb_orderby_derived_expr.py`; two stages own ORDER BY carry
  but neither is provably removable until 2.3(a)); the materialized-roots
  retry in `_search_concepts` (1/26 thelook); `_bridge_pseudonyms`,
  `_clear_groupmate_completed_partials` (0 corpus fires; persist projection
  matrix, union_reproject tests).
- PHYSICAL: `gen_multiselect`, `gen_union`, `gen_subselect`, `gen_recursive`,
  `SubselectNode`, `RecursiveNode`, `ConstantNode.resolve_from_constant_datasources`,
  `_interpose_limit_node`, `condition_utility.{is_fully_covered,
  filter_union_children, strip_condition_atoms}` (stocks/thelook),
  discovery_utility diagnostics, `select_merge_node` `covered_conditions`
  retry and the multi-parent `_merge_condition_routing` branch
  (`tests/generators/test_condition_routing.py`), `validate_stack` (never
  non-COMPLETE on any corpus; sole production consumer
  `select_merge_node.py` L1003-1020, otherwise test-driven).
- OPTIMIZED: `JoinHoist` (538 LOC for 2 queries, ds:35 and ds:69, +1.3 KB
  when off; cross-sibling sharing, not omission; keep, low value);
  `_push_having_into_group_parent` (42 / 25 queries; the rule a previous
  coverage-based claim nearly deleted); `UnionDimPushdown` (16 / 16);
  `_narrow_directionally` (72 / 59; the documented preserving-render
  contract); `_external_forced_map` (38 / 22; decisive for SQL in ds:59,
  ds:64, h:adhoc07); `OrderInnerJoinsFirst` (17 / 16, 0 bytes but must run
  after types settle); `StripRedundantNotNull` (8-9 / 8-9; all 12 dropped
  atoms are AUTHORED `is not null`, so it is not an undo rule);
  `upgrade_join_on_guards.base_join_only` (dropping it changes 16/138);
  direct return (disabling changes 11 queries; see 2.4 for the fold);
  `UnionDimPushdown` plain/pass-through path (`_optimize_plain`,
  `_apply_plain`, `_coarsen_dead_fk_grain`, `_rendered_addrs`,
  `union_dim_pushdown.py` L363-403, L728-841, ~160 LOC: 0 corpus fires, the
  q10/q04 shapes it cites no longer produce the dedup pass-through, but
  pinned by `test_union_dim_pushdown.py:208,252,285,326,709`; maintainer
  decision, and if retained the planner's `group_graph` dim placement is the
  natural owner); `_optimization_visit_order` +
  `grouped_unbound_passthrough_should_wait` (7 reorders / 6 queries, 0 SQL
  diffs, pinned `test_collapse_basic_into_group.py:163`);
  `_promotion_would_cycle` (0 true / 34, pinned);
  `csp.rename_only` multi-consumer fold (1 fire); `lower_full_joins`
  (MySQL-only, 451 LOC, 8 unit tests, no corpus coverage);
  `filter_irrelevant_ctes` / `canonicalize_graph` / `reorder_ctes`
  (physical GC; `CTE.__add__` same-name merges 1584 per corpus run are how
  the planner's repeated shared subtrees reunite).
- RENDER: `_canonical_render_siblings` (hackernews), `_aggregate_collapse_safe`
  (thelook), `union_arm_cast_target`, `_render_subselect`,
  `_render_expression_membership_exists`
  (`tests/engine/test_duckdb_expression_membership.py`), array membership,
  `_constant_output_group_by_fallback` (usa_names), `CTE.base_alias` /
  `source_address` fallback chains (hackernews recursion), `BaseJoin.concepts`
  legacy validation (16 test construction sites), `_build_joinkeys` inner
  COALESCE. Wrong-stage but live, for the record: outer-join key COALESCE
  `base.py:842` (417 hits), multi-source COALESCE `base.py:874` (6952),
  join-key COALESCE `common.py:295` (167), `left_is_local` pin
  `common.py:211-217` (54), COUNT -> `coalesce(..., 0)` `base.py:1681-1696`
  (206; the owner would be nullability stamping in `merge_node.py`
  L976-1014).

Design-level note, not a handoff: 448 planner FULLs "because partial" are
narrowed by `_narrow_directionally` in 59 statements. This is the documented
preserving-render contract and conflicts with the planner-owns-joins rule
only in letter; the planner has the same inputs (`partial_concepts`, domain
graph, `_subtree_restrictions`), so a later handoff could move
`_pair_side_fully_matches` / `_complete_values` to node level. Not measured.

---

## Appendix: optimizer rule firing table

Render-only DuckDB sweep, 217 query units (109 tpc-ds + 1 aggregates + 39
tpc-h + 24 thelook_duckdb + 3 gcat dashboards + 41 gcat/gcat2/faa/the_look
test-module queries), 212 `optimize_ctes` invocations. "fires" = `optimize`
returned opt=True.

| Rule | fires | calls | queries | notes |
|---|---|---|---|---|
| InlineDatasource | 1065 | 8570 | 187 | 771 real inlines; 294 no-op candidate registrations (3.11) |
| CollapseSingleParent | 525 | 4040 | 190 | aggregate 354, basic 120, passthrough 41, window 10 |
| UpgradeJoinOnGuards | 258 | 2258 | 108 | cross-CTE `_external_proofs` 38x / 22q (2.1) |
| HideUnusedConcepts | 246 | 1275 | 111 | branch-only hide 10x / 4q |
| PredicatePushdown | 108 | 1588 | 61 | `_check_parent` 128, HAVING relocate 38/23q, union push 15/10q, union prune 7/7q (2.6) |
| PredicatePushdownRemove | 76 | 1060 | 44 | full removal 26/14q; `_prune_unused_single_row_parents` 0 (1.17) |
| UpgradeOuterFromKeySetEquivalence | 72 | 961 | 59 | `_narrow_directionally` 78; `_upgrade_to_inner` 1/286 (3.13) |
| MergeIrrelevantGroupBy | 27 | 1889 | 13 | merge 21/9q; `_clear_identity_group` 6/5q (2.2) |
| OrderInnerJoinsFirst | 17 | 780 | 16 | |
| UnionDimPushdown | 16 | 827 | 16 | all via `_optimize_union`; plain path 0 |
| SimplifyNullSafeJoins | 11 | 780 | 9 | decays with 2.1 |
| StripRedundantNotNull | 9 | 758 | 9 | all authored atoms |
| JoinHoist | 6 | 770 | 2 | q35, q69; `_lock_in_guarded_upgrades` 0 changes (1.20) |
| PushFilteredAggregateInput | 6 | 747 | 6 | |
| NarrowKeylessFullJoins | 4 | 1457 | 4 | second registration 0/212 (0.3); planner move 2.5 |
| PushSemiJoinIntoAggregate | 2 | 738 | 2 | q64, h:21 |
| PushFilteredCountIntoJoin | 1 | 722 | 1 | h:13 |
| lower_full_joins | n/a | n/a | n/a | MySQL only; 8 unit tests |

Driver: every phase converges in <= 3 loops except `inline_datasource`
(mean 4.8, max 11); `MAX_OPTIMIZATION_LOOPS=100` never approached; root
remap 164; `canonicalize_graph` union re-alignment 0/4586;
`filter_irrelevant_ctes` second pass 0/4586; `_grains_equivalent` pseudonym
fallback 0/245. The whole-config ablations from the cross-stage sweep
(statements changed / 235): `datasource_inlining` 199, `merge_aggregate`
193 (+1 error, h:14, item 0.2), `hide_unused_concepts` 121,
`upgrade_condition_joins` 110, `predicate_pushdown` 71,
`upgrade_outer_key_set_equivalence` 59, `merge_irrelevant_group_by` 17,
`union_dim_pushdown` 16, `direct_return` 16, `order_inner_joins_first` 16
(0 bytes), `strip_redundant_not_null` 8, `simplify_null_safe_joins` 4,
`narrow_keyless_full_joins` 3, `join_hoist` 2,
`narrow_equal_domain_joins` 0.

## Suggested sequencing

1. Tier 0 (0.3 is a two-minute delete; 0.4 a one-line wire; 0.1 and 0.2
   are real bugs).
2. Tier 1 items outside the active files, in any order, one PR each or a
   few per PR grouped by file: 1.1, 1.2, 1.3, 1.9, 1.10, 1.11, 1.12, 1.13,
   1.14, 1.15, 1.16, 1.17, 1.18, 1.19, 1.20, 1.21 (~1100 LOC, every leg
   byte-identical).
3. Tier 1 items in active files once the branch lands: 1.4, 1.5, 1.6, 1.7,
   1.8, plus the 3.10 bundle.
4. Tier 3 self-contained items: 3.1, 3.2 (render; fix 0.1), 3.11, 3.14,
   3.7, 3.8; 3.12 and 3.13 after a maintainer decision.
5. Tier 2 in the order 2.2, 2.1 (with 3.5), 2.5, 2.4 (after 0.2), 2.3(a),
   2.7, 2.6, 2.3(b)(c), 2.8, then 3.3, 3.4, 3.6. Each one deletes an
   optimizer or resolve-time repair only after the upstream change is
   proven by the firing count reaching 0 with byte-identical SQL.

Rough totals: ~1100 LOC of proven-inert deletions, ~700 LOC of downstream
repair deletable after Tier 2 upstream fixes, ~700 LOC of consolidation.
