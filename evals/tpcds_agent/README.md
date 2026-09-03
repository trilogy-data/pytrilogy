# TPC-DS agent eval - open issues

Everything in this directory that is a bug report, handoff or roadmap doc is **open**.
Resolved reports get deleted, not archived - `git log --diff-filter=D -- evals/tpcds_agent`
finds any that were closed. Each file below carries a re-verification stamp under its title;
re-verify against HEAD before acting on one, and delete it when it stops reproducing.

**Deleting a file closes every item in it.** If a report's tail still holds open work when
its headline bug is fixed, split that work into its own file before deleting - otherwise the
remainder disappears with no deletion record anyone will think to look for.

Last sweep: **2026-08-21**, an audit of the 08-20 probe wave against `84bc9ffeb`: every
wave row's repro was re-run or its regression gate executed, six reports closed and
deleted, and the survivor rewritten down to its two open items (details in
`INDEX_probe_wave_2026_08_20.md` and under "Closed after the 08-20 sweep" below). The
stack rank below is unchanged and was last re-run 2026-08-20 against `6bdb4d7b4`, where
nothing closed as fixed: eight of the nine ranked below were re-run and reproduce exactly as written, and
rank 5 is a static gap whose absence was confirmed in the source and is pinned by an
existing test. One report was deleted as **declined** rather than fixed (see below). Two
files the 08-16 sweep had ranked were deleted in `facdd161c` (2026-08-19) with open items
still inside them; both are recovered below.

## Stack rank

| # | file | why here |
|---|---|---|
| 1 | `bug_q54_int32_arithmetic_overflow.md` | **P1 loud, trivially reachable.** `big::int * 50` overflows INT32 at execution; integer multiplication never widens. Type-inference fix, small and self-contained. |
| 2 | `bug_q05_float32_union_placeholder_drift.md` | **P1 silent money drift, one-line core.** `float8` reverse-maps to `DataType.FLOAT` (`trilogy/dialect/base.py:478`), so a physical DOUBLE column ingests as 4 bytes. `DataType.DOUBLE` exists, so the narrowing is the whole bug. |
| 3 | `bug_q05_where_breaks_cross_model_aggregates.md` | **P2 hygiene, but it plans and then dies.** A WHERE over disconnected aggregate islands falls through both existing relatability checks and renders `INVALID_REFERENCE_BUG` sentinels instead of the clean error the same run produced elsewhere. Carries the follow-up inherited from the closed q17 handoff and a secondary internal-invariant leak. |
| 4 | `q14_file_list_unsandboxed_crash_bug.md` | **P2 tooling/safety.** `LocalFileBackend._resolve` is a bare `Path().expanduser()`, and `read`/`write`/`delete` all route through it - `file list /` still escapes the workspace. The crash half is fixed (100-entry cap). |
| 5 | `handoff_aggregate_selection_size_and_linearity.md` | Recovered 2026-08-20 from the deleted `handoff_aggregate_selection_gap.md`, whose items 1-3 landed on 08-12. Open: size-aware tie-break, aggregates inside an enclosing expression, sum-linearity / per-measure non-null counts. Optimization, degrades silently. |
| 6 | `handoff_scalar_where_aggregate_two_roots.md` | **Designed, not implemented.** Scalar WHERE-aggregate gate needs a distinct unfiltered root; the parse-time guard keeps users safe meanwhile. Sizable planner change and the code map is stale. |
| 7 | `handoff_empty_group_aggregate_null_lint.md` | Doc fix landed; the **authoring-time warning for `<null-on-empty agg> = 0`** is not implemented. Cheap post-parse AST check, high agent value (drove a 2.1M-token loop). |
| 8 | `feature_array_contains_function.md` | Agent-compat sugar - `array_contains(arr, elem)` is the DuckDB/Spark spelling agents reach for; today it dies at the open paren. Ships with a wiring checklist. The bundled "unknown function name" diagnostic is the higher-leverage half. |
| 9 | `rowset_as_connector_support.md` | Sugar only - accept `rowset name as select …`. The dead-end error is already fixed (Syntax [105]), so severity is low. |

## 2026-08-20 probe wave (run 20260820-031800, filed same day - not yet folded into the rank above)

Seven probes over the run's >500k sinks. **Audited 2026-08-21 against `84bc9ffeb`:
six of the seven are closed and their reports deleted** (a, c, d, e, f, plus the
q66 report filed off the 153007 triage); the per-row record and the fix summaries
live in `INDEX_probe_wave_2026_08_20.md`. What is left is one report of open
engine work and one question/harness item.

| | file | why |
|---|---|---|
| b | `bug_presence_probe_no_ops_and_q72_axis_residual.md` | **P1 silent, two items.** A null test on a coalescing join-key member renders POST-merge on the fused column and no-ops on one side, so an authored intersection silently keeps one-sided rows (q59/q77; placement bug, not eligibility). Plus the q72 residual: `_aggregate_axis_members` keeps a relation member in the branch grain when the counted row identity holds one, so the authored grain never re-aggregates. Both repro on committed models. |
| g | `bug_q29_cross_leg_sink.md` | Not a framework bug: the question omitted the sale-to-return match keys. Rewording APPLIED 2026-08-21: match keys and billed-to catalog customer stated, and the question asks the natural version rather than the spec query's fan-out summation. The fan-out came out of `tests/modeling/tpc_ds_duckdb/query29.{sql,preql}` at the same time, so question, corpus query and eval reference agree; no result changes at sf=1. q29 is not prompt-comparable across that date. Still open: the >500k detector should read cache-adjusted tokens - reasoning-replay inflated q29/q17 raw counts ~10x over fresh cost, and `evals/common/analyze_run.py` still ranks on raw prompt+completion. |

## Closed after the 08-20 sweep

- `bug_q17_join_condition_syntax_loop.md` - FIXED 2026-08-20. The end-of-input 202 probe now
  runs ahead of the 225 branch in both parser backends, so an otherwise-valid post-select
  join missing only its `;` reports "Missing closing semicolon?" rather than "Expected a
  join condition" (`detect_join_missing_key`'s only guard - a `select` between the join and
  the failure - can never fire in that position). The ingest leg's second spelling got its
  own detector: Syntax [230] + `detect_join_comma_group` for a comma between join groups,
  caret on the comma, the join-group sibling of `detect_align_missing_and` (221). The
  query-guide nudge landed too (`trilogy/ai/syntax_examples.py`: an example that ends
  directly after a join clause, plus `;`-terminator and comma-vs-`and` notes). Regression
  guard: `tests/complex/test_join_missing_key_error.py`. The report's harness note (>500k
  detector should read fresh tokens) is unaffected and still carried by
  `bug_q29_cross_leg_sink.md`. Report deleted 2026-08-21.

- `bug_q47_window_rowset_churn.md` - both P1 silent codegen bugs FIXED 2026-08-20.
  (1) An OR-rooted `BuildConditional` child under an AND parent now renders parenthesized
  (`_protect_conditional_child`, `trilogy/dialect/base.py`), so a pushed-down HAVING atom
  binds to the whole OR chain instead of its last arm.
  (2) `QueryDatasource.__add__` now carries `nullable_concepts` through a merge
  (`trilogy/core/models/execute.py`), so outer-join padding nullability survives and the
  windowed and window-free plans no longer disagree on LEFT OUTER vs INNER.
  Regressions: `tests/rendering/test_engine_or_chain_precedence.py`,
  `tests/core/processing/test_query_datasource_add_nullability.py`; both verified failing
  with the respective fix reverted. Its two non-bug polish items were split into
  `handoff_q47_diagnostic_polish.md`, now also closed (below). Report deleted 2026-08-21.

- `handoff_q47_diagnostic_polish.md` - both diagnostic-polish items FIXED 2026-08-21.
  (1) `EnvironmentConceptDict._find_similar_concepts` now offers a rowset output's leaf
  shorthand (`monthly_totals.name`) directly behind its full path
  (`monthly_totals.ss.store.name`), so the suggestion list carries the spelling the docs
  teach. `_rowset_leaf_shorthand` withholds it whenever a sibling output under the same
  rowset also matches the leaf, judged against the full candidate set - including the
  internal names filtered out of the suggestion pool - so a suggested shorthand always
  resolves. Existing suggestion ranking is unchanged.
  (2) `window_filter_needs_having` no longer fires on the bookend shape (widen the window's
  navigation axis in the WHERE so `lag`/`lead` have neighbours, narrow back in the HAVING).
  Window scopes now carry salt-stripped WHERE/HAVING/ORDER BY concept sets, and
  `_window_filter_is_deliberate` suppresses when a HAVING atom re-constrains a concept the
  WHERE constrains, or when the WHERE touches only concepts the window orders by. Corpus
  footprint measured across every tpc-ds/tpc-h `query*.preql`: exactly q47 (both firings)
  and q57, the catalog-sales twin of the same template; q02/q36/q49/q51/q59/q67/q70/q86
  still warn. Regressions: `tests/test_undefined_concept.py` (4 cases),
  `tests/test_scope_diagnostics.py` (3 cases). Spec updated in
  `docs/SPEC_query_derived_value_scopes.md`. Report deleted 2026-08-21.

- `bug_keyless_join_guard_ingest_cluster.md` - FIXED 2026-08-20, all 37+3 guard firings and
  the silent `on 1=1` no-fact-key variant, from two sites. Forward reach in
  `group_rules._cosource_component_groups` now walks THROUGH a rename without counting it,
  so `select x as t` buckets like `select x` and the zero-reach bailout engages; and the
  BASIC axis-upstream wiring in `concept_graph.py` dropped its rename carve-out, so a bare
  `dim.attr as a` keeps the scoped-join axis. Gate:
  `tests/engine/test_duckdb_aliased_dim_attr_join_axis.py` (5 cases). The filed one-FK-hop
  connectivity defect needed no code; do NOT re-add a per-root FK reach field (it caches a
  pure function of the environment onto every leaf). Latent and deliberately unfixed: the
  shared-datasource rule gates on `purpose != PROPERTY`, so it never fires for a
  `UNIQUE_PROPERTY`; no failing case demonstrates it today. Report deleted 2026-08-21.

- `bug_q44_empty_unexpected_error.md` - FIXED 2026-08-20, both halves.
  `InlineDatasource._join_key_demand` refuses to fold a parent whose join keys (the
  `all_rows` broadcast marker, resolved through `CTEConceptPair` rather than `source_map`)
  are not columns of the raw datasource, and the render assert in
  `trilogy/core/models/execute.py` carries a message. CLI half:
  `handle_execution_exception` falls back to the exception class name when `str(e)` is
  empty, so no message-less exception reaches an agent as silence. Gate:
  `tests/optimization/test_inline_broadcast_join_key.py`. Report deleted 2026-08-21.

- `bug_q14_values_list_virt_filter_binder.md` - FIXED 2026-08-20. `count(key ? cond)`
  dedups its input to the key's grain, so the filter mask is now computed BELOW that dedup
  (`v4_helper/projection.py`, `strategy_builder.py`); neither the phantom `_virt_filter_*`
  union render (BinderException) nor the "Missing source reference" generation error
  occurs. Gate: `tests/engine/test_filtered_key_aggregate_dedup.py` (5 cells including the
  union-datasource arm). Report deleted 2026-08-21.

- `bug_q66_union_output_drops_nullable.md` - FIXED 2026-08-20. `union_item_to_concept` ORs
  the signature flag with the arms' own nullability (`_expr_is_nullable`), so a `union(...)`
  TVF output key inherits arm nullability, the sibling filtered-aggregate rejoin renders
  `is not distinct from` and the NULL group survives. Gate:
  `tests/engine/test_duckdb_union_tvf_nullable_output.py`; corpus render byte-identical.
  Report deleted 2026-08-21.

## Filed 2026-08-21

- `handoff_q29_aggregate_in_where_plan_size.md` - q29's rewrite (fan-out removed from
  question, corpus query and reference alike) left a plan-size question: a pinned aggregate
  that is both the WHERE-level intersection test and a projected measure renders the catalog
  side in both scopes, 14,113 chars against 10,614 for a two-level spelling of the same
  answer. Correctness is gated; this is deferred optimization with the measured
  alternatives and two incorrect-but-smaller spellings recorded.

## Known-open elsewhere

`evals/tpch_agent/bug_inline_aggregate_alias_before_by_cryptic_error.md` - ranks between
7 and 8 above: a friendly `detect_alias_before_by` detector, sibling to two that already
exist in `trilogy/parsing/v2/errors.py`.

## Closed between the 08-16 and 08-20 sweeps

- `handoff_composite_membership_invalid_reference_q17.md` - primary FIXED: tuple membership
  inside an inline-filtered aggregate now plans off the same fact-anchored island as the
  plain-`where` path (the arg group, not the address, is the unit of existence sourcing).
  Row-asserting regression `tests/modeling/tpc_ds_duckdb/test_q17_composite_membership.py`.
  Its open follow-up moved into rank 3 above.
- `bug_q08_split_membership_projection_with_aggregate.md` - a projected
  `x in split(param, ',')` alongside an aggregate no longer sentinels the split's virt
  concept. Filed and fixed inside the same PR, so it never appears in a squashed deletion
  record; verified not reproducing 2026-08-20.
- `handoff_aggregate_selection_gap.md` - items 1-3 landed 08-12; the rest is rank 5.
- `handoff_auto_aggregate_postfix_where_sugar.md` - **declined, not fixed.** Postfix
  `where` on a derived aggregate (`auto x <- avg(a*b) where p`) is a shape we do not want;
  `avg(a*b ? p)` is the spelling. Deleted 2026-08-20 so it stops reading as a backlog item.

## Closed in the 08-16 and 08-18 sweeps

Fifteen reports were deleted on 08-16 (fourteen verified fixed, plus the q16 enum-tautology
design-rationale file whose reasoning is now inlined at each of its five citation sites).
Three closures from those sweeps carry lessons worth keeping in front of whoever picks up
the next grain or join bug:

- `bug_keyless_join_axis_lost_guard_fires.md` - both shapes fixed in the demand/contract
  passes (ROOT capability advertises the FD-determined authored axis members;
  `_consumer_required_input_grain` drops axes a grouping descendant already folded;
  `_relevant_root_preserve_keys` keeps authored members). Regressions:
  `tests/engine/test_duckdb_subset_join_pivot_axis.py` and
  `test_rollup_with_basic_over_aggregates_executes` in
  `tests/engine/test_duckdb_rollup_rowset_binding.py`.
- `bug_q30_null_key_join_fans_out.md` - an outer-join padding NULL is absence, not a key
  value, so it no longer satisfies `is not distinct from` against a real NULL group
  (`nulls_are_values` in `trilogy/core/processing/join_resolution.py`). q30 at sf=1 goes
  6,373 rows -> 153. Regression: `tests/join_matrix/test_outer_padding_null_not_a_value.py`.
- `bug_membership_predicate_drops_output_group_by.md` (filed and closed 08-18) - the cause
  was a planner-wide convention, not one site: `StrategyNode._resolve` derived a node's
  grain from its own `output_concepts` whenever the planner passed none, which states the
  grain the projection SELECTS, not its row grain. `SelectNode` and `FilterNode` now set
  `inherits_parent_grain`. **Worth knowing for the next grain bug:** a spot fix that made
  only the membership wrapper truthful passed everything, and so did one that taught
  `calculate_effective_parent_grain` to distrust declarations. Neither was right - the
  declaration is what should be true. Gate:
  `tests/discovery/test_row_preserving_grain_invariant.py` (census 149 -> 0).

**DISPROVEN, do not re-file:** `bug_fact_fk_membership_sourced_from_dimension.md` claimed
that `ss.customer.sk` membership wrongly sourced from the dimension. It is the customer
dimension key namespaced under the `ss` import, so it is full-dimension extent by design
and `exists (select 1 from customer ...)` is the correct rendering. The report never landed
in a commit, so `git log --diff-filter=D` will not find it; this paragraph is the whole
verdict.

## Everything else here is strategy, not a defect

`plan_close_ingest_gap.md`, `handoff_noise_crossover.md`, `handoff_enriched_token_reduction.md`,
`handoff_messy_warehouse_first20.md`, `design_messy_warehouse_v2.md`.

## Everything else here is tooling

`run_eval.py` / `run_ingest_eval.py` (entrypoints), `spec.py`, `repeat_query.py`,
`error_scan.py`, `incremental_funnel.py`, `ingest_runs.py`, `clean_runs.py`,
`regen_spliced_report.py`, `reviewer_corpus.py`, `analyze_run.py` (shim into `evals/common`),
plus `query_prompts.json` (the 99 prompts), `reviewer_corpus/`
and `charts/`. `results/` and `.cache/` are gitignored per-run output.
