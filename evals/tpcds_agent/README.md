# TPC-DS agent eval - open issues

Everything in this directory that is a bug report, handoff or roadmap doc is **open**.
Resolved reports get deleted, not archived - `git log --diff-filter=D -- evals/tpcds_agent`
finds any that were closed. Each file below carries a re-verification stamp under its title;
re-verify against HEAD before acting on one, and delete it when it stops reproducing.

**Deleting a file closes every item in it.** If a report's tail still holds open work when
its headline bug is fixed, split that work into its own file before deleting - otherwise the
remainder disappears with no deletion record anyone will think to look for.

Last sweep: **2026-08-20**, every report re-run against `6bdb4d7b4`. Nothing closed as
fixed: eight of the nine ranked below were re-run and reproduce exactly as written, and
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
