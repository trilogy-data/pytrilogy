# TPC-DS agent eval — open issues

Everything in this directory that is a bug report, handoff or roadmap doc is **open**.
Resolved reports get deleted, not archived — `git log --diff-filter=D -- evals/tpcds_agent`
finds any that were closed. Each file below carries a re-verification stamp under its title;
re-verify against HEAD before acting on one, and delete it when it stops reproducing.

Last sweep: **2026-08-16**, every report re-run against `a65b13c9c`. Fifteen reports
were deleted (fourteen verified fixed, plus the q16 enum-tautology design-rationale
file whose reasoning is now inlined at each of its five citation sites). The survivors
are ranked below.

Also closed 2026-08-16 (same day, working tree): `bug_keyless_join_axis_lost_guard_fires.md`:
both shapes fixed in the demand/contract passes (ROOT capability advertises FD-determined
authored axis members; `_consumer_required_input_grain` drops axes a grouping descendant
already folded; `_relevant_root_preserve_keys` keeps authored members). Row-asserting
regression tests: `tests/engine/test_duckdb_subset_join_pivot_axis.py` and the
`test_rollup_with_basic_over_aggregates_executes` case in
`tests/engine/test_duckdb_rollup_rowset_binding.py`. The `all_sales` pivot now matches
DuckDB's `PRAGMA tpcds(4)` reference exactly at sf=1.

Also closed 2026-08-16 (same day, working tree): `bug_q30_null_key_join_fans_out.md`.
An outer-join padding NULL is absence, not a key value, so it no longer satisfies
`is not distinct from` against a real NULL group on the other side
(`nulls_are_values` in `trilogy/core/processing/join_resolution.py`, consumed by
`get_modifiers`). q30 at sf=1 goes 6,373 rows -> 153, matching the reference exactly.
Row-asserting regression: `tests/join_matrix/test_outer_padding_null_not_a_value.py`
(fan-out cell plus a both-sides-value-NULL control). Corpus footprint is one query,
q64, where the same asymmetry demoted one key to `=` with its row assertions unchanged.

Filed and closed 2026-08-18 (working tree): `bug_membership_predicate_drops_output_group_by.md`, from the gpt-5.6-luna 99-question four-leg run (`20260818-022328_*`). The existence wrapper that `gen_root` builds to apply a membership predicate (`trilogy/core/processing/v4_node_generators/root.py`) passed no `grain`, so `StrategyNode._resolve` stamped it with `BuildGrain.from_concepts(output_concepts)`: the wrapper only filters, but it claimed the grain of its own narrowed projection rather than the fact rows it reads. An output `as` alias stacks a BASIC rename on top of that wrapper, and the FINAL dedup check (`_group_to_grain_if_required`) then read the scan as already at output grain and skipped the `GroupNode`. Unaliased there is no extra layer, and `calculate_effective_parent_grain` looks through the wrapper's multiple datasources to the real grain, which is why only the aliased form broke. The wrapper now inherits its parent's grain, one line. Row-asserting regression: `tests/join_matrix/test_membership_output_alias_grain.py`, six cells over the trigger matrix. Corpus footprint is zero across all 132 tpc-ds and tpc-h queries, and the q83 nested-rowset-join shape is byte-identical. Worth knowing for the next grain bug: declared node grain is NOT an enforced invariant here. A scan over the corpus finds 149 nodes (125 `SelectNode`, 24 `FilterNode`) that declare a coarser grain than their parents with no grouping, so a fix phrased as "distrust declared grain in `calculate_effective_parent_grain`" is the wrong altitude; make the specific node truthful instead. A shape sweep over eight alias-vs-bare query forms diverged only on the membership shapes, confirming this wrapper was the only reachable liar. A second report filed the same day, `bug_fact_fk_membership_sourced_from_dimension.md`, was DISPROVEN and deleted within the hour: `ss.customer.sk` is the customer dimension key namespaced under the `ss` import, so it is full-dimension extent by design and `exists (select 1 from customer ...)` is the correct rendering. It never landed in a commit, so `git log --diff-filter=D` will not find it; this paragraph is the whole verdict. Do not re-file it.

## Stack rank

| # | file | why here |
|---|---|---|
| 1 | `bug_q54_int32_arithmetic_overflow.md` | **P1 loud, trivially reachable.** `big::int * 50` overflows INT32 at execution; integer multiplication never widens. Type-inference fix, small and self-contained. |
| 2 | `bug_q05_float32_union_placeholder_drift.md` | **P1 silent money drift, one-line core.** `float8` ingests as 4-byte `FLOAT`. The old "no DOUBLE type" framing is stale — `DataType.DOUBLE` exists, so the reverse-map narrowing is the whole bug. |
| 3 | `handoff_composite_membership_invalid_reference_q17.md` | **FIXED 2026-08-16.** Tuple membership inside an inline-filtered aggregate now plans off the same fact-anchored island as the plain-`where` path (arg group, not address, is the unit of existence sourcing). Kept for the open follow-up: a foreign-fact ROW predicate inside a filtered aggregate still renders `INVALID_REFERENCE_BUG` sentinels instead of the clean disconnected-subgraph error. |
| 4 | `q14_file_list_unsandboxed_crash_bug.md` | **P2 tooling/safety.** `LocalFileBackend._resolve` is a bare `Path().expanduser()` — `file list /` still escapes the workspace. The crash half is fixed (100-entry cap). |
| 5 | `handoff_aggregate_selection_gap.md` | Items 1-3 landed; **items 4 (size-aware tie-break) and 5 (sum-linearity / per-measure non-null counts) are open**, plus the pinned `sum(x) + 0` gap. Optimization, degrades silently. |
| 6 | `handoff_scalar_where_aggregate_two_roots.md` | **Designed, not implemented.** Scalar WHERE-aggregate gate needs a distinct unfiltered root; the parse-time guard keeps users safe meanwhile. Sizable planner change and the code map is stale. |
| 7 | `handoff_empty_group_aggregate_null_lint.md` | Doc fix landed; the **authoring-time warning for `<null-on-empty agg> = 0`** is not implemented. Cheap post-parse AST check, high agent value (drove a 2.1M-token loop). |
| 8 | `handoff_auto_aggregate_postfix_where_sugar.md` | Sugar only — `auto x <- avg(a*b) where p` for the working `avg(a*b ? p)`. Grammar work in both backends. |
| 9 | `rowset_as_connector_support.md` | Sugar only — accept `rowset name as select …`. The dead-end error is already fixed (Syntax [105]), so severity is low. |

## Known-open elsewhere

`evals/tpch_agent/bug_inline_aggregate_alias_before_by_cryptic_error.md` — ranks between
7 and 8 above: a friendly `detect_alias_before_by` detector, sibling to two that already
exist in `trilogy/parsing/v2/errors.py`.

## Everything else here is strategy, not a defect

`plan_close_ingest_gap.md`, `handoff_noise_crossover.md`, `handoff_enriched_token_reduction.md`,
`handoff_messy_warehouse_first20.md`, `design_messy_warehouse_v2.md`.

## Everything else here is tooling

`run_eval.py` / `run_ingest_eval.py` (entrypoints), `spec.py`, `repeat_query.py`,
`error_scan.py`, `incremental_funnel.py`, `ingest_runs.py`, `clean_runs.py`,
`regen_spliced_report.py`, `reviewer_corpus.py`, `analyze_run.py` (shim into `evals/common`),
plus `query_prompts.json` (the 99 prompts), `reviewer_corpus/`
and `charts/`. `results/` and `.cache/` are gitignored per-run output.
