# TPC-DS agent eval — open issues

Everything in this directory that is a bug report, handoff or roadmap doc is **open**.
Resolved reports get deleted, not archived — `git log --diff-filter=D -- evals/tpcds_agent`
finds any that were closed. Each file below carries a re-verification stamp under its title;
re-verify against HEAD before acting on one, and delete it when it stops reproducing.

Last sweep: **2026-08-16**, every report re-run against `a65b13c9c`. Fifteen reports
were deleted (fourteen verified fixed, plus the q16 enum-tautology design-rationale
file whose reasoning is now inlined at each of its five citation sites). The survivors
are ranked below.

## Stack rank

| # | file | why here |
|---|---|---|
| 1 | `bug_keyless_join_axis_lost_guard_fires.md` | **P0 blocking.** Two query shapes (`all_sales` YoY pivot, rollup over union-joined rowsets) hard-fail `UnresolvableQueryException`. Both emitted silent `on 1=1` cartesians before `a65b13c9c`; the q04 600s eval hang was one of them. Neither shape has a test. |
| 2 | `bug_q30_null_key_join_fans_out.md` | **P1 silent duplication.** A padding NULL matches a real NULL under `is not distinct from`, cross-joining 311 unmatched addresses with 20 NULL-key aggregate groups = 6,220 duplicate rows that swamp the LIMIT. Preserving the address side is NOT the bug (that is the declared partial-binding semantic); the 311x multiplication is. |
| 3 | `bug_q54_int32_arithmetic_overflow.md` | **P1 loud, trivially reachable.** `big::int * 50` overflows INT32 at execution; integer multiplication never widens. Type-inference fix, small and self-contained. |
| 4 | `bug_q05_float32_union_placeholder_drift.md` | **P1 silent money drift, one-line core.** `float8` ingests as 4-byte `FLOAT`. The old "no DOUBLE type" framing is stale — `DataType.DOUBLE` exists, so the reverse-map narrowing is the whole bug. |
| 5 | `handoff_composite_membership_invalid_reference_q17.md` | **P2 capability gap**, cleanly rejected today. Tuple membership inside an inline-filtered aggregate. The plain-`where` path proves the machinery exists; audited as a lost invariant, not a principled rejection. |
| 6 | `q14_file_list_unsandboxed_crash_bug.md` | **P2 tooling/safety.** `LocalFileBackend._resolve` is a bare `Path().expanduser()` — `file list /` still escapes the workspace. The crash half is fixed (100-entry cap). |
| 7 | `handoff_aggregate_selection_gap.md` | Items 1-3 landed; **items 4 (size-aware tie-break) and 5 (sum-linearity / per-measure non-null counts) are open**, plus the pinned `sum(x) + 0` gap. Optimization, degrades silently. |
| 8 | `handoff_scalar_where_aggregate_two_roots.md` | **Designed, not implemented.** Scalar WHERE-aggregate gate needs a distinct unfiltered root; the parse-time guard keeps users safe meanwhile. Sizable planner change and the code map is stale. |
| 9 | `handoff_empty_group_aggregate_null_lint.md` | Doc fix landed; the **authoring-time warning for `<null-on-empty agg> = 0`** is not implemented. Cheap post-parse AST check, high agent value (drove a 2.1M-token loop). |
| 10 | `handoff_auto_aggregate_postfix_where_sugar.md` | Sugar only — `auto x <- avg(a*b) where p` for the working `avg(a*b ? p)`. Grammar work in both backends. |
| 11 | `rowset_as_connector_support.md` | Sugar only — accept `rowset name as select …`. The dead-end error is already fixed (Syntax [105]), so severity is low. |

## Known-open elsewhere

`evals/tpch_agent/bug_inline_aggregate_alias_before_by_cryptic_error.md` — ranks between
9 and 10 above: a friendly `detect_alias_before_by` detector, sibling to two that already
exist in `trilogy/parsing/v2/errors.py`.

## Everything else here is strategy, not a defect

`plan_close_ingest_gap.md`, `handoff_noise_crossover.md`, `handoff_enriched_token_reduction.md`,
`handoff_messy_warehouse_first20.md`, `design_messy_warehouse_v2.md`.

## Everything else here is tooling

`run_eval.py` / `run_ingest_eval.py` (entrypoints), `spec.py`, `repeat_query.py`,
`error_scan.py`, `incremental_funnel.py`, `ingest_runs.py`, `clean_runs.py`,
`regen_spliced_report.py`, `reviewer_corpus.py`, `analyze_run.py` (shim into `evals/common`),
`repro_q05_rollup_rowset_binding.{py,preql}` (repro for #1 — its assertion is stale, it now
dies inside `generate_sql`), plus `query_prompts.json` (the 99 prompts), `reviewer_corpus/`
and `charts/`. `results/` and `.cache/` are gitignored per-run output.
