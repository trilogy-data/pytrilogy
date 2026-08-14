# TPC-DS agent eval — open issues

Everything in this directory that is a bug report, handoff or roadmap doc is **open**.
Resolved reports get deleted, not archived — `git log --diff-filter=D -- evals/tpcds_agent`
finds any that were closed. Each file below carries a re-verification stamp under its title;
re-verify against HEAD before acting on one, and delete it when it stops reproducing.

Last sweep: **2026-08-10**, against `results/20260726-191755_*` (enriched **85/99**, sql_bare
78/99 — Trilogy leads by +7). Every failure in that run is "result set differs from reference";
there are no errors, crashes, hangs or timeouts, which is what retired the 2026-07 backlog.

## Engine defects

| file | one-line |
|---|---|
| `bug_q23_scalar_subquery_rendered_as_duckdb_parameter.md` | scalar subquery inline in a select output reaches the driver as an unrendered `SubqueryItem` |
| `bug_q54_int32_arithmetic_overflow.md` | integer multiplication never promotes to BIGINT → INT32 overflow at execution |
| `bug_q05_float32_union_placeholder_drift.md` | no 8-byte DOUBLE type; `FLOAT` → DuckDB REAL → silent money drift |
| `handoff_q05_rollup_rowset_binding.md` | rollup over a joined rowset emits an ungrouped binding column (`repro_q05_rollup_rowset_binding.py`) |
| `bug_q95_filtered_count_nested_max.md` | filtered count over `is_returned` emits invalid nested `count(max(CASE ...))` SQL |

## Language / DX gaps

| file | one-line |
|---|---|
| `handoff_scalar_where_aggregate_two_roots.md` | scalar WHERE-aggregate gate needs a distinct unfiltered root; designed, not implemented — code map is stale |
| `handoff_auto_aggregate_postfix_where_sugar.md` | postfix `where` on a derived aggregate (`auto x <- avg(a*b) where …`); sugar over the working `?` form |
| `rowset_as_connector_support.md` | grammar still rejects `rowset name as select …`; error message is fixed, enhancement is not |
| `q14_file_list_unsandboxed_crash_bug.md` | `trilogy file list` is not confined to the workspace root |
| `bug_q95_explore_json_hides_fk_and_filter_scope.md` | `explore --format json` hides imported-dimension FKs |
| `bug_q89_numeric_type_unit_mocking.md` | `trilogy unit` cannot mock precision-bearing `numeric(p,s)` columns |

## Not a bug — kept as design rationale

`bug_q16_enum_tautology_drops_joined_null_rejection.md` is resolved, but
`trilogy/core/models/core.py`, `docs/type_validators_design.md` and
`tests/engine/test_enum_unions.py` all cite it as the reason the constant-satisfiability
check flags only provably-FALSE predicates and never tautologies. Update those three before
deleting it.

## Known-open elsewhere

`repro_rollup_inferred_grain_in_def.py` still fails — `rollup()` with an inferred grain
recurses inside `def` macros. It has no report file.

## Everything else here is tooling

`run_eval.py` / `run_ingest_eval.py` (entrypoints), `spec.py`, `repeat_query.py`,
`error_scan.py`, `incremental_funnel.py`, `ingest_runs.py`, `clean_runs.py`,
`regen_spliced_report.py`, `reviewer_corpus.py`, `analyze_run.py` (shim into `evals/common`),
plus `query_prompts.json` (the 99 prompts), `reviewer_corpus/` and `charts/`.
`results/` and `.cache/` are gitignored per-run output.
