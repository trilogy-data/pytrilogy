# Bug: `grouping_id(...)` over a `by rollup` aggregate → SQL with no GROUP BY → "GROUPING statement cannot be used without groups"

**Status:** FIXED 2026-06-07 (found enriched eval q70). Rollup hard-cluster (q70/q77/q80).
Fix in `trilogy/parsing/v2/select_finalize.py` `_fix_projection_grouping_mode`: generalized
the B3 helper to (a) find the rollup spec from by-name `auto` concepts (not just inline
`ConceptTransform`s) via `context.concepts`, and (b) walk nested expression trees
(`case`/`CaseWhen`/comparisons/etc.) to align every STANDARD-mode `grouping`/`grouping_id`
wrapper with the rollup's mode+by so it co-locates in the single ROLLUP CTE. Tests:
`test_grouping_id_in_case_over_named_rollup_{colocates,executes}` in `tests/engine/test_duckdb.py`.
**Severity:** high — the canonical rollup idiom (use `grouping_id` to derive the subtotal LEVEL) is
unauthorable: Trilogy emits `grouping_id(...)` into the SQL but never emits the matching
`GROUP BY … WITH ROLLUP` / `GROUPING SETS`, so DuckDB rejects it. The agent flailed across **33
distinct rewrites / 2.17M tokens** trying to work around it (nearly exhausted at 70 calls).

## Symptom

```
(_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups
LINE 89:  WHEN grouping_id("thoughtful"."ss_store_state", …) = 1 then 1
```
`generate_sql` succeeds; DuckDB rejects at execution. The generated SQL contains `grouping_id(...)`
(in the SELECT, typically inside a CASE deriving the rollup level) but the GROUP BY is a plain
`GROUP BY 1,2,…`, not a `ROLLUP`/`GROUPING SETS` — so `grouping_id` has no grouping context.

## Trigger shape (verified, executes-fails)

A rollup aggregate plus a `grouping_id` over the same dims to compute the level:
```trilogy
import physical_sales as ss;
auto net_profit <- sum(ss.net_profit) by rollup ss.store.state, ss.store.county;
auto lvl <- case
    when grouping_id(ss.store.state, ss.store.county) = 3 then 2
    when grouping_id(ss.store.state, ss.store.county) = 1 then 1
    else 0 end;
select ss.store.state, ss.store.county, net_profit, lvl, ...
order by ... ;
```
The exact q70 body is in `evals/tpcds_agent/results/20260607-225157/workspace/` (the file-write whose
`run` returned `GROUPING statement cannot be used`; this run the content flag is `-c`). Driver:
`generate_sql(BODY)` succeeds; executing the SQL against `tpcds.duckdb` raises the BinderException.

## Root cause

`by rollup` and `grouping_id` are planned independently: the rollup output's grain is materialized,
but when `grouping_id(...)` is projected it renders as a literal SQL `grouping_id()` against a CTE
whose GROUP BY is NOT a ROLLUP/GROUPING SETS. The grouping function and the grouping clause must be
emitted together. (Distinct from `bug_B3_grouping_in_orderby_rollup.md` — that fixed `grouping()` in
ORDER BY; this is `grouping_id()` in a SELECT/CASE level-derivation.)

## Suggested fix

When a query projects `grouping_id(...)`/`grouping(...)` over dims that are a `by rollup` set, emit
the aggregation as `GROUP BY ROLLUP(...)` (or `GROUPING SETS`) so the grouping function has its
context — and keep them in the same SELECT scope (don't split the rollup into separate CTEs). If
that lowering is out of scope, reject `grouping_id` outside a rollup/grouping-sets context with a
clear message, and document the supported "derive the level from the rollup output's NULL pattern"
idiom (the canonical `.preql` for q70 does exactly that: `case when state is null then … end`
instead of `grouping_id`). Right now the agent reaches for `grouping_id` (the TPC-DS-native idiom)
and gets unrunnable SQL with no hint.

## Provenance

Enriched eval q70 (store net-profit rolled up by state/county with subtotal levels + rank). 33
rewrites, 2.17M tokens, 70 calls. The companion q59 in the same run was NOT a framework bug — it was
over-verification (one query, run 20×).
