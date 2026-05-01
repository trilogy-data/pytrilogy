# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 30 | 1, 2 (+02-one, 02-two), 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 19, 20, 21, 22, 24, 25, 26, 30, 32, 40, 42, 43, 52, 55, 56, 82, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| Skipped (preql exists) | 5 | 4, 5, 17, 34, 44 |
| Missing (no preql / no test) | 64 | 14, 18, 23, 27, 28, 29, 31, 33, 35–39, 41, 45–51, 53, 54, 57–81, 83–94 |

(2-one / 2-two / 97-one / 97-two are alternative phrasings of the same query
exercising different planner paths — they share an SQL reference.)

## Skipped Tests and Why

- **q4, q5** — `pytest.mark.skip(reason="Is duckdb correct??")` — pre-existing, not investigated this round.
- **q17** — needs `stddev_samp`. Trilogy stdlib has no `stddev` aggregate; q17 has
  12 occurrences across `ss_quantity`, `sr_return_quantity`, `cs_quantity` plus
  ratio expressions. *Action:* add `stddev_samp` (and likely `variance`) to the
  trilogy function library, then unskip.
- **q29** — same shape as q17, also needs `stddev_samp`. Currently has no preql
  or test; pair with q17.
- **q44** — `pytest.mark.skip(reason="Still cooking")` — pre-existing, not
  investigated this round.


## Pre-existing Model Bugs Not Yet Fixed

- `store.preql` maps `S_STORE_MANAGER` to `store_manager`, but the duckdb
  column is `s_manager`. Currently latent (no test references the field). Fix
  when something needs it.
- `store.preql` maps `S_MARKET_ID` twice (to both `market_id` and `market`).
  Likely a leftover; only one is needed.
- `customer.preql` adds `birth_date` via `cast(concat(year/'/'/month/'/'/day) as
  date)`. With sf=1, some customers have `birth_year=NULL` or month/day
  combinations that produce invalid dates (e.g. Feb 30) — DuckDB will throw on
  cast. Currently safe because no test actually selects `birth_date`.

## Suggested Next Batch (by complexity)

These look tractable without touching the framework:

- **q14** (UNION ALL of three channels filtered by category/brand "best
  sellers" — same `unified_sales` pattern as q56).
- **q23** (similar UNION-ALL across channels with year totals).
- **q45** (web_sales by zip + correlated `i_item_id IN ...` subquery — needs
  to verify trilogy handles the OR-with-subquery pattern).
- **q46, q68, q79** (household_demographics + customer + multi-aggregate;
  model already in place after q13/q34 work).
- **q60** (drop-in clone of q56 with `i_category='Music'` instead of color set —
  use `unified_sales` pattern; should be ~10 minutes).
- **q88** (eight cross-joined `count(*)`-of-time-bucket subqueries; needs
  `count_records` or the concat trick generalized).
- **q53, q63, q89** (window function `avg(sum(x)) OVER (PARTITION BY …)` —
  trilogy supports window functions; verify the
  `partition by ... order by ... window` syntax against existing usages in
  `query97.preql`).
- **q41** (item self-correlated subquery with many OR branches — verify
  trilogy can express the inner aggregate filter pattern).

These need framework work first:

- **q17, q29** — stddev_samp.
- **q67, q70** — `rank() OVER (PARTITION BY rollup(...))` — depends on rollup
  + window combo.
- **q86** — `grouping(i_category)+grouping(i_class)` — needs `grouping()`
  function exposure.

## Test Infra Notes

- `run_query(engine, idx)` defaults to `PRAGMA tpcds(N)` for the reference
  comparison. For queries where the duckdb pragma differs from the canonical
  SQL (or for queries with NULLs-FIRST/LAST behavior that varies), pass
  `sql_override=True` and provide a `queryNN.sql` file so we compare against
  a deterministic reference. Used for: q2, q19, q34 (skipped), q40, q56, q82.
- Skipped tests still parse — the preql files are valid; they just disagree
  with the reference at runtime.
