# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 65 | 1, 2 (+02-one, 02-two), 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 19, 20, 21, 22, 24, 25, 26, 30, 31, 32, 33, 34, 35, 37, 39, 40, 41, 42, 43, 45, 46, 47, 48, 50, 52, 53, 55, 56, 57, 58, 60, 62, 63, 65, 68, 69, 71, 73, 79, 82, 83, 88, 89, 90, 92, 94, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| Skipped (preql exists) | 4 | 4, 5, 29, 44 |
| Missing (no preql / no test) | 30 | 14, 18, 23, 27, 28, 36, 38, 49, 51, 54, 59, 61, 64, 66, 67, 70, 72, 74, 75, 76, 77, 78, 80, 81, 84, 85, 86, 87, 91, 93 |

(2-one / 2-two / 97-one / 97-two are alternative phrasings of the same query
exercising different planner paths — they share an SQL reference.)

## Skipped Tests and Why

- **q4, q5** — `pytest.mark.skip(reason="Is duckdb correct??")` — look at these in next run
- **q29** — same `merge bill_customer + item` shape as q17. With the merge planner
  producing a FULL JOIN of (store_sales+store_returns) with
  (store_sales+store_returns+catalog_sales), rows without a matching catalog
  appear too. q17 avoids this only because the reference returns 0 rows on
  sf=1; q29's reference returns 1 row vs trilogy's 3. Needs INNER-merge
  semantics or an "exists in catalog_sales" filter that is correctly pushed
  to all branches.
- **q44** — `pytest.mark.skip(reason="Still cooking")` — pre-existing, not
  investigated.
- **q85** - DuckDB Comparison (PRAGMA tpcds(85)) hangs on dev machine; skip.

## Pre-existing Model Bugs Not Yet Fixed

- `store.preql` maps `S_MARKET_ID` twice (to both `market_id` and `market`).
  Likely a leftover; only one is needed.
- `customer.preql` adds `birth_date` via `cast(concat(year/'/'/month/'/'/day) as
  date)`. With sf=1, some customers have `birth_year=NULL` or month/day
  combinations that produce invalid dates (e.g. Feb 30) — DuckDB will throw on
  cast. Currently safe because no test actually selects `birth_date`.
- `address.preql` is missing `street_number`, `street_name`, `street_type`,
  `suite_number`, `location_type` — needed for q81 (and q30 if it ever switches
  to the address-based ORDER BY shape).
- `catalog_returns.preql` doesn't expose `customer`, `return_address`,
  `call_center`, or `return_amt_inc_tax` — blocks q81, q91.

## Framework Notes Worth Recording

- **`run_query` zero-row tolerance** — `assert len(comp_results) > 0` is now
  conditional on the reference also returning rows (q17 returns 0/0 on sf=1).
- **`sum/avg by …` is required for fact-row counts (q88).** Trilogy's
  default planner deduplicates by (time_id, store_id, hdemo_id) before
  summing the CASE WHEN. Solution: pin the inner aggregate to row grain via
  `auto … <- sum(CASE …) by store_sales.ticket_number, store_sales.item.id;`
  then sum it on the outside.
- **Aggregates dropped by trilogy's grain inference need explicit `by` (q79).**
  Pin per-aggregate grain with `sum(...) by store_sales.ticket_number,
  store_sales.customer.id, store_sales.sale_address.id, store_sales.store.city`.
- **Inner-join enforcement against a dimension requires an explicit
  predicate (q53, q48).** The reference SQL has `AND ss_store_sk = s_store_sk`
  which silently filters store_sales rows where the store row is missing.
  Trilogy doesn't perform a join to `store` if no store concept is selected.
  Add `store_sales.store.id is not null` to the WHERE to force the join.
- **Cross-table customer match in returns joins (q50).** Trilogy joins
  store_returns to store_sales on the loose `~ticket_number, ~item.id` mapping
  only. To match the reference's `ss_customer_sk = sr_customer_sk` constraint,
  add `store_sales.customer.id = store_sales.return_customer.id` to WHERE.
- **ORDER BY only allows columns in the SELECT projection (q73).** Trilogy
  rejects `ORDER BY x` when `x` is not in the SELECT. The reference's
  `ORDER BY ... ss_customer_sk` requires us to use '--' to hide the customer id from the output projection.
- **HAVING only allows columns in the SELECT projection (q31, q65).**
  Trilogy rejects `HAVING ssq1 > 0` when `ssq1` isn't selected. Add the
  HAVING-referenced concepts to SELECT and prefix with `--` to hide them
  from the output. For aliased aggregates, the alias is what's available
  in HAVING — `SELECT item_revenue as revenue` → `HAVING revenue <= …`,
  not `HAVING item_revenue <= …`.
- **DuckDB rejects parameter refs in ORDER BY (q39).** Trilogy parameterises
  literal SELECT constants like `1 as dmoy1` as `$1`, then if used in ORDER
  BY, DuckDB errors `Parameter not supported in ORDER BY clause`. Drop the
  constant from ORDER BY or use a non-constant expression.
- **3-channel customer cycles (q69).** When using `merge` to unify
  `customer.id` across store/web/catalog sales AND filtering with both
  `IN store_buyers` and `NOT IN web_buyers`/`NOT IN catalog_buyers`, the
  derivation graph contains a cycle. Workaround: drop the merges and use
  per-channel `<channel>.customer.id` directly in the buyer filters
  (e.g. `web_sales.customer.id ? …`).
- **SELECT column ordering (q58).** Trilogy reorders SELECT columns when
  inline aggregates are mixed with derived expressions, breaking row
  comparisons. Workaround: lift all aggregates and derived terms into
  `auto … <- …` declarations and list them in the desired order in SELECT.
- **`is null` vs row existence in SUM (q83).** `sum(returns.return_quantity ?
  filter)` returns NULL both when no row matches the filter AND when matching
  rows have `return_quantity` NULL — a different semantic than the
  reference's INNER JOIN by item. To require row existence (matching
  reference), use a separate `count(... ? filter) > 0` presence guard
  rather than `sum is not null`.
- **Decimal vs float ratio precision (q90).** `cast(amc as float) / cast(pmc
  as float)` diverges from the reference at the 7th decimal. Use
  `cast(... as numeric(15, 4))` to match the reference's
  `cast(amc as decimal(15,4))/cast(pmc as decimal(15,4))` exactly.
- **Date arithmetic syntax.** `date_add(d, n, day)` is rejected by the parser
  ("expected DATE_PART" — needs the unit token uppercased). Use
  `date_diff(d1, d2, DAY)` (uppercase DAY) for date deltas, as in
  `catalog_sales.preql:days_to_ship`.

## Suggested Next Batch (by complexity)

These look tractable without further framework work:

- **q23** (UNION ALL of catalog/web sales joined to best_ss_customer +
  frequent_ss_items; needs HAVING > 50% of MAX subquery and item filter).
- **q49** (3-channel rank + UNION) — like q83 but with `rank()` per channel.
  Doable via merge of channels through item.
- **q66** — 50+ conditional aggregates across web/catalog sales by month.
  Plumbing-heavy but mechanical.
- **q72** — model extensions are now done (catalog_sales has
  `bill_household_demographic`), but the inventory + sales + returns
  triple-join blows up to OOM during planning. Needs filter pushdown
  improvements or a smaller, more targeted query shape.
- **q76** — UNION ALL of three null-column filters; needs unified-sales-style
  join surface that exposes `ws_ship_customer_sk` and `cs_ship_addr_sk`
  separately from the bill-side keys (currently unified_sales hides them).
- **q80** — needs `catalog_page` model (parquet exists in `memory/`,
  just no preql).
- **q81** — needs catalog_returns model extension (customer, return_address,
  call_center, return_amt_inc_tax) plus address.preql street/location_type
  fields. Otherwise q30-twin.
- **q91** — same catalog_returns extensions as q81.
- **q93** — needs `reason` model.
- **q85** — needs `reason` and `web_page` models (web_page is now defined).

These need framework work first:

- **q14, q38, q87** — INTERSECT / EXCEPT of 2–3 sets (no clean trilogy
  primitive).
- **q61** — needs LEFT JOIN to promotion, OR a way to compute total without
  pulling promotion into the join (currently inner-joins drop NULL promo rows).
- **q67, q70, q77** — `rank() OVER (PARTITION BY rollup(...))` — depends on
  rollup + window combo. Trilogy doesn't currently support rollup as a
  partition argument.
- **q86** — `grouping(i_category)+grouping(i_class)` — needs `grouping()`
  function exposure.
- **q18, q27** — `GROUP BY ROLLUP (...)` patterns. Doable via the q22-style
  `MERGE … align …` cascade but verbose.
- **q51** — FULL OUTER JOIN + cumulative window over union of two
  per-channel CTEs. Needs both full-outer-join semantics and the cumulative
  `sum(sum(...)) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
  pattern.
- **q64** — 20+ table join with `income_band` (model not present); even
  with the model, the plan likely OOMs.
- **q72** — listed above; OOM on planning.

## Test Infra Notes

- `run_query(engine, idx)` defaults to `PRAGMA tpcds(N)` for the reference
  comparison. For queries where the duckdb pragma differs from the canonical
  SQL (or for queries with NULLs-FIRST/LAST behavior that varies), pass
  `sql_override=True` and provide a `queryNN.sql` file so we compare against
  a deterministic reference. Used for: q2, q17, q19, q34, q40, q56, q82.
- Skipped tests still parse — the preql files are valid; they just disagree
  with the reference at runtime.
- q46 and q68 currently take ~2+ minutes each. Plan churn from the
  `customer.address.city != customer_address.city` cross-comparison; not
  yet investigated.
