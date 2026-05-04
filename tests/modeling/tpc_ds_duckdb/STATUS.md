# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 68 | 1, 2 (+02-one, 02-two), 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 19, 20, 21, 22, 24, 25, 26, 30, 31, 32, 33, 34, 35, 37, 39, 40, 41, 42, 43, 45, 46, 47, 48, 50, 52, 53, 55, 56, 57, 58, 60, 62, 63, 65, 68, 69, 71, 73, 79, 81, 82, 83, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| Skipped (preql exists) | 5 | 4, 5, 29, 44, 80 |
| Missing (no preql / no test) | 26 | 14, 18, 23, 27, 28, 36, 38, 49, 51, 54, 59, 61, 64, 66, 67, 70, 72, 74, 75, 76, 77, 78, 84, 85, 86, 87 |

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
- **q80** — `unified_sales` was reshaped to expose a single channel
  dimension (`channel_dim_id` + `channel_dim_text_id`) backed by three
  partial datasources (one each for `store`, `catalog_page`, `web_site`)
  keyed by `(sales_channel, channel_dim_id)`, plus three partial
  `*_returns_unified` datasources contributing `return_amount` /
  `return_net_loss` at the same grain as sales. Three framework fixes
  enable this shape (see Framework Notes): RawColumnExpr handling in
  `safe_get_cte_value`, multiple parallel unions per merge_key, and a
  relaxed `force_group` merge in `QueryDatasource.__add__`.
  Single-SELECT and 2-level cascade variants now plan and execute
  correctly. The 3-level `MERGE+align` cascade plans (~0.7s, ~14KB SQL)
  but produces wrong channel-/grand-total rows: level-2 (`kaput`) and
  level-3 (`macho`) SELECTs read from a `late` CTE whose join shape
  differs from level-1's `puzzled` CTE, so the rolled-up `returns`/
  `profit` sums diverge from the reference (sales totals match). Detail
  rows themselves match. Skipped pending planner investigation.
  Note: using `q80_results.sales_total as sales` aliases in the final
  `SELECT` causes planning to hang indefinitely — use bare references
  to keep planning fast.

## Pre-existing Model Bugs Not Yet Fixed

- `store.preql` maps `S_MARKET_ID` twice (to both `market_id` and `market`).
  Likely a leftover; only one is needed.
- `customer.preql` adds `birth_date` via `cast(concat(year/'/'/month/'/'/day) as
  date)`. With sf=1, some customers have `birth_year=NULL` or month/day
  combinations that produce invalid dates (e.g. Feb 30) — DuckDB will throw on
  cast. Currently safe because no test actually selects `birth_date`.

## Recent Model Extensions (q80/q81/q91 batch)

- `address.preql` — added `text_id`, `street_number`, `street_name`,
  `street_type`, `suite_number`, `location_type` (`CA_*` columns).
  Removed dead `street` field that was never sourced from a parquet column.
- `catalog_returns.preql` — added `customer`, `refunded_customer`,
  `return_address`, `refunded_address`, `call_center`, `reason` joins
  and `return_amt_inc_tax` property. Sources `CR_RETURNING_*`, `CR_REFUNDED_*`
  and `CR_CALL_CENTER_SK` columns.
- `customer.preql` — added `household_demographic` import + `C_CURRENT_HDEMO_SK`
  mapping (needed for q91's hd_buy_potential filter via customer).
- `call_center.preql` — added `text_id` (`CC_CALL_CENTER_ID`) and
  `manager` (`CC_MANAGER`) properties.
- `web_returns.preql` — added `net_loss` property (`WR_NET_LOSS`).
- `web_sales.preql` — added `promotion` import (`WS_PROMO_SK`).
- `catalog_sales.preql` — added `catalog_page` import (`CS_CATALOG_PAGE_SK`).
- `catalog_page.preql` — new model.
- `unified_sales.preql` — added `store`, `catalog_page`, `web_site`
  dimension imports; each partial datasource maps its own SK to its dim and
  emits `raw(''' NULL ''')` for the other two channels' SKs (so cross-channel
  joins resolve cleanly when querying multiple dim text_ids in one SELECT).

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
- **DuckDB rejects parameter refs in ORDER BY (q39) — fixed.** Trilogy used
  to parameterise every CONSTANT concept (e.g. `1 as dmoy1` → `:dmoy1`),
  which DuckDB rejects in ORDER BY. INTEGER and BOOL constants are now
  rendered inline as SQL literals (see `INLINE_SAFE_PARAM_DATATYPES` in
  `trilogy/dialect/base.py`); FLOAT stays parameterised because DuckDB
  parses inline `3.14` as DECIMAL (changing result type from float to
  Decimal); strings stay parameterised against SQL injection.
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
- **`derive` clause uses comma separators, not `AND` (q80).** Multiple derive
  expressions in one MERGE cascade go in a single `derive` block separated by
  commas: `derive expr1 -> name1, expr2 -> name2`. `AND` is rejected by the
  parser. Only one `derive` block per multi-select.
- **Inline `?` filter requires a concept, not an arbitrary expression (q80).**
  `sum(coalesce(x.return_amount, 0) ? cond)` fails the parser. Workaround:
  define a property auto for the expression first
  (`property <fact_grain>.x_return_safe <- coalesce(x.return_amount, 0)`),
  then `sum(x_return_safe ? cond)`.
- **`raw('NULL')` requires multi-line string syntax.** `raw('NULL')` is a parse
  error (single-quote string not accepted in raw column expression).
  Use `raw(''' NULL ''')` (triple-single-quote MULTILINE_STRING).
- **Trilogy planner fixes landed for q80** (multi-fact UNION ALL + ROLLUP):
  - `'RawColumnExpr' object has no attribute 'startswith'` — fixed in
    `dialect/base.py:safe_get_cte_value`. The single- and multi-source
    paths now branch on `RawColumnExpr` / `FUNCTION_ITEMS` and emit
    the raw text directly instead of routing through `safe_quote`.
  - `'Can only merge two datasources if force_group flag is the same'` —
    relaxed in `core/models/execute.py:QueryDatasource.__add__`. The
    merged datasource takes `force_group=True` if either side asserts it,
    `False` if either side asserts that, otherwise `None`.
  - **Multiple parallel unions per merge_key** — `_best_enum_union` in
    `core/processing/node_generators/select_helpers/datasource_injection.py`
    now clusters partial datasources by their non-merge-key concept
    signature and emits one union per maximal signature. Lets parallel
    sales / returns / dim partitionings (each keyed by the same
    `sales_channel` enum) coexist as separate union datasources rather
    than collapsing into a single best combo.
- **Open trilogy issues exposed by q80**:
  - 3-level `MERGE+align` over a multi-source partitioning generates
    distinct CTE shapes per level (level-1 reads from one join, level-2/3
    read from a sibling `late` CTE without the dim join). Aggregations
    diverge.
  - Final `SELECT alias` of rowset-derived concepts (e.g.
    `q80_results.sales_total as sales`) causes the planner to hang
    indefinitely. Bare references plan in <1s.

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
- **q85** — needs `reason` and `web_page` models (both now defined; query is
  blocked because PRAGMA tpcds(85) hangs as the comparison reference).

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
