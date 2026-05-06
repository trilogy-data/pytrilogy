# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 77 | 1, 2 (+02-one, 02-two), 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 37, 39, 40, 41, 42, 43, 45, 46, 47, 48, 49, 50, 52, 53, 55, 56, 57, 58, 60, 62, 63, 65, 66, 68, 69, 71, 73, 74, 78, 79, 80, 81, 82, 83, 86, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| Skipped (preql exists) | 5 | 4, 5, 29, 44, 85 |
| Missing (no preql / no test) | 17 | 14, 18, 36, 38, 51, 54, 59, 61, 64, 67, 70, 72, 75, 76, 77, 84, 87 |

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
- **q85** — DuckDB Comparison (PRAGMA tpcds(85)) hangs on dev machine; skip.
  Preql exists and parses; this is solely a comparison-reference issue.

## Pre-existing Model Bugs Not Yet Fixed

- `customer.preql` adds `birth_date` via `cast(concat(year/'/'/month/'/'/day) as
  date)`. With sf=1, some customers have `birth_year=NULL` or month/day
  combinations that produce invalid dates (e.g. Feb 30) — DuckDB will throw on
  cast. Currently safe because no test actually selects `birth_date`.

## Recent Model Extensions (q28/q74/q78 batch)

- `store_sales.preql` — added `wholesale_cost` (`SS_WHOLESALE_COST`).
  Per-unit cost (distinct from `ext_wholesale_cost`); needed for q28.
- `item.preql` — added `class_id` (`I_CLASS_ID`). Needed for q75 (and
  any future query grouping by class_id).
- `unified_sales.preql` — added `is_returned bool?` property. Each
  returns partial datasource emits `raw(''' true ''')` for it; sales
  rows without a matching return have `is_returned IS NULL`. Use this
  rather than `return_quantity is null` for "no return" filters: the
  returns tables contain ~4% rows with `*_return_quantity = NULL`,
  so `return_quantity is null` falsely passes returned sales.

## Recent Model Extensions (unified_sales backport batch)

- `unified_sales.preql`:
  - Added `return_quantity int?`, `net_paid_inc_tax float?` properties.
  - Added `warehouse` and `ship_mode` dim imports + mappings on web/catalog
    sales partials. Store partial emits `raw(''' NULL ''')` for both so the
    `_best_enum_union` planner can include all 3 channels for queries that
    don't reference warehouse/ship_mode.
  - Added `import date as return_date` + `WR_RETURNED_DATE_SK` /
    `CR_RETURNED_DATE_SK` / `SR_RETURNED_DATE_SK` mappings on the partial
    returns datasources, so q83 can filter by `sales.return_date.week_seq`.
  - Added `WS_NET_PAID_INC_TAX` / `CS_NET_PAID_INC_TAX` / `SS_NET_PAID_INC_TAX`
    mappings; q66's catalog metric is `cs_net_paid_inc_tax * cs_quantity`.

## Recent Model Extensions (q23/q49/q66 batch)

- `web_sales.preql` — added `net_paid` (`WS_NET_PAID`), `list_price`
  (`WS_LIST_PRICE`), and `return_quantity` (`WR_RETURN_QUANTITY`) to
  the inline web_returns datasource.
- `catalog_sales.preql` — added `net_paid` (`CS_NET_PAID`),
  `net_paid_inc_tax` (`CS_NET_PAID_INC_TAX`), `return_amount`
  (`CR_RETURN_AMOUNT`), and `return_quantity` (`CR_RETURN_QUANTITY`)
  via a new inline `catalog_returns_inline` datasource.
- `ship_mode.preql` — added `carrier` (`SM_CARRIER`).

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
  - Final `SELECT alias` of rowset-derived concepts (e.g.
    `q80_results.sales_total as sales`) causes the planner to hang
    indefinitely. Bare references plan in <1s.
- **`unified_sales` channel pruning trap (q23).** With outer
  `WHERE sales.sales_channel in ('CATALOG', 'WEB')`, the trilogy planner
  drops the STORE partial datasource entirely from the shared `cheerful`
  CTE — even when other autos in the same query (e.g. for
  `frequent_ss_items` or `best_ss_customer`) reference `sales.sales_channel
  = 'STORE'` and need STORE rows. Workaround: push locally scoped conditions 
  into the fields that need filtering with them via ? 

- **NULL customer/key trap on aggregates over unified_sales (q23).**
  `sum(... ? sales.sales_channel = 'STORE') by sales.customer.id` groups
  rows with NULL `customer.id` into a single phantom group whose total can
  dwarf any real customer's total (in q23, ~18M vs the true max of
  236K), poisoning downstream `max(...)`. Reference SQL filters NULL via
  the INNER JOIN to `customer`. In trilogy, add `sales.customer.id is not
  null` to the inline `?` filter AND the rowset's `WHERE` so the phantom
  group is excluded from the grain.

## Recent Model Extensions (q27/q86 batch)

- `store.preql` — removed the redundant `S_MARKET_ID: market_id` mapping.
  `S_MARKET_ID` is still mapped to `market`, the field referenced by q24;
  `market_id` was unused. Cleaned the leftover documented in this file.
- `web_sales.preql` — added `import customer as ship_customer;` plus
  `WS_SHIP_CUSTOMER_SK: ?ship_customer.id` mapping. `?` flags the FK as
  nullable (parquet has rows with NULL ship customer). Symmetric with the
  existing catalog_sales side: catalog_sales already exposes `customer.id`
  for `CS_SHIP_CUSTOMER_SK`. Unblocks q76 modelling, even if q76 itself
  remains unsolved.

## Recent Query Additions (q27/q86 batch)

- **q27** — 3-level `MERGE … align … derive` cascade modelled on q22/q80.
  Detail (item, state) → roll up state → grand total. `g_state` literal
  per level (0/1/1) is carried through `align`. Filter (gender='M',
  marital='S', education='College', year=2002, state='TN') is repeated in
  every WHERE branch — required since each branch is a separate aggregate
  scope. avg() aggregates use `cast(... as numeric(12, 2))` to match
  reference `cast(... AS decimal(12, 2))` precision.
- **q86** — 3-level rollup over (i_category, i_class) with rank windows
  per level, replacing `rank() OVER (PARTITION BY grouping(...)+grouping(...),
  CASE WHEN grouping(class)=0 THEN i_category END)`. The trick: rather than
  expressing one cross-level rank, compute per-level ranks where the
  partition is implicit:
  - L1: `rank class over category by class_total desc` (class_total is
    `sum(net_paid) by category, class`).
  - L2: `rank category by category_total desc` (no partition; one rank
    sequence across all categories).
  - L3: `1` (only one row).
  Class- and category-level aggregates are pre-defined as autos with
  inline `?` filter so trilogy uses the right grain in the rank's `by`
  clause (the naive `rank … by sum(...)` makes trilogy compute the sum
  at the rank target's grain, which dropped the needed partition dim).
  ORDER BY uses an inline `CASE WHEN lochierarchy=0 THEN i_category END`
  (trilogy now accepts CASE expressions in ORDER BY without needing a
  hidden `--alias`).

## Suggested Next Batch (by complexity)

These look tractable without further framework work:

- **q72** — model extensions are now done (catalog_sales has
  `bill_household_demographic`), but the inventory + sales + returns
  triple-join blows up to OOM during planning. Needs filter pushdown
  improvements or a smaller, more targeted query shape.
- **q76** — UNION ALL of three null-column filters. `ship_customer` is
  now exposed on web_sales, but `store.id is null` filters require the
  store-side join to be LEFT, which trilogy currently inners. Same on
  catalog's `customer_address.id is null`. Needs nullable-join semantics
  (or a raw escape hatch) for the FK NULL probe.

## Backport Candidates (use unified_sales)

Blocked by missing `unified_sales` features:

- **q10, q35** — reference SQL is asymmetric: `ws_bill_customer_sk` for web but
  `cs_ship_customer_sk` for catalog. unified_sales currently maps both web and
  catalog to bill-side customer. Would need a `ship_customer` concept added to
  unified_sales (mapped from WS_SHIP_CUSTOMER_SK / CS_SHIP_CUSTOMER_SK; falls back
  to SS_CUSTOMER_SK for store).
- **q97-one** — intentional alternative phrasing exercising the merge planner;
  keep as-is.


These need framework work first:

- **q14, q38, q87** — INTERSECT / EXCEPT of 2–3 sets (no clean trilogy
  primitive).
- **q61** — needs LEFT JOIN to promotion, OR a way to compute total without
  pulling promotion into the join (currently inner-joins drop NULL promo rows).
- **q67, q70, q77** — `rank() OVER (PARTITION BY rollup(...))` — depends on
  rollup + window combo. The q86 trick (per-level rank windows folded
  through align) likely extends to q70/q67 as well; haven't tried yet.
- **q18** — 5-level `GROUP BY ROLLUP (i_item_id, country, state, county)`
  cascade. Same shape as q22/q27 but bigger. Adds `avg(c_birth_year)`
  and `avg(cd_dep_count)` aggregates that need explicit
  `by catalog_sales.order_number, catalog_sales.item.id` to keep grain
  correct (dim properties otherwise dedup before averaging).
- **q36** — 3-level rollup (q22-style cascade) + `rank() OVER (PARTITION BY
  lochierarchy, CASE WHEN t_class=0 THEN i_category END ORDER BY gross_margin)`
  window. Doable but verbose; needs window over rollup output.
- **q51** — FULL OUTER JOIN + cumulative window over union of two
  per-channel CTEs. Needs both full-outer-join semantics and the cumulative
  `sum(sum(...)) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
  pattern.
- **q54** — DISTINCT customer set built from `(catalog_sales UNION ALL
  web_sales)`-derived "is buyer" lookup, then revenue aggregation against
  store_sales over a `d_month_seq BETWEEN scalar_subq AND scalar_subq` window,
  then bucket binning + count(*). Needs scalar subqueries / nested set
  derivation that don't fit `unified_sales`.
- **q59** — week-over-52-weeks self-join with day-of-week pivots. Doable in
  trilogy with filtered aggregates per day, but the cross-year self-join on
  `week_seq = week_seq2 - 52` plus `s_store_id1 = s_store_id2` is verbose.
- **q64** — 20+ table join with `income_band` (model not present); even
  with the model, the plan likely OOMs.
- **q72** — listed above; OOM on planning.
- **q75** — UNION DISTINCT of three per-channel `(year, brand, class, cat,
  manufact, qty_per_row, amt_per_row)` row sets. The DISTINCT collapses
  ~1500 cross-channel duplicate tuples on sf=1 (480396 vs 478874 rows).
  Trilogy's `unified_sales` aggregates after summing per-channel; expressing
  "dedup by exact value tuple before sum" needs a primitive we don't have.
  Without dedup, summed counts diverge by ~16 per affected (brand, class,
  cat, manufact) group.
- **q84** — reference SQL `SELECT customer_id, customername FROM customer,
  ..., store_returns WHERE sr_cdemo_sk = cd_demo_sk AND ...` produces
  duplicate customer rows (one per matching `store_returns`) with no
  `DISTINCT`. Trilogy's GROUP BY semantics dedup by default. Also requires
  an `income_band` model (parquet present, no preql) and a merge between
  `customer.demographics.id` and `store_returns.customer_demographic.id`.

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
