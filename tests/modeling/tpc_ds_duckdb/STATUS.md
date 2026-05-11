# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 92 | 1, 2 (+02-one, 02-two), 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 76, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| Skipped (preql exists) | 2 | 5, 29 |
| XFAIL (committed broken for experimentation) | 1 | 14 |
| Missing (no preql / no test) | 4 | 54, 64, 75, 77 |

(2-one / 2-two / 97-one / 97-two are alternative phrasings of the same query
exercising different planner paths — they share an SQL reference.)

## Skipped Tests and Why

- **q5** — 97/100 rows match. Restructured 2026-05-09 from a 3-branch
  MERGE/align/derive cascade to a single SELECT with native `by rollup`
  syntax + WHERE-clause date filter (sale OR return in window) + per-row
  CASE-WHEN gating to 0 (mirroring the reference's UNION ALL with
  `cast(0 AS decimal)` zero-fillers). Phantom NULL rows for every dim_id
  are gone. Remaining shortfall:
  - Grand total returns: 3208516 vs ref 3255243 (-46K, 1.4% short)
  - Catalog channel returns: 1093915 vs ref 1083574 (+10K)
  
  Root cause is structural: every fact row in trilogy carries the SALE
  side's `channel_dim_id` (e.g. `cs_catalog_page_sk`). When a return's
  own `cr_catalog_page_sk` ≠ the sale's, the return amount is
  attributed to the sale's page (trilogy) instead of the return's page
  (reference). And when the sale's `*_store_sk` is NULL, the
  `INNER JOIN thoughtful` (dim CTE) drops the row entirely.
  
  The model now declares `CR_CATALOG_PAGE_SK: ?channel_dim_id` /
  `SR_STORE_SK: ?channel_dim_id` on the returns partials (truthful, since
  these columns exist), but the planner still sources `channel_dim_id`
  only from the sales partial and the returns CTE doesn't pull it. Real
  fix needs framework work — the planner must coalesce sale-side and
  return-side `channel_dim_id` per row.

  **Tried (2026-05-09)** introducing a parallel `return_channel_dim_id`
  / `return_channel_dim_text_id` concept on `unified_sales` (sourced
  from CR/SR SK on returns partials, from WS_WEB_SITE_SK on web sales
  partial), with separate dim partials for each channel, and a q5
  shape that aggregates sales-by-sale-dim and returns-by-return-dim in
  two rowsets joined via `merge`. Two structural blockers:
  1. Without a `return_channel_dim_id` mapping on the catalog/store
     sales partials, the planner errors with "Missing source map
     entry" — every concept must have a source per partial.
  2. Even with `raw('NULL')` fallback on sales partials, joining the
     two rowsets uses INNER JOIN (not FULL OUTER) — so detail rows
     where one side has no entry are dropped, AND no rollup-level
     rows survive. Adding `~` to the merge doesn't change this.
  
  Combining sales-by-sale-dim and returns-by-return-dim into a single
  output requires either (a) FULL OUTER join semantics for cross-
  rowset merges, or (b) a model that splits each fact row into two
  logical rows (one per attribution side) — both framework changes.
- **q29** — same `merge bill_customer + item` shape as q17. With the merge planner
  producing a FULL JOIN of (store_sales+store_returns) with
  (store_sales+store_returns+catalog_sales), rows without a matching catalog
  appear too. q17 avoids this only because the reference returns 0 rows on
  sf=1; q29's reference returns 1 row vs trilogy's 3. Needs INNER-merge
  semantics or an "exists in catalog_sales" filter that is correctly pushed
  to all branches.
- ~~**q85**~~ — moved to passing 2026-05-09 by switching the test fixture
  from `engine` (sf=1) to `engine_sf001` (sf=0.01). PRAGMA tpcds(85) at
  sf=1 hangs; sf=0.1 completes in ~28 min; sf=0.01 completes in ~3s.

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

- **`group <prop> by <keys>` re-grains a dim property (q18).** When you need
  `avg(dim.prop) row-weighted by fact rows`, the naive `avg(catalog_sales.bill_customer.birth_year)`
  computes at customer.id grain (each distinct customer counted once). Use
  `auto row_birth_year <- group catalog_sales.bill_customer.birth_year by
  catalog_sales.order_number, catalog_sales.item.id;` to broadcast the
  property to the row grain, then `avg(row_birth_year)` weights correctly.
  Prefer this over declaring `property <key1, key2>.x <- prop` which can
  hit "Missing source reference" inside multi-fact MERGE selects.
- **`align` is UNION semantics, not JOIN (q44 lesson).** The merged rowset's
  rows are the rows from each branch concatenated, with the aligned columns
  joined column-wise via `IS NOT DISTINCT FROM`. Branches with non-overlapping
  alignment patterns (typical rollup levels) produce disjoint rows. To force
  a true cross-branch join (e.g. asc-rank rows paired with desc-rank rows at
  the same `rnk`), use two separate rowsets and `merge a.rnk into b.rnk;`
  outside the rowset bodies — that ties the rank concept across them so the
  final SELECT can co-reference both.
- **Rollup level direction is column-order, not reversed (q18 lesson).**
  `GROUP BY ROLLUP(a, b, c, d)` strips dims off the right, producing levels
  (a,b,c,d), (a,b,c), (a,b), (a), (). When laying out MERGE branches in
  trilogy, mirror that — L0 has all dims, L1 keeps the leftmost N-1 dims and
  NULLs the rightmost, etc. Reversing this layout silently produces
  syntactically valid but semantically wrong rollup output.
- **NULL customer/key trap on aggregates over unified_sales (q23).**
  `sum(... ? sales.sales_channel = 'STORE') by sales.customer.id` groups
  rows with NULL `customer.id` into a single phantom group whose total can
  dwarf any real customer's total (in q23, ~18M vs the true max of
  236K), poisoning downstream `max(...)`. Reference SQL filters NULL via
  the INNER JOIN to `customer`. In trilogy, add `sales.customer.id is not
  null` to the inline `?` filter AND the rowset's `WHERE` so the phantom
  group is excluded from the grain.
- **Pin window-function grain by passing extra fields to `rank(...)` when
  ranking ROLLUP output (q67).** `gen_window_node`'s `parent_concepts`
  only includes `(target, partition, order_by)` from the window lineage
  plus whatever's in `concept.keys`. Other dims requested by the SELECT
  come back through a `gen_enrichment_node` join keyed on
  `parent_concepts` with `IS NOT DISTINCT FROM`. For ROLLUP output that
  join key is non-unique (multiple rolled-up rows can share `(NULL,
  NULL, value)`), so the back-join multi-matches and duplicates results.
  The SQL-style window grammar accepts extra comma-separated fields
  after the first — `rank(target, pin1, pin2, ...) over (partition by p
  order by o)` — and `WindowItem.pin` flows them into `concept.keys`,
  so the planner pulls every pin through the window CTE and no
  enrichment join is needed. Pair with `HAVING rk <= 100` (not WHERE) —
  WHERE re-routes the threshold into the source path, which produces a
  second ROLLUP and a third rank (plan balloons to 5 CTEs and the
  result is wrong).

## Recent Model Extensions (2026-05-09: truthful nullable across `unified_sales`)

`unified_sales.preql` now mirrors the actual sf=1 parquet nullability
across both sales and returns partials. Every FK and metric in the
shared property declaration is nullable (`?`) and every per-partial
mapping flags `?` where the source column is nullable:

- web_sales/catalog_sales/store_sales partials: every `_SK` column +
  every metric column has a nullable rate of 0.02% / 0.5% / 4.5%
  respectively (only `*_ITEM_SK` and `*_ORDER_NUMBER`/`*_TICKET_NUMBER`
  are non-nullable).
- store_returns_unified, web_returns_unified: `*_RETURNED_DATE_SK` is
  nullable (only `*_ITEM_SK`/`*_ORDER_NUMBER` non-nullable).
- catalog_returns_unified: only `CR_RETURNED_DATE_SK` and
  `CR_RETURNED_TIME_SK` happen to be non-nullable in the data.
- Returns partials now also declare `CR_CATALOG_PAGE_SK` / `SR_STORE_SK`
  → `?channel_dim_id` (truthful — both columns exist in the parquet).
  Currently ignored by the planner (the returns CTE doesn't pull
  `channel_dim_id`); kept for future framework support.

This change rippled into one query:
- q74: needed `sales.customer.id is not null` (same NULL-customer
  phantom group trap documented for q23 — now applies because
  `customer.id` flowing from sales partials is genuinely nullable).
  All other 97 unified_sales-using tests still pass unchanged.

## Recent Model Extensions (truthful nullable FKs across fact tables)

Every FK on `store_sales`, `catalog_sales`, and `web_sales` that is
actually nullable in the sf=1 parquet data is now marked `?` (nullable):
- `store_sales`: 4.5% NULL rate on every `_sk` (date, time, customer,
  cdemo, hdemo, addr, store, promo).
- `catalog_sales`: 0.5% NULL rate on every `_sk` (date, sold_date, time,
  ship_date, customer, cdemo, hdemo, addr, ship_addr, ship_cdemo,
  ship_hdemo, call_center, catalog_page, ship_mode, warehouse, promo).
- `web_sales`: 0.02–0.03% NULL rate across all `_sk`s.

Grain keys (`SS_TICKET_NUMBER`, `CS_ORDER_NUMBER`, `WS_ORDER_NUMBER`,
`*_ITEM_SK` for the item.id grain part) stay non-nullable.

### What this means for queries

When a fact has a nullable FK to a complete dim:
- The trilogy planner picks `JoinType.FULL` between the fact and the dim
  (see `get_join_type` in `core/processing/join_resolution.py`). This is
  the truthful join: it preserves *both* fact rows with NULL FK *and*
  dim rows with no matching fact.
- For queries whose reference SQL uses `FROM fact, dim WHERE
  fact.fk = dim.pk` (INNER), add an explicit `dim.id is not null` filter
  per dim hop **and** a `fact.<grain_key> is not null` filter to drop
  the phantom dim-only rows. The grain-key filter is the trilogy
  equivalent of "row exists in the fact table".

Examples of the filter pattern:
- q6: added `store_sales.customer.id is not null and
  store_sales.customer.address.id is not null` (reference INNER joins
  customer and address).
- q34, q79: added `store_sales.customer.id is not null`.
- q50, q65: added `store_sales.store.id is not null`.
- q62: added `ws.warehouse.id is not null and ws.ship_mode.id is not
  null and ws.web_site.id is not null`.
- q76 (NULL-FK probe query): adds both `<dim>.id is null` *and*
  `<fact>.<grain_key> is not null` *and* `<fact>.date.id is not null`,
  so the catalog branch is `WHERE cs.customer_address.id is null and
  cs.order_number is not null and cs.date.id is not null`.

### Note on `FULL JOIN` vs `LEFT JOIN`

q61's `get_join_type` already handles the simple cases (single-side
nullable, single-side partial) by returning LEFT_OUTER or RIGHT_OUTER.
For "left nullable + left partial-on-key, right complete" — the typical
fact-to-dim shape — `get_join_type` returns FULL because:
> if the right has nulls on the join key the non-nullable left has
> nothing to match them against — they'd land on the dropped side.
> Upgrade to FULL so they survive.

The phantom dim rows that come with this are legitimate truthful output
(every dim row with no fact match), and the burden of filtering them
falls on the query writer.

## Recent Query Additions (q76)

- **q76** — Per-channel UNION ALL of NULL-FK probes (ss_store_sk,
  ws_ship_customer_sk, cs_ship_addr_sk). Three-branch MERGE with
  per-channel `WHERE <dim>.id is null` filters that work directly off
  the now-nullable FK joins.
  - **count() in multi-fact MERGE branches gets wrapped in
    `coalesce(., 0)`, breaking the cross-branch
    `coalesce(cnt_a, cnt_b, cnt_c)` derive** — the inner coalesce
    converts NULL to 0, then the outer coalesce takes the first non-NULL
    (the 0). Workaround: replace `count(...)` with a row-grain 0/1 flag
    and `sum(flag)`:
    `auto ss_row_flag <- sum(case when ss.store.id is null then 1 else 0 end) by ss.ticket_number, ss.item.id;`
    then `sum(ss_row_flag) as cnt_a`. Similar pattern for each branch.

## Open issue: planner FULL JOIN for fact-dim with nullable FK

When a fact has a nullable FK to a dim and the join graph also has the
fact marked as partial on that key (typical of any fact→dim FK — the
fact only spans a subset of the dim's rows), `get_join_type` in
`trilogy/core/processing/join_resolution.py:154` falls into the
"both not complete" branch and returns `JoinType.FULL`. With
`is not distinct from` equality and a FULL JOIN, every dim row appears
in the result (even those without a matching fact), producing phantom
dim-only rows that — though *truthful* output — pollute aggregations
that expected reference-SQL INNER semantics.

This is currently treated as expected behavior (the model says the FK
is nullable; the join is truthful), and individual queries use the
`<fact>.<grain_key> is not null` + `<dim>.id is not null` filter
pattern to drop phantom rows when needed.

A future refinement could have the planner emit `fact LEFT JOIN dim`
(forcing fact-on-LEFT) when only the fact-side FK is nullable — the
phantom dim rows aren't actually useful for any query I've written so
far. But that's an optimization, not a correctness fix.

## Recent Query Additions (q14 — XFAIL, blocked)

- **q14** — `INTERSECT` of 3 (brand_id, class_id, category_id) sets +
  4-level rollup + `avg_sales` HAVING threshold. Two shapes attempted,
  both blocked; the in_cross_items chain shape is committed in
  `query14.preql` with `@pytest.mark.xfail(strict=True)` on
  `test_fourteen` so we can iterate.
  - **Shape A (committed)**: defines `store_pres / catalog_pres /
    web_pres` autos at `(brand_id, class_id, category_id)` grain,
    combines them into `in_cross_items`, and uses it inside
    `bucket_sum`'s case-when. Trilogy re-derives the presence
    aggregations inside each merge branch's `bucket_sum > avg_sales`
    filtered scope — `store_pres` becomes "rows that *also* pass
    HAVING" rather than "rows in the original dataset". Result: ~6.6%
    undercount on sf=1.
  - **Shape B (rowset + IN, not committed)**: replaces the chain with
    `with cross_items_set as WHERE in_window_channel_count = 3 SELECT
    sales.item.id;` and filters `sales.item.id in cross_items_set
    .ci_item_id`. Avoids the re-derivation, but `auto bucket_sum <-
    sum(sales.quantity * sales.list_price) by sales.sales_channel,
    sales.item.brand_id, sales.item.class_id, sales.item.category_id`
    is then sourced from the unfiltered partial-datasource union (no
    Nov-2001 date filter, no IN cross_items_set membership applied),
    giving all-time totals. count() also routed through the returns
    partial in this shape, producing absurd over-counts.

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

## Recent Query Additions (q4/q18/q38/q44/q70/q87 batch)

- **q4** — extends q11/q74 to all three channels (store/web/catalog) using
  the unified_sales `def channel_year_total(channel, year)` macro. Three pairs
  of first/second-year totals and a `case when first > 0 then second/first`
  ratio compare per-channel growth — tractable because all per-channel
  metrics are pinned `by sales.customer.id` inside the macro.
- **q18** — 5-level rollup over (i_item_id, country, state, county) following
  q22/q27. Adds `avg(c_birth_year)` and `avg(cd_dep_count)` aggregates that
  need to be re-grained to the row level via `auto x <- group <prop> by
  cs.order_number, cs.item.id;` so dim-property values aren't dedup'd by
  their source key before averaging. **Trilogy `group … by` lets you re-grain
  a property to keys outside its native grain — much cleaner than declaring
  a `property <key1, key2>.x <- prop` shim, which can fail with
  "Missing source reference" when the new property is then used in an
  aggregate inside a multi-fact MERGE.**
- **q38** — `INTERSECT` of (last_name, first_name, date.date) tuples across
  store/catalog/web sales. Same pattern as q97: per-channel CASE-WHEN sum
  flagged at the tuple grain, then `count(case when all three > 0 then 1)`.
  Filtering on `customer.id is not null` mirrors the reference's INNER JOIN
  to `customer`.
- **q44** — best/worst items by avg net_profit at store 4. Two parallel
  rowsets (asc-rank vs desc-rank) of the SAME item set, joined by `merge
  ascending.rnk_a into descending.rnk_d;` so a single `rnk` row carries
  different `best_performing` / `worst_performing` items. Threshold filter
  pushed into the rowset's `where`. **Note: `align` does NOT join across
  branches — each branch's rows independently appear in the merged rowset.
  For true cross-branch joins (different items at the same rnk position),
  use `merge X into Y` between rowsets.**
- **q70** — 3-level rank-over-rollup. Top-5 states by total profit are
  picked in a small rowset (`with top_states as ... HAVING state_rnk <= 5`),
  then per-level rowsets compute (state, county) detail / state subtotal /
  grand total with the q86 trick — `rank county over state by county_total
  desc` at L0, `rank state by state_total desc` at L1, constant `1` at L2.
- **q87** — `EXCEPT` of store sales tuples vs catalog/web. Same shape as
  q38 but with `store > 0 AND catalog = 0 AND web = 0`. NULL-name customers
  are filtered via `customer.id is not null` to mirror the reference's
  inner join semantics.

## Recent Query Additions (q36/q61 batch)

- **q36** — same shape as q86 (3-level rollup over (i_category, i_class)
  with per-level rank). Aggregate is a ratio: `gross_margin =
  sum(net_profit) / sum(ext_sales_price)`. Pre-compute paired profit/sales
  sums per grain and derive the ratio: `cast(profit as numeric(15,4)) /
  cast(sales as numeric(15,4))` matches duckdb's
  `(sum*1.0000)/sum` (DOUBLE result) at the bit level — `cast as float`
  diverges at the 7th decimal (q90 caveat). ASC rank order, otherwise
  identical to q86.
- **q61** — promotional / total ratio over (Jewelry × gmt_offset=-5)
  filter. Concise shape: `metric promotional_sales <- sum(filter ...)`,
  shared `WHERE` clause, inline `sum(ss.ext_sales_price) -> total` and
  inline ratio in the same SELECT. Required three fixes to land:
  - **Model:** `store_sales.preql` `SS_PROMO_SK: promotion.id` →
    `?promotion.id`. ~4.5% of `ss_promo_sk` values are NULL in the data;
    the non-nullable mapping forced an INNER JOIN that under-counted the
    `all_sales` denominator (~$25K of $5.586M dropped in the q61 window).
  - **Planner — join direction:** `get_join_type` in
    `trilogy/core/processing/join_resolution.py` returned `RIGHT_OUTER`
    for "left nullable, right complete", preserving the dim and *still*
    dropping the fact's NULL-key rows. Now branches on partial vs nullable:
    nullable-only on the left maps to `LEFT_OUTER` (preserves left's
    NULL-key rows); partial-only stays `RIGHT_OUTER` (preserves the
    complete right); both maps to `FULL`. Symmetric to the existing
    right-side handling.
  - **Planner — merge dedup:** `deduplicate_nodes` in
    `trilogy/core/processing/nodes/merge_node.py` compared parent
    `output_concepts` (excluding partials only) and dropped subset
    parents. With the inline ratio shape, the discovery loop produced
    three GroupNodes including one for `total` alone and one for
    `{promotional_sales, ratio, total}` where both `promotional_sales`
    and `total` were marked **hidden**. The total-only parent was
    dropped as "subset" — but the surviving parent's `total` is hidden
    and `resolve_concept_map` skips hidden concepts when building the
    source map, leaving `local.total` unmapped and tripping
    `QueryDatasource.__post_init__`'s validate_missing check. Fix:
    exclude hidden concepts from the subset comparison so a parent that
    hides a concept doesn't shadow another that exposes it.

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

## Recent Query Additions (q51 — cumulative window via unified_sales)

- **q51** — FULL OUTER JOIN of two per-channel cumulative-sum CTEs +
  outer running-max forward-fill. Solved without explicit FULL JOIN /
  rowset merge by leaning on three properties of `unified_sales` +
  trilogy's window semantics:
  1. The (item, date) grain emitted from a `sales_channel in ('WEB',
     'STORE')` filtered SELECT is the union of (item, date) tuples from
     either channel — implicitly a FULL JOIN of the two per-channel
     daily-aggregate sets.
  2. Each per-channel daily total uses an inline `?` filter:
     `sum(sales.sales_price ? sales.sales_channel = 'WEB') by sales.item.id, sales.date.date`.
     On rows where the channel had no sale, the SUM yields NULL.
  3. SQL's cumulative `sum(daily) over (partition by item order by date)`
     ignores NULL inputs, so the cumulative is automatically
     forward-filled across NULL-daily rows — no explicit running MAX
     needed (the reference's outer `max(cume_sales) OVER ...` is
     redundant given the inner `sum()` is already monotonic in
     non-negative sales).

  Subtleties found:
  - The reference's `web_sales` / `store_sales` columns are NULL on
    `(item, date)` rows where the channel had no sale-row at all — but
    `daily IS NOT NULL` is the wrong gate, because some rows have a
    real `ss_sales_price = NULL` (sf=1 has these). Use a row-presence
    flag aggregate instead:
    `auto store_has_row <- max(case when sales.sales_channel = 'STORE' then 1 else 0 end) by sales.item.id, sales.date.date;`
    then `case when store_has_row = 1 then store_cume else null end`.
  - **WHERE on cross-window concepts trims the wrong CTE column.** The
    naive `WHERE web_cume > store_cume` produced an extra "juicy" CTE
    that carried `web_cume` but **not** `store_cume` — the outer
    SELECT then tried to re-derive `store_cume` from a CTE missing
    `sales_ext_sales_price`, producing a `Missing source reference`
    error. Workaround: move the cross-concept comparison to `HAVING
    web_cumulative > store_cumulative` (HAVING-on-non-aggregates is
    accepted) so both columns are projected through the SELECT
    plumbing.
  - Reference uses `ws_sales_price` (per-unit), **not**
    `ws_ext_sales_price` (line total). Diff is ~50× per row — easy to
    miss; q51 has no row-by-row reference checker for that field.

## Trilogy planner change for q51

- **`max` / `min` as window functions** (`trilogy/dialect/base.py`).
  `WindowType.MAX` / `WindowType.MIN` were enum-defined and accepted
  by the parser, and the parser also auto-converted them to
  `AggregateWrapper` when no `order_by` was present, but the
  `WINDOW_FUNCTION_MAP` lacked render entries — so `max X over Y order
  by Z` failed with `KeyError`. Added `window_factory("max",
  include_concept=True)` / `window_factory("min", include_concept=True)`
  alongside the existing SUM/COUNT/AVG entries. q51's solution does
  not use these (cumulative SUM of NULLs already forward-fills), but
  the support is now landed for future running-max / running-min
  needs.

## Suggested Next Batch (by complexity)

These look tractable without further framework work:

- **q72** — model extensions are now done (catalog_sales has
  `bill_household_demographic`), but the inventory + sales + returns
  triple-join blows up to OOM during planning. Needs filter pushdown
  improvements or a smaller, more targeted query shape.

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

- **q14** — `INTERSECT` of 3 (brand_id, class_id, category_id) sets to find
  items present in all 3 channels, plus a 4-level rollup over (channel,
  brand, class, category) and an `avg_sales` HAVING threshold. **XFAIL**
  — `query14.preql` is committed with the in_cross_items-chain shape and
  `test_fourteen` has `@pytest.mark.xfail(strict=True)`. See "Recent
  Query Additions (q14 — XFAIL, blocked)" above for the two attempted
  shapes and the underlying planner bugs.
- **q77** — same store/catalog/web rollup as q5. Has the same sale-vs-return
  date filter mismatch (sale-date filter vs return-date filter). Catalog uses
  `cs_call_center_sk` as id (not catalog_page) and store/web use LEFT JOINs
  for returns. Blocked on the same return-side dim mapping as q5.
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

## Test Infra Notes

- `run_query(engine, idx)` defaults to `PRAGMA tpcds(N)` for the reference
  comparison. For queries where the duckdb pragma differs from the canonical
  SQL (or for queries with NULLs-FIRST/LAST behavior that varies), pass
  `sql_override=True` and provide a `queryNN.sql` file so we compare against
  a deterministic reference. Used for: q2, q17, q19, q34, q40, q56, q59, q82.
- Skipped tests still parse — the preql files are valid; they just disagree
  with the reference at runtime.
- q46 and q68 currently take ~2+ minutes each. Plan churn from the
  `customer.address.city != customer_address.city` cross-comparison; not
  yet investigated.
- An `engine_sf01` fixture (sf=0.1 dataset, separate `memory_sf01/` dir) is
  available for tests where the sf=1 reference PRAGMA hangs/OOMs on a given
  machine. Opt in by taking `engine_sf01` instead of `engine` as the test
  parameter. Dataset is generated lazily on first use via raw duckdb (avoids
  capturing trilogy's `uv_run` macro into the exported `schema.sql`).

## Recent Query Additions (q84)

- **q84** — Edgewood customers in a fixed income band, joined to
  `store_returns` on `sr_cdemo_sk = cd_demo_sk`. The reference SQL has
  no DISTINCT, so a customer with N matching returns rows shows up N
  times (sf=1 returns 16 rows, including one customer twice).

  Modelling pieces added:
  - `income_band.preql` — new dim (id, lower_bound, upper_bound).
  - `household_demographic.preql` — added
    `import income_band as income_band` and a second mapping
    `HD_INCOME_BAND_SK: income_band.id` (alongside the existing scalar
    `income_band_id` mapping). This lets queries traverse
    `customer.household_demographic.income_band.lower_bound` etc.
  - `store_returns.preql` — added
    `import customer_demographic as customer_demographic` and
    `SR_CDEMO_SK: customer_demographic.id` so q84 can match
    `customer.demographics.id` to the return-side cdemo via merge.

  Query mechanics:
  - `merge customer.demographics.id into returns.customer_demographic.id;`
    fuses the two cdemo concepts so the planner emits an INNER JOIN
    `customer.cdemo_sk = returns.sr_cdemo_sk`.
  - Hidden `--returns.store_sales.ticket_number, --returns.item.id` in
    SELECT keep the row grain at (customer.id, ticket, item) instead of
    grouping by (customer_id, customername) — preserves the per-return
    duplicates without exposing the keys in the output.

## Recent Query Additions (q67)

- **q67** — 8-dim ROLLUP with `rank() OVER (PARTITION BY i_category
  ORDER BY sumsales DESC)` across all rolled-up rows. Earlier the q86
  per-level-rank trick didn't extend (one rank ordering crosses every
  level inside a category), so this stayed missing.

  Working shape: a single SELECT with `auto sumsales <- sum(...) by
  rollup ...;` for the aggregate plus an explicit
  `property <8 dims>.rk <- rank target over partition by order;` declaration
  to pin the rank concept's keys. The property keys go into
  `concept.keys`, which `resolve_window_parent_concepts` appends to
  `parent_concepts` — so `gen_window_node` carries every rollup dim
  through the window CTE rather than enriching back via a narrow
  `(target, partition, order_by)` join. Plan: 3 CTEs (base + grouped
  rollup + windowed) and a flat outer SELECT, no enrichment join.

  Caveats:
  - The threshold filter must be `HAVING rk <= 100`, not `WHERE`. Putting
    `rk <= 100` in WHERE makes the planner re-enter the source path with
    `rk` as an additional output, which produces a second ROLLUP and a
    third rank — plan balloons to 5 CTEs and the result is wrong.
  - Without the property declaration, `rank target over X by Y` plans
    the rank with `parent_concepts = (target, X, Y)`, then enriches via
    a 3-key join that's non-unique under ROLLUP (e.g. multiple rolled-up
    rows share `(NULL_category, NULL_target, sumsales)` at sf=1) — the
    Cartesian blowup pushes legitimate rows past `LIMIT 100`.

## Recent Query Additions (q59, q72 batch)

- **q59** — week-over-year-week store sales ratios per day-of-week. wss
  CTE computed as autos `sun_sales`/.../`sat_sales` at `(ss.store.id,
  ss.date.week_seq)` grain via inline `?` filter on `ss.date.day_name`.
  Two rowsets `year1` / `year2` filter weeks by `in_year{1,2}` presence
  flags (max-of-case at the same grain), and merge on
  `store_text_id` + a derived `week_seq + 52` join key. The merge produces
  a `FULL JOIN`, so `1 as year{1,2}_present` flags are projected and
  filtered in the outer SELECT to drop unmatched FULL-JOIN-synthesised
  NULL rows — the documented "FULL JOIN trap" workaround.
  - Uses `sql_override` (`query59.sql`): the reference TPC-DS q59 cross-
    joins wss to `date_dim` on week_seq inside both y/x subqueries,
    producing ~49 duplicate output rows per logical (store, week_seq)
    pair. The override pre-deduplicates the week_seq filter so row-by-row
    comparison is meaningful.
- **q72** — catalog_sales × inventory triple join with bill demographics
  filter (`hd_buy_potential = '>10000'`, `cd_marital_status = 'D'`,
  `d_year=1999`), `inv.date.week_seq = cs.sold_date.week_seq` alignment,
  `date_diff(sold, ship, DAY) > 5` ship-delay filter, and
  `inv.quantity_on_hand < cs.quantity` stock-shortage filter. Modelled
  via `merge inv.item.id into cs.item.id;` plus the four conjunctive
  WHERE conditions. count, no_promo, promo aggregates emitted per
  (item.desc, warehouse.name, sold_date.week_seq) grain. Earlier OOM
  concern (STATUS) was a stale data point — at sf=1 the trilogy plan
  runs in ~1s and the reference PRAGMA matches in ~1s.

## Recent Query Conversions (native ROLLUP — q18, q27)

The new `by rollup col1, col2, ...` aggregate syntax replaces the manual
N-branch `MERGE … align … derive coalesce()` pattern with a single
`GROUP BY ROLLUP` SQL emit.

- **q22, q80** — POC conversions; previously had simple per-level rollups.
- **q18** — converted from 5-level MERGE/align cascade. Each aggregate
  carries its own `by rollup` clause; all share the same dim list, so
  the planner emits one `GROUP BY ROLLUP (...)` over the joined source.
- **q27** — converted from 3-level MERGE/align. Uses `grouping(state)` to
  emit the `g_state` column (1 for L1/L2, 0 for L0).

### Planner fix landed for compatible grouping merging

`gen_group_node` previously rejected merging two aggregates whose
GROUP BY grain matched but whose argument-derived parent grains
differed (e.g. `avg(quantity)` at row grain + `grouping(state)` at
store.id grain). Result: aggregates were split into two ROLLUP CTEs
joined back via FULL JOIN with `=` (not `is not distinct from`) on
non-nullable keys, so grand-total NULL rows didn't match.

Fix in `trilogy/core/processing/node_generators/group_node.py`: when
two aggregates share the same non-standard grouping mode (ROLLUP /
CUBE / GROUPING_SETS) and the same `by` list, they MUST share a
GROUP BY in SQL — so the merge rule loosens to widen parent_concepts
even when the per-arg grains differ. Captured in `_shared_nonstandard_grouping`.

### Not converted

- **q14** (xfailed) — tried single-rowset + ROLLUP shape; hits an
  unrelated planner cycle (`Graph contains a cycle`). The blocker is
  the cross-channel intersect + HAVING interaction, not rollup.
- **q36, q70, q86** — left as per-level MERGE/align. Reference SQL uses
  `rank() OVER (PARTITION BY grouping(...)+grouping(...), CASE WHEN
  grouping(class)=0 THEN i_category END ORDER BY total DESC)`. Two
  obstacles to a single-rollup conversion:
  1. Trilogy's window `PARTITION BY` only accepts concept refs
     (`over_list = concept_lit, …`), not arbitrary expressions.
     Workaround: hoist the CASE/grouping expressions into autos
     (`auto level_key <- grouping(a)+grouping(b);`).
  2. Trilogy's rank syntax requires a per-row "target" concept
     (`rank X over Y by Z`). Bare `rank()` over rollup output has no
     natural target — the rolled-up `(category, class)` tuple is
     unique per row but each individual column is NULL at higher
     levels. Synthesising via `coalesce(class, category, '__total__')`
     parses but the planner can't source the rowset-derived ROLLUP
     concepts in the outer SELECT (same family as the q80 issue
     "Final SELECT alias of rowset-derived concepts hangs").
  Future: extending `over_list` to accept arbitrary exprs OR allowing
  bare `rank()` (no target) would unlock single-rollup conversion.
