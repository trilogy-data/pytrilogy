# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 97 | 1, 2 (+02-one, 02-two), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| XFAIL (committed broken for experimentation) | 1 | 14 |
| Missing (no preql / no test) | 1 | 77 |

(2-one / 2-two / 97-one / 97-two are alternative phrasings of the same query
exercising different planner paths — they share an SQL reference.)

## Recent Query Additions (q75)

- **q75** — UNION DISTINCT of three per-channel `(year, brand, class, cat,
  manufact, sales_cnt, sales_amt)` row streams, then year-over-year
  comparison. The 7-tuple dedup is expressed as a `with deduped as SELECT
  …` rowset over `unified_sales`; trilogy's implicit GROUP BY at the
  SELECT projection collapses cross-channel duplicates (the SELECT
  DISTINCT semantic).
  - **Planner fix required** (`resolve_function_parent_concepts`):
    when two aggregates over the same named rowset (e.g.,
    `sum(deduped.cnt)` + `sum(deduped.amt)`) plan independently, each
    requests just its own column from the rowset; the discovery loop
    sees the rowset materialization carrying more cols than the
    consumer needs and prunes/regroups to the consumer's tighter grain.
    Two CTEs end up with different `GROUP BY` projections, silently
    splitting the rowset's declared grain across consumers. Fix: when
    resolving an aggregate's parent concepts, if any parent transitively
    wraps a rowset item, also include the rowset's sibling outputs as
    parents (analogous to how property keys are included). Both
    aggregates then request the same parent grain — the rowset's full
    declared grain — so the discovery loop materializes the rowset
    *once* at full grain and CTE dedup shares it across both consumers.
    Walks lineage to handle inline-filter autos (`_virt_filter_*` from
    `sum(x ? cond)`) and other BASIC wrappers around rowset items.
    Regression coverage:
    `tests/engine/test_duckdb.py::test_rowset_full_tuple_dedup_plain_select`
    and `test_rowset_full_tuple_dedup_with_aggregates`.

## Recent Query Additions (q54)

- **q54** — customer set built from catalog/web Women/maternity buyers in
  Dec 1998, then per-customer revenue against store_sales over a 3-month
  window joined to stores via county/state. Two rowsets keyed by
  `(county, state)` get merged: `cust_ss` per-customer ss revenue at
  customer.id grain, and `stores_cs` count of stores per (county, state).
  Final revenue = `ss_revenue * scs_count` materialized in a third rowset
  `my_revenue`, then `cast(round(revenue/50) as int)` bucket + count.
  - **The reference q54 does an INNER cross-join**, not a constrained
    fact↔dim join. `customer_address.ca_county = store.s_county AND
    ca_state = s_state` is a cardinality multiplier (per customer, ss rows
    are duplicated by N_stores in their county/state). Pin this with two
    keyed rowsets + merge as above, NOT with `ss.customer.address.county =
    ss.store.county` (that filter constrains the ss row's natural store,
    giving you ~1/12th the result).
  - **Materializing the final per-customer revenue in a separate rowset is
    required** for the outer `count()` to reflect post-merge cardinality.
    Without it, `count(cust_ss.ss_cust_id)` sourced from `cust_ss` alone
    (pre-merge) counts all customers with any ss in the window — not just
    those whose county/state has matching stores.

## Recent Query Conversions (q29)

- **q29** — previously skipped for "FULL JOIN of store + return + catalog
  produces extra rows" — but the root cause is the same as q50: trilogy's
  store_sales↔store_returns join is on the loose `~ticket_number,~item.id`
  mapping, leaving `ss_customer_sk = sr_customer_sk` unenforced. Adding
  `store_sales.customer.id = store_sales.return_customer.id` to WHERE
  matches the reference and trims the 3-row trilogy result to the 1-row
  reference. Same workaround documented for q50.

## Recent Query Additions (q64)

- **q64** — 16+ table self-join shape with a `cs_ui` filter (items where
  catalog sale > 2× refund). Tested at sf=0.01 via `engine_sf001` fixture
  with `sql_override=True`. Both reference and trilogy return 0 rows at
  sf=0.01, so the test verifies the query parses, plans, and executes
  end-to-end (sf=1 reference PRAGMA OOMs / times out; sf=0.1 reference
  exceeds 5min). Two parallel `cross_99` / `cross_00` rowsets aggregate
  ss + ad1 + ad2 + cd1 + cd2 + hd1 + hd2 + ib1 + ib2 + d1 + d2 + d3 +
  store + item + promotion + cs_ui per (item, store, addr1, addr2,
  year-triple), then merge on `(item_sk, store_name, store_zip)` with
  `cnt_00 <= cnt_99`.

  Model extensions:
  - `customer.preql` — added `first_sales_date` / `first_shipto_date`
    (date imports; `C_FIRST_SALES_DATE_SK` / `C_FIRST_SHIPTO_DATE_SK`).
    Both nullable.
  - `catalog_returns.preql` — added `reversed_charge` /
    `store_credit` (`CR_REVERSED_CHARGE` / `CR_STORE_CREDIT`).
  - `catalog_sales.preql` — added `ext_list_price` (`CS_EXT_LIST_PRICE`).

  Key shape detail:
  - The `cs_ui` filter is expressed via `merge cs.order_number into
    cr.order_number; merge cs.item.id into cr.item.id;` joining
    catalog_sales to catalog_returns (INNER on the loose merge), then
    `auto cs_ui_sale <- sum(cs.ext_list_price) by cs.item.id;` /
    `auto cs_ui_refund <- sum(cs.item.cs_ui_refund_amt) by cs.item.id;`.
    The refund expression is hoisted into a property on `cs.item.id`
    (per the q80 inline-`?`-filter caveat — `coalesce(x,0)+coalesce(y,0)`
    needs to be a property before being summed).
  - `ORDER BY` references *aliased* names (`s11`, `s12`) not the
    rowset-qualified ones — the q73 ORDER BY restriction applies.

## Pre-existing Model Bugs Not Yet Fixed

- `customer.preql` adds `birth_date` via `cast(concat(year/'/'/month/'/'/day) as
  date)`. With sf=1, some customers have `birth_year=NULL` or month/day
  combinations that produce invalid dates (e.g. Feb 30) — DuckDB will throw on
  cast. Currently safe because no test actually selects `birth_date`.


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
- **Rowset-derived alias hang (q80 / q64) — fixed.** Final `SELECT
  alias` of rowset-derived concepts (e.g.
  `q64_results.cnt_99 as cs1cnt`) used to blow up `generate_sql()`
  superlinearly in the count of unaligned "carry-through" columns —
  4 columns took ~11s, 12 columns hung indefinitely. Bare references
  planned in <1s. Two planner fixes landed together:
  - `BuildDatasource.__eq__` / `UnnestJoin.__eq__` /
    `QueryDatasource.__eq__` now align with `__hash__` and compare
    by identifier instead of recursing through nested datasources
    and source maps. The default dataclass-generated eq blew up
    exponentially in nested multi-fact merges.
  - `gen_rowset_node` now memoizes the parent select-node by
    `rowset.name` on `History.rowset_history`, since the search
    loop visits it twice when an outer SELECT mixes an aliased
    rowset concept (BASIC derivation) with bare rowset references.
  After the fix, q64 plans in <1s as a single multi-select with
  `align` (see `query64.preql`).

- **`group <prop> by <keys>` re-grains a dim property (q18).** When you need
  `avg(dim.prop) row-weighted by fact rows`, the naive `avg(catalog_sales.bill_customer.birth_year)`
  computes at customer.id grain (each distinct customer counted once). Use
  `auto row_birth_year <- group catalog_sales.bill_customer.birth_year by
  catalog_sales.order_number, catalog_sales.item.id;` to broadcast the
  property to the row grain, then `avg(row_birth_year)` weights correctly.
  Prefer this over declaring `property <key1, key2>.x <- prop` which can
  hit "Missing source reference" inside multi-fact MERGE selects.
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



## Suggested Next Batch (by complexity)

All single-query gaps that have a clear path are now covered. Remaining
unconverted queries (q14, q75, q77) all need framework work — see
"Not converted" below and the "Backport Candidates" subsection above.

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
- **q77** — same store/catalog/web rollup as q5 in structure, but each
  channel uses a different dim than q5/q80: **store** uses `s_store_sk`
  (same), **catalog** uses `cs_call_center_sk` (q5 uses
  `cp_catalog_page_sk`), **web** uses `wp_web_page_sk` (q5 uses
  `web_site_sk`). The q5 fix to unified_sales (separate `channel_dim_id`
  and `return_channel_dim_id`) doesn't help here because the dim sources
  themselves differ per channel. Worse, q77's catalog branch does a
  CROSS JOIN of cs and cr (no key match), inflating subtotal returns by
  `N_cs`. Would need either: (a) a parallel `channel_dim_id_alt` /
  `return_channel_dim_id_alt` concept set in unified_sales mapping
  store/call_center/web_page, plus cross-join semantics for the catalog
  branch; or (b) a 6-rowset hand-written shape outside unified_sales.
  Both are significant work. Defer.
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
     concepts in the outer SELECT. Worth retrying now that the
     rowset-alias hang has been fixed (see q64 planner notes above).
  Future: extending `over_list` to accept arbitrary exprs OR allowing
  bare `rank()` (no target) would unlock single-rollup conversion.
