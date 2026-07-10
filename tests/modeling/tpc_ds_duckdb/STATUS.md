# TPC-DS DuckDB Equivalence Suite — Status

This document tracks current TPC-DS coverage against the 99-query DuckDB reference set
(`https://github.com/duckdb/duckdb/tree/main/extension/tpcds/dsdgen/queries`), the
gaps, and known framework limitations encountered while authoring tests in
`test_queries.py`.

## Coverage Summary

| State | Count | Queries |
|---|---|---|
| Passing | 99 | 1, 2 (+02-one, 02-two), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97 (+97-one, 97-two), 98, 99 |
| XFAIL (committed broken for experimentation) | 0 | — |
| Missing (no preql / no test) | 0 | — |

(2-one / 2-two / 97-one / 97-two are alternative phrasings of the same query
exercising different planner paths — they share an SQL reference.)

## Recent Query Additions (q77)

- **q77** — store/catalog/web rollup with per-channel dims that differ
  from q5/q80 (store→s_store_sk, catalog→cs_call_center_sk,
  web→wp_web_page_sk), so unified_sales doesn't fit. Hand-shaped using
  the raw fact imports (catalog_sales, catalog_returns, store_sales,
  web_sales, web_returns) per channel.
  - **Catalog branch CROSS-JOIN broadcast.** The reference does
    `FROM cs , cr` (no join key), so per-id sales/profit get fanned
    out by N_cr_groups and per-id returns/loss broadcast as the cr
    total. Compute cr per call_center (NULL group preserved via
    `coalesce(cr.call_center.id, -1)` sentinel), fold to a single-row
    `cr_totals` rowset with `count(cr_grouped.cr_cc_key) as cr_n_groups`
    + sum totals, then in the cs SELECT do
    `sum(cs.ext_sales_price) * cr_totals.cr_n_groups` and broadcast
    `cr_totals.cr_total_returns` directly. The cs WHERE filter restricts
    only the cs side; the cross-join with the scalar rowset is what
    inflates the per-id values.
  - **Inlining branches into the multi-select is required.** Wrapping
    the catalog branch in its own `with catalog_branch as …` rowset
    and then MERGEing rowsets in the multi-select hits "can only merge
    two datasources if the force_group flag is the same" during merge
    resolution — the catalog rowset's cross-join with `cr_totals` sets
    a different force_group than the store/web rowsets' plain
    aggregations. Inlining the SELECTs directly inside the multi-select
    avoids this.
  - **`numeric(15,2)` casts on every branch's metric columns.**
    `store_sales.preql` declares `ext_sales_price numeric(15,2)` while
    catalog/web use `float`, and `align` requires matching datatypes
    across branches. Cast all sales/returns/profit expressions to
    `numeric(15,2)` so align doesn't reject the merge.
  - **Final ROLLUP via `by rollup channel, id` on the L0 union.**
    The L0 union (3-branch multi-select with align) is wrapped in
    `with l0_union as …`, then the final SELECT does
    `sum(...) by rollup l0_union.u_channel, l0_union.u_id` for each
    metric, with `ORDER BY channel asc nulls first, id asc nulls first,
    returns_ desc`.




## Pre-existing Model Bugs Not Yet Fixed

- `customer.preql` adds `birth_date` via `(year || '/' || month || '/' || day)::date`
  (`||` so a NULL component yields a NULL date — `concat()` skips NULLs and would
  produce an uncastable string). With sf=1, some month/day combinations still
  produce invalid dates (e.g. Feb 30) — DuckDB will throw on cast. Currently safe
  because no test actually selects `birth_date`.


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
  `avg(dim.prop) row-weighted by fact rows`, the naive `avg(catalog_sales.billing_customer.birth_year)`
  computes at customer.id grain (each distinct customer counted once). Use
  `auto row_birth_year <- group catalog_sales.billing_customer.birth_year by
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

(q72 was the last item here; now passing at sf=0.01 via `engine_sf001` —
see `test_seventy_two`. q75 was the other; now passing as a deduped
rowset shape with per-(brand,class,cat,manufact) curr/prev aggregates.)


## Backport Candidates (use unified_sales)

Blocked by missing `unified_sales` features:

- **q10, q35** — reference SQL is asymmetric: `ws_billing_customer_sk` for web but
  `cs_ship_customer_sk` for catalog. unified_sales currently maps both web and
  catalog to bill-side customer. Would need a `ship_customer` concept added to
  unified_sales (mapped from WS_SHIP_CUSTOMER_SK / CS_SHIP_CUSTOMER_SK; falls back
  to SS_CUSTOMER_SK for store).
- **q97-one** — intentional alternative phrasing exercising the merge planner;
  keep as-is.


**q14 — now passing.** Shape that works: tuple_key + cross_tuples rowset
(`count_distinct(sales.sales_channel) = 3` HAVING) + scalar avg_sales rowset
+ single l0_filtered rowset grouping by (channel, brand, class, cat) with
`HAVING bucket_sum > avg_sales`, then a final `SELECT … sum(...) by rollup`
over the l0_filtered concepts. The Shape C blocker (5-rowset merge dropping
the SUM in higher rollup levels) is bypassed entirely by using `sum() by
rollup` on the single l0_filtered rowset — same pattern q77 uses on
l0_union. Notes:
- HAVING in a rowset only resolves concepts that appear in the SELECT
  projection. `--cross_channel_count,` and `--avg_sales.average_sales,`
  are listed in the respective rowset SELECTs so HAVING can reference
  them. (Same rule q31/q65 documented.)
- `fact_row_one <- count(sales.order_id) by sales.sales_channel,
  sales.order_id, sales.item.id` pins the count to row grain so the
  outer `sum(fact_row_one)` reproduces `count(*)` correctly across the
  unified union.


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

### q36/q70/q86 — converted to single-rollup form

All three now passing as a single `with rowset as ... SELECT ... rank()
over (partition by … order by …)` shape, replacing the prior 3-level
MERGE/align expansion. The crucial planner-quirks discovered while
landing this:

- **Multiple `by rollup` aggregates in one SELECT get split across CTEs.**
  When two `sum(...) by rollup` aggregates (or `sum + grouping`) live in
  the same SELECT/rowset and the planner sees that they need different
  base columns, it materializes them into separate rollup CTEs and joins
  them. The join uses `=` (not `IS NOT DISTINCT FROM`) on the rollup
  dims, so the L2 grand-total row (all-NULL dims) drops out. Workaround:
  put all the aggregates inside a rowset with the underlying sums and
  grouping bits hidden (`--`), and expose only the derived columns
  (gross_margin, lochierarchy, partition_cat). The hidden form keeps the
  planner from treating the two sums as separately-projectable outputs.
  See `query36.preql` for the canonical example.
- **`grouping()` is needed when source data may contain NULLs in the
  rollup dims (q36, q86).** Detecting "is this NULL because of rollup or
  data?" via `IS NULL` on the rollup output gives the wrong answer when
  the dim itself can be NULL (item.category has 65 NULL rows at sf=1).
  When the dims are guaranteed non-null (store.state, store.county for
  q70), the NULL-pattern trick is fine and avoids the planner split
  entirely — q70 uses this and ends up with a 2-CTE plan.
- **PARTITION BY accepts arbitrary expressions** via `expr_over_list`
  in the SQL-style window grammar — `rank(...) over (partition by
  grouping(a)+grouping(b), CASE WHEN ... THEN ... END order by ...)`
  parses fine. (The pest binary needed `maturin develop` to pick up the
  May 11 grammar change; older builds reject expression partitions
  with "expected window_sql_order or OVER_COMPONENT_REF".)

q36/q70/q86 collectively shed the `coalesce(...rank_a, rank_b, rank_c)`
align/derive scaffolding, reducing each preql to ~50 lines.
