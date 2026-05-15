# TPC-DS Generated SQL Optimization Notes

This document tracks optimization work that is likely to improve measured
TPC-DS execution time. It intentionally weights patterns by current timing
regressions against DuckDB's reference SQL, not by how strange the SQL looks.

Dotted variant query ids such as `97.1` are excluded from the priority ranking;
those are intentionally testing alternate shapes and are known-bad performance
cases.

Current main-query timing snapshot:

| Query | Trilogy | Reference | Delta | Ratio | Primary suspected cause |
| --- | ---: | ---: | ---: | ---: | --- |
| q09 | 0.478s | 0.039s | +0.439s | 12.25x | Filtered aggregate expansion |
| q97 | 0.382s | 0.068s | +0.314s | 5.62x | Late date filtering + broad merge |
| q28 | 0.216s | 0.043s | +0.174s | 5.04x | Wide conditional aggregate scan |
| q66 | 0.305s | 0.143s | +0.162s | 2.13x | Very large generated join/aggregate plan |
| q78 | 0.304s | 0.188s | +0.116s | 1.62x | Multi-branch sales/returns alignment |
| q65 | 0.202s | 0.090s | +0.112s | 2.24x | Repeated scans + late dimension joins |
| q73 | 0.142s | 0.037s | +0.105s | 3.86x | Repeated filtered store-sales graph |
| q25 | 0.135s | 0.041s | +0.094s | 3.31x | Outer-join merge instead of direct join graph |
| q85 | 0.709s | 0.623s | +0.087s | 1.14x | Large but near-reference shape |
| q29 | 0.163s | 0.078s | +0.085s | 2.10x | Outer-join merge instead of direct join graph |
| q81 | 0.117s | 0.032s | +0.084s | 3.60x | Wide CTE/grouping expansion |

## Highest-impact Patterns

### 1. Filtered aggregate expansion can be slower than filtered branches

Queries q09 and q28 are the clearest measured regressions. The generated shape
tries to compute many filtered aggregates from one broad scan using large
`CASE WHEN ... THEN value ELSE NULL END` expressions. DuckDB's reference SQL
uses separate filtered scalar subqueries or branch subqueries.

This is counterintuitive: one broad scan looks cheaper, but the generated plan
does more per-row expression work, materializes wide intermediate columns, and
can force `count(distinct CASE ...)` over a larger input than necessary.

Examples:

- `zquery09.log` builds a `highfalutin` CTE with 15 filtered virtual columns,
  then five scalar aggregate CTEs read from it. This is the largest main-query
  regression: +0.439s, 12.25x slower than reference.
- `query09.sql` uses scalar subqueries filtered per quantity bucket. Despite
  repeated references to `store_sales`, DuckDB executes this much faster.
- `zquery28.log` computes all bucket metrics in one SELECT with repeated
  `CASE` filters and `count(distinct CASE ...)` expressions over
  `ss_quantity between 0 and 30`.
- `query28.sql` computes each bucket in its own filtered subquery and combines
  the scalar results.

Potential optimizer work:

- Add a cost heuristic for filtered aggregate fanout. When many filtered
  aggregates have disjoint selective predicates, prefer branch-local filtered
  aggregate CTEs over one wide CASE-projection scan.
- For `count(distinct CASE WHEN predicate THEN x END)`, consider rendering as a
  filtered subquery with `count(distinct x)` when the predicate is selective.
- Avoid materializing wide virtual-filter CTEs when every consumer immediately
  aggregates them.
- Treat q09 and q28 as the first benchmark pair for this pass.

### 2. Push filters before merge/align joins

q97 is the second-largest main-query regression. The reference query filters
store/catalog sales by month before grouping customer-item pairs, then full
joins the two small grouped sets. The generated SQL first dedupes broader
store/catalog rows, merges through item/customer/date dimensions, and only then
applies month filters as `CASE` expressions.

Examples:

- `zquery97.log` materializes sales rows, joins date later, then computes
  `store_sales` and `catalog_sales` flags from month-filtered CASE expressions.
- `query97.sql` applies `d_month_seq between 1200 and 1211` inside each sales
  CTE before grouping and full joining.

Potential optimizer work:

- Push relation-local filters into each branch before MERGE/align.
- Prefer grouping branch keys before joining dimensions that are only needed
  for filters.
- Detect "presence by filtered branch" patterns and render as filtered branch
  CTEs plus an outer join, not as a broad merged rowset with CASE flags.
- Use q97 as the first regression test for filter-before-merge planning.

### 3. Avoid outer-join merge shapes when the reference is a direct inner join

Several slower queries build a broad nullable merge with `LEFT`, `RIGHT`, or
`FULL` joins, then apply predicates that require rows from both sides. This is
not just cosmetic: it can prevent join reordering, delay filters, and preserve
too many nullable rows before aggregation.

Examples:

- `zquery25.log` creates two separate branches (`vacuous`, `yummy`) over
  `store_sales`, `store_returns`, `catalog_sales`, `date_dim`, `store`, and
  `item`, using left joins that are null-rejected by later date/customer/return
  predicates. `query25.sql` is a direct inner join over the same tables with one
  aggregate.
- `zquery29.log` has the same broad TPC-DS q25/q29 family shape.
- `zquery17.log` uses nullable joins across store sales, returns, catalog
  sales, date, and item, with filters that require return/catalog dates.
- `zquery65.log` uses `FULL JOIN abundant` and `RIGHT OUTER JOIN vacuous`, but
  the final `WHERE juicy.item_revenue <= 0.1 * vacuous.store_avg_revenue`
  requires the revenue side.

Potential optimizer work:

- Add null-rejection analysis for joined aliases and convert eligible outer
  joins to inner joins.
- Run existence-predicate normalization before join simplification so patterns
  like `CASE WHEN return_key THEN TRUE ELSE FALSE END` become visible as
  `return_key IS NOT NULL`.
- When all measures come from a shared fact grain, prefer one direct join graph
  plus grouped measures over separately aggregated branches joined later.
- Use q25/q29/q17/q65 as validation cases.

### 4. Repeated filtered fact graphs are expensive

q65 and q73 show repeated scans over the same filtered store-sales shape. These
are not as dramatic as q09/q97, but they are consistent regressions and likely
generalize.

Examples:

- `zquery65.log` computes item revenue, then separately rebuilds store and item
  dimension CTEs from `store_sales` with the same date range. `query65.sql`
  computes the revenue subquery once, computes the store average from it, then
  joins `store` and `item` directly at the end.
- `zquery73.log` builds `cooperative`, `abundant`, `yummy`, and `questionable`
  around the same `store_sales` + `date_dim` + `store` +
  `household_demographics` filter. `query73.sql` performs one filtered grouped
  aggregate, then joins `customer`.

Potential optimizer work:

- Extract shared filtered fact graphs into one CTE when the source table set,
  join graph, and predicates match.
- Delay decorative dimension columns until after the selective aggregate when
  the reference only needs them for final projection/order.
- Prefer "aggregate first, decorate later" for queries whose output grain is
  narrower than the base fact grain.

### 5. Over-broad GROUP BY is a secondary performance issue

The generated SQL contains many `GROUP BY` blocks without local aggregates.
This should still be cleaned up, but timing suggests it is usually not the
first-order runtime driver unless it appears inside one of the expensive shapes
above.

Examples:

- `zquery53.log` final SELECT groups by `1, 2, 3` after joining two already
  aggregated CTEs. This is a small, clean correctness-preserving cleanup case,
  not a major runtime target.
- `zquery59.log` has projection CTEs grouped by every selected column.
- `zquery64.log` groups raw projection CTEs before later aggregation, but q64 is
  not currently a top measured regression.

Potential optimizer work:

- Add a report-only no-op GROUP BY detector.
- Remove `GROUP BY` when upstream grain guarantees uniqueness.
- Keep distinct-at-hidden-grain behavior explicit; do not remove grouping just
  because the local SELECT has no aggregate.

### 6. Scalar `FULL JOIN on 1=1` is probably cleanup, not priority

Scalar/cartesian `FULL JOIN ... on 1=1` looks odd and appears in q09, q59,
q77, q76, q66, and others. It is probably not the main runtime driver when both
inputs are single-row aggregate CTEs. In q09, the expensive part is the wide
filtered aggregate expansion, not the final five-row scalar combine.

Potential optimizer work:

- Track scalar cardinality and render scalar combines as `CROSS JOIN` or a
  single projection when null-extension semantics are not required.
- Keep this behind the filtered-aggregate and filter-pushdown work.

## Suggested Implementation Order

1. Filtered aggregate fanout heuristic for q09/q28.
2. Filter-before-merge planning for q97-style branch presence queries.
3. Null-rejection analysis and outer-to-inner join simplification for
   q25/q29/q17/q65.
4. Shared filtered fact-graph extraction for q65/q73.
5. Aggregate-first/decorate-later planning for q65/q73 and similar dimension
   projection queries.
6. Report-only no-op GROUP BY detector, then selective cleanup.
7. Scalar-cardinality rendering cleanup for `FULL JOIN on 1=1`.

## Query Size Minimization

`query67.preql`: let a bare rank/rollup inherit the grain dimensions from the
select, same as a bare aggregate. This saves redefining those dimensions.

## Watchlist

- q09, q28: filtered aggregate rendering.
- q97: branch filter pushdown before merge.
- q25, q29, q17, q65: null-rejected outer joins and direct join graph planning.
- q73, q65: repeated fact graph extraction.
- q66, q78, q81, q77, q75: large CTE expansions that need more targeted
  inspection after the first passes land.

## Notes

- Be careful with rollups. `STATUS.md` documents q36/q70/q86 planner behavior
  where hidden grouping bits and rollup NULL semantics matter. Any group-by
  cleanup pass must preserve rollup grain and `grouping()` behavior.
- Be careful with `NOT IN` and NULLs. Anti-semijoin rewrites must preserve SQL
  null semantics unless the filtered key is known non-null.
- DuckDB may already optimize some generated shapes internally. Prioritize
  changes that improve the slower-than-reference queries above, not just SQL
  aesthetics.
