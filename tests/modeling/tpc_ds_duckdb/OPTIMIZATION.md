# TPC-DS Generated SQL Optimization Notes

This document tracks repeated generated-SQL patterns observed in the
`zquery*.log` outputs. It is intentionally query-plan focused: each item should
map to a planner or renderer improvement, not just a one-off query rewrite.

## Priority Patterns

### 1. No-op or over-broad GROUP BY

Many generated CTEs emit `GROUP BY` without any local aggregate. A scan across
the generated logs found roughly 165 such blocks across 56 parsed queries.
Some are intentional dedupe at a hidden grain, but many are projection-only
CTEs that group by every selected output.

Examples:

- `zquery53.log` final SELECT groups by `1, 2, 3` after joining two already
  aggregated CTEs. There is no outer aggregate, so this is likely a no-op
  dedupe:
  `zquery53.log`, final `GROUP BY`.
- `zquery59.log` has repeated full-join projection CTEs (`divergent`,
  `sparkling`, `busy`, `abhorrent`) with `GROUP BY` over every selected field.
- `zquery64.log` has raw projection CTEs (`busy`, `kaput`, `scrawny`) grouped
  by all projected columns before later aggregation.
- `zquery09.log` has one-column CTEs grouped by the projected expression plus a
  hidden ticket-number grain, e.g. `GROUP BY 1, thoughtful.store_sales_ticket_number`.

Potential optimizer work:

- Distinguish required distinct-at-grain behavior from accidental grouping.
- Remove `GROUP BY` when the upstream grain already guarantees uniqueness.
- Render explicit `SELECT DISTINCT` only when the plan truly needs dedupe.
- Avoid carrying hidden grouping expressions into projection CTEs when they are
  only artifacts of a filtered aggregate expansion.

### 2. Null-rejected LEFT/FULL joins

Several `LEFT OUTER JOIN`s are followed by `WHERE` predicates that reference
right-side columns. Those predicates null-reject the right side, so the join is
semantically an inner join. The same shape appears with `FULL JOIN`, where a
post-join predicate requires both sides.

Examples:

- `zquery64.log` CTE `busy` left-joins customer and demographics tables, then
  filters right-side fields in the same `WHERE`, including
  `ss_customer_customers.C_CURRENT_ADDR_SK is not null` and demographic-status
  comparisons.
- `zquery64.log` final SELECT uses `FULL JOIN charming`, then filters
  `charming.cnt_00 <= divergent.cnt_99`, which null-rejects both sides.
- `zquery47.log` CTE `thoughtful` left-joins `date_dim`, then filters
  `date_dim` fields in `WHERE`.
- `zquery57.log` has the same date-dimension shape as q47.

Potential optimizer work:

- Add a null-rejection analysis pass after predicates are attached.
- Convert null-rejected `LEFT` joins to `INNER`.
- Convert null-rejected `FULL` joins to `INNER` where both sides are required.
- Prefer attaching right-side filters in join planning so join type can be
  selected correctly up front.

### 3. Repeated base scans with shared predicates

Some queries scan the same fact/dimension shape multiple times with nearly the
same filters. q64 is the clearest case: it builds separate CTEs for year 2000,
year 1999 product names, and year 1999 address/profit outputs, all repeating
the same expensive item/customer/return filters.

Examples:

- `zquery64.log` CTEs `busy`, `kaput`, and `scrawny` repeat joins across
  `store_sales`, `date_dim`, `store_returns`, `item`, customer, address, and
  demographic tables.
- `zquery59.log` builds multiple store/week CTEs over `store_sales`,
  `date_dim`, and `store` with shared `store_id` and `week_seq` filters.
- `zquery14.log` repeats date/item filtered scans for related sales branches.
- `zquery33.log`, `zquery56.log`, and `zquery60.log` repeat similar
  channel-union branches with identical date/item/address filters.

Potential optimizer work:

- Extract shared fact filters into a base CTE before branch-specific projection.
- Use a common subexpression key that includes source table set, join graph, and
  pushable predicates.
- Split branch-specific predicates, such as year, after shared filters are
  applied.
- Be conservative around row-multiplying joins; only extract shared CTEs when
  the join graph and grain are identical or provably compatible.

### 4. Semijoin and anti-semijoin rendering

The generator frequently emits membership filters as:

```sql
column in (
  select cte.value
  from cte
  where cte.value is not null
)
```

This is correct, but it hides a semantic semijoin behind SQL text and makes
dedupe/filter pushdown harder to reason about.

Examples:

- `zquery64.log` filters item ids through `questionable`.
- `zquery02.log` and `zquery2.1.log` filter relevant week sequences through a
  CTE.
- `zquery23.log` filters each sales channel by frequent item and best customer
  CTEs.
- `zquery69.log` combines `IN` and `NOT IN` filters for store/web/catalog buyers.
- `zquery94.log` and `zquery95.log` filter multi-warehouse and returned-order
  sets.

Potential optimizer work:

- Represent `IN` as a semijoin node before SQL rendering.
- Represent `NOT IN` / anti-membership as an anti-semijoin node with explicit
  null semantics.
- Deduplicate semijoin inputs once, then reuse them across branches.
- Push semijoins as close as possible to the fact scan that owns the filtered
  key.

### 5. Scalar/cartesian FULL JOINs

Several single-row or scalar result CTEs are combined with `FULL JOIN ... on
1=1`. This tends to appear in bucketed aggregate queries and presence-flag
queries.

Examples:

- `zquery09.log` combines bucket aggregate CTEs using repeated `FULL JOIN ... on
  1=1`.
- `zquery59.log` uses constant-present CTEs (`year1_present`,
  `year2_present`) and joins them via `on 1=1`.
- `zquery77.log` has multiple scalar/full joins over channel totals.
- `zquery76.log`, `zquery61.log`, `zquery66.log`, and `zquery95.log` show
  similar scalar-combine shapes.

Potential optimizer work:

- Track scalar cardinality explicitly in the plan.
- Render scalar combines as `CROSS JOIN` or a single projection over scalar
  subqueries when null-extension semantics are not required.
- Avoid `FULL JOIN on 1=1` unless the plan explicitly needs row preservation
  for missing scalar inputs.

### 6. Verbose existence predicates

Some existence checks render as boolean `CASE` expressions instead of simpler
null checks.

Examples:

- `zquery64.log`, `zquery17.log`, `zquery24.log`, `zquery25.log`, and
  `zquery29.log` include patterns like:

```sql
CASE WHEN table.return_time_key THEN TRUE ELSE FALSE END
```

For key columns, this usually wants `table.return_time_key IS NOT NULL`. For
boolean columns, the column can usually be rendered directly.

Potential optimizer work:

- Normalize key-existence checks to `IS NOT NULL`.
- Normalize boolean identity checks to the boolean expression itself.
- Run this normalization before null-rejection analysis so join simplification
  can see the predicate.

## High-value Query Targets

### q64

Most useful optimization sandbox. It shows repeated fact scans, null-rejected
left joins, no-aggregate grouping, semijoin membership filters, and a final
null-rejected full join.

Relevant generated shapes:

- `busy`, `kaput`, `scrawny` repeat most of the same base joins and predicates.
- Demographic and customer joins are `LEFT OUTER JOIN` but are required by
  `WHERE`.
- The final `FULL JOIN charming` is filtered by both sides' metrics.

### q59

Good target for dedupe/grouping cleanup and scalar-presence handling.

Relevant generated shapes:

- `cheerful`, `late`, and `uneven` are projection/dedupe CTEs over shared
  store/week data.
- Later `FULL JOIN` projection CTEs group by all projected columns.
- Constant `year1_present` / `year2_present` flags are joined through scalar
  CTEs.

### q09

Good target for filtered aggregate expansion.

Relevant generated shapes:

- One base scan computes bucket averages.
- Separate CTEs count bucket ticket numbers, each grouping by a hidden ticket
  grain.
- Scalar bucket outputs are combined with `FULL JOIN ... on 1=1`.

### q53

Small, easy case for final no-op grouping.

Relevant generated shape:

- The final SELECT joins quarterly sums with manufacturer averages and then
  groups by all selected outputs without any aggregate.

## Suggested Implementation Order

1. Add a plan-level no-op `GROUP BY` detector and make it report-only first.
2. Add null-rejection analysis for joined aliases and convert eligible joins to
   inner joins.
3. Normalize existence predicates (`CASE WHEN key THEN TRUE ELSE FALSE END`) to
   `IS NOT NULL`.
4. Model semijoin/anti-semijoin filters explicitly instead of rendering ad hoc
   `IN (SELECT ...)`.
5. Add common base-scan extraction for repeated branches with identical join
   graphs and compatible grains.
6. Add scalar-cardinality tracking to replace unnecessary `FULL JOIN on 1=1`.

## Notes

- Be careful with rollups. `STATUS.md` documents q36/q70/q86 planner behavior
  where hidden grouping bits and rollup NULL semantics matter. Any group-by
  cleanup pass must preserve rollup grain and `grouping()` behavior.
- Be careful with `NOT IN` and NULLs. Anti-semijoin rewrites must preserve SQL
  null semantics unless the filtered key is known non-null.
- DuckDB may already optimize some of these patterns internally, but simplifying
  the generated SQL still improves portability, planner predictability, and
  explainability.


## Query Size Minimization

query67.preql - let a bare rank/rollup inherit the grain dimensions from the 
select, same as a bare aggregate. saves redefining those.


##   q09, q77, q83