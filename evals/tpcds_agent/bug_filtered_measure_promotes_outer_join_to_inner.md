# Bug handoff: a filtered measure on a secondary datasource promotes its join from LEFT OUTER to INNER (drops rows)

## RESOLVED (2026-06-02)

Fixed in `gen_filter_node` (`trilogy/core/processing/node_generators/filter_node.py`):
the disjoint-pushdown branch lifted a `sum(measure ? cond)` predicate into the
*merged* parent relation's WHERE whenever the predicate's concepts were disjoint
from `local_optional`. When `local_optional` held a measure from a *different*
key-joined datasource, that pushed-down WHERE filtered the whole join, collapsing
LEFT OUTER → INNER. The fix adds a `_optional_cosourced_with_content` guard: only
take the disjoint pushdown when a single datasource supplies both the filter
content and every `local_optional` concept; otherwise fall back to
`LEFT OUTER JOIN` + `CASE WHEN cond THEN measure`. Regression test:
`tests/complex/test_filtered_measure_join_type.py`. The repro below is now 3/3 OK,
and `tests/modeling/tpc_ds_duckdb/query05.preql` is an exact match vs `query05.sql`
(see that file's per-measure-gate + coalesce comments). Original notes preserved below.

## Summary

When two measures live on two datasources joined by a key, aggregating them
together correctly emits a **LEFT OUTER JOIN** (keep every row of the primary
side; the secondary measure is NULL where unmatched). Applying a row-level
filter (`sum(measure ? cond)`) to the measure on the **secondary** datasource
**promotes that join to an INNER JOIN**, silently dropping every primary-side row
that has no match on the secondary side. The primary measure is then
**undercounted** — it only sums the rows that happen to also exist on the
filtered side.

This parses and resolves fine — it is a **SQL-generation / planner** bug, not a
modeling error. The filter on the secondary measure should not change the
join type; it should remain a LEFT OUTER JOIN (the filter scopes the aggregate,
not the row population of the other measure).

## Minimal, self-contained repro

`evals/tpcds_agent/repro_filtered_measure_inner_join.preql` (also inline below):

```trilogy
key order_id int;
property order_id.sale_amount float?;
property order_id.return_amount float?;
property order_id.return_flag int?;

datasource sales_src (
    order_id: order_id,
    sale_amount: sale_amount,
)
grain (order_id)
query '''select 1 as order_id, 100.0 as sale_amount
   union all select 2, 200.0
   union all select 3, 300.0''';

datasource returns_src (
    order_id: ~order_id,
    return_amount: return_amount,
    return_flag: return_flag,
)
grain (order_id)
query '''select 1 as order_id, 10.0 as return_amount, 1 as return_flag''';

select
    sum(sale_amount) as sales,
    sum(return_amount ? return_flag = 1) as returns;
```

`trilogy run repro_filtered_measure_inner_join.preql duck_db`

Data: 3 sales (orders 1/2/3 = 100/200/300), 1 return (order 1 = 10).

- **Expected:** `sales=600, returns=10`
- **Actual:** `sales=100, returns=10` — orders 2 & 3 (sales with no return) are dropped.

## What narrows it (from `repro.py`)

The only change between correct and buggy is whether the **secondary** measure is
filtered. The join line in the generated SQL flips accordingly:

| case | select | join emitted | result |
|---|---|---|---|
| 1. no filter | `sum(sale_amount), sum(return_amount)` | `LEFT OUTER JOIN` | `600, 10` ✅ |
| 2. filter both sides | `sum(sale_amount ? sale_flag=1), sum(return_amount ? return_flag=1)` | `INNER JOIN` | `100, 10` ❌ |
| 3. filter return side only | `sum(sale_amount), sum(return_amount ? return_flag=1)` | `INNER JOIN` | `100, 10` ❌ |

So: a filter (`? cond`) on the measure sourced from the secondary datasource is
what promotes `LEFT OUTER JOIN` → `INNER JOIN`. No dates or dimension joins are
required to trigger it.

## Real-world failure (TPC-DS q05)

q05 reports per-store sales **and** returns across store/catalog/web, with sales
filtered by sale date and returns by return date. Against the `all_sales` model
(`tests/modeling/tpc_ds_duckdb/all_sales.preql`), the sale measure
(`ext_sales_price`, from the `*_sales` datasources) and the return measure
(`return_amount`, from the `*_returns` datasources) are two measures on two
datasources joined on `(item.id, order_id, sales_channel)`. The per-date filters
trigger this bug: the planner builds a join tree anchored on the return side
(observed as a `RIGHT OUTER JOIN` to the return `date_dim`, plus a `FULL JOIN` to
the returns fact) that evicts sale rows lacking a matching return. Sales collapse
from the reference 112,458,734 to ~11,140,064 (≈ the returned-sales subset), so
no single-select form of q05 can match `query05.sql`. This blocks q05 across all
four eval legs.

Because the correct semantics are "sales by sale-entity UNION returns by
return-entity, then rollup," the only Trilogy form that currently produces the
right answer is the `merge`/`align` multiselect (each branch sets its own grain
independently, so the two measures never share a single join tree). The single
rollup select — agent-written or hand-written — cannot, purely due to this bug.

## Desired fix

A row-level filter on an aggregate (`sum(x ? cond)`) scopes only that aggregate's
input; it must not change how the measure's source datasource is joined to the
rest of the query. Two independent measures on key-joined datasources should
remain a LEFT OUTER JOIN (or FULL OUTER when both sides may be sparse) regardless
of whether either measure is filtered, so neither side's rows are dropped.

## Repro assets

- `evals/tpcds_agent/repro_filtered_measure_inner_join.preql` — minimal `.preql`,
  runs via `trilogy run … duck_db`.
- `evals/tpcds_agent/repro_filtered_measure_inner_join.py` — the 3-case
  characterization table above (prints the join type + result per case).
