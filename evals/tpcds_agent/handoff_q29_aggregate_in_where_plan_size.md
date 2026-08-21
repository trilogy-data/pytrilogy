# handoff: q29's per-row aggregate test renders both scopes of the catalog side

Filed 2026-08-21, when `tests/modeling/tpc_ds_duckdb/query29.{sql,preql}` were rewritten to
drop the spec query's fan-out (see `bug_q29_cross_leg_sink.md` for why). Correctness is
settled and gated; this is the plan-size half, deferred.

## The shape

```trilogy
auto catalog_customer_item_quantity <- sum(
    catalog_sales.quantity ? catalog_sales.sale_date.year in (1999, 2000, 2001)
) by catalog_sales.billing_customer.sk, catalog_sales.item.sk;

where <store filters> and catalog_customer_item_quantity > 0
select <4 dims>, sum(store_sales.quantity), sum(store_sales.return_quantity),
       sum(catalog_customer_item_quantity)
subset join catalog_sales.billing_customer.sk = store_sales.customer.sk
subset join catalog_sales.item.sk = store_sales.item.sk
```

The pinned aggregate is the intersection test AND a projected measure. As a WHERE-level
aggregate it takes the dual-scope split (population twin plus filtered twin), so the
catalog branch renders twice.

## Numbers (sf=1, `zquery29.log`)

| spelling | generated SQL | preql source | correct |
|---|---|---|---|
| old, spec fan-out (pair-summed) | 4,334 | 1,545 | n/a, different semantics |
| shipped: flat, aggregate in WHERE | 14,113 | 1,256 | yes |
| rowset at (group, customer, item), then roll up | 10,614 | 1,743 | yes |
| flat, `having catalog_sales_quantity > 0` | 9,690 | 1,240 | NO - group-level test, keeps purchases with no catalog order |
| flat, `catalog_sales.order_number is not null` | 9,904 | 1,247 | NO - 100 rows; null test reads through the coalescing join key |

`test_twenty_nine`'s size guard was moved 12,000 -> 15,000 to take the shipped spelling.

## What to look at

The two correct spellings differ by ~3.5k of rendered SQL for the same answer, so the
question is whether a WHERE-level pinned aggregate that is ALSO a select-list measure needs
both scopes. It is the same concept at the same grain in both roles here; the population
twin exists to keep the aggregate's own scope unfiltered, which for this query is what the
filtered twin computes too.

The last row is the q59/q77 presence-probe family in
`bug_presence_probe_no_ops_and_q72_axis_residual.md`: it fails for the reason that report
documents, so it is not a spelling to reach for until that one is fixed.
