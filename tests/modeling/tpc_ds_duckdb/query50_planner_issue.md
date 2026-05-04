# query50: bucket-fusion regression in planner

## Summary
After rewriting `query50.preql` from `sum(case when ... then 1 else 0 end)` to the
more semantically explicit `sum(count(item.id ? <bucket-filter>) by ticket_number, store.id)`
pattern (matching the shape used in `query88.preql`), the generated SQL grows from
**4163 → 12148 chars** and the test size assertion had to be raised from `< 5000`
to `< 13000`.

Output is correct (matches `PRAGMA tpcds(50)` for all 6 stores). The blow-up is
purely a planner-shape problem.

## Reproduction
- Branch: `tpc_ds_three`
- File: `tests/modeling/tpc_ds_duckdb/query50.preql`
- Test: `tests/modeling/tpc_ds_duckdb/test_queries.py::test_fifty`

The function is:
```preql
def bucket_count(low, high) -> sum(
    count(store_sales.item.id ? days_to_return > low and days_to_return <= high)
        by store_sales.ticket_number, store_sales.store.id
) by store_sales.store.id;
```
called five times with `(-1,30)`, `(30,60)`, `(60,90)`, `(90,120)`, `(120,99999)`.

## What the planner does well
4 of the 5 buckets fuse correctly. They share a single source CTE and the per-ticket
`count(case when ... then item_id else null end)` is computed in one pass:
```sql
cooperative as (SELECT ... GROUP BY 1,2,3,4,5),         -- per-(ticket,item,...) source
questionable as (SELECT store_id,
    count(case when ... > -1 and ... <= 30  ...) as bucket_a,
    count(case when ... > 30 and ... <= 60  ...) as bucket_b,
    count(case when ... > 60 and ... <= 90  ...) as bucket_c,
    count(case when ... > 90 and ... <= 120 ...) as bucket_d
    FROM cooperative GROUP BY store_id, ticket_number),
```

## What goes wrong
The 5th bucket (`days_120_plus`) gets its own parallel pipeline (`young` → `sparkling`
→ `sweltering`). The two source CTEs (`cooperative` vs `young`) are **identical in
JOINs and WHERE**, but `young` adds extra GROUP BY columns:
```sql
cooperative GROUP BY 1,2,3,4,5
young       GROUP BY 1,2,3,4,5,
                     "store_sales_return_date_date"."D_MOY",
                     "store_sales_store_returns"."SR_CUSTOMER_SK",
                     "store_sales_store_sales"."SS_CUSTOMER_SK",
                     cast("store_sales_return_date_date"."D_YEAR" as int)
```
i.e., the WHERE-clause condition columns (`year=2001`, `moy=8`,
`customer.id = return_customer.id`) are kept as group-by keys for one bucket and
elided for the others. Then four extra `INNER JOIN` CTEs (`yummy`, `juicy`,
`vacuous`, `concerned`) are stacked to weld the split-off bucket back to the
fused four — most of the 12k SQL chars are these stacked store_id-keyed joins.

## Hypothesis
This looks related to commit `2f47f425 further_predicate_pushdown` (and
`15e9b375 hide-fixes`). The pushdown pass appears to decide bucket-by-bucket
whether to push WHERE-condition columns down into the per-row representation
(adding them to the inner GROUP BY); for some reason the 5th call comes out the
opposite way to the first four, even though all five `bucket_count` calls have
the same predicate shape (`days_to_return > L and days_to_return <= H`) and the
outer WHERE is identical.

Reordering the `bucket_count` calls in SELECT does **not** change which one
splits — the `days_120_plus` bucket consistently splits regardless of position,
suggesting it's tied to the literal values, not call order.

The `99999` upper sentinel was tried because TPC-DS date-sk diffs never reach it,
but that's already the same shape as the other four — swapping it for any other
literal does not change behavior.

## Why `query88.preql` doesn't hit this
Same `sum(count(item.id ? ...) by ticket_number)` pattern, 8 buckets, fuses cleanly
into a single CTE (gen_length 3833). Two relevant differences:
1. `query88` has no outer grouping dims in the SELECT (no store fields), so the
   outer aggregation is grain-less and there's nothing to FULL/INNER-join across
   buckets even if they did split.
2. `query88`'s bucket predicates reference dim attributes directly
   (`time.hour = h and time.minute >= ...`); `query50`'s reference a derived
   value (`return_date.id - date.id`).

## Suggested investigation
1. Inspect the predicate-pushdown pass to see why it chooses different group-by
   shapes for sibling `count(... ? ...) by` aggregations whose predicates differ
   only in literal bounds.
2. Check whether the issue is per-bucket (one specific predicate confuses it) or
   triggered at >4 buckets (some threshold/heuristic).
3. Swap the derived `days_to_return` for an inline expression to see if the
   `auto`-pass plays a role (size was within ~700 chars either way, so probably
   not the root cause, but worth confirming).

## Workaround currently in place
- Test assertion in `test_fifty` raised from `< 5000` to `< 13000`.
- Output correctness verified against `PRAGMA tpcds(50)`.
