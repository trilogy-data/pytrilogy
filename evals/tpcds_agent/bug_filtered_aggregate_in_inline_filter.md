# Engine bug handoff: an inline `?` row-filter whose condition compares a FILTERED aggregate generates invalid SQL (DuckDB `Binder Error: GROUP BY clause cannot contain aggregates`)

## Summary

A concept of the form

```
auto keep <- <dim> ? (<agg>(<x> ? <inner_cond>) by <grain>) > <k>;
```

— i.e. a row-filter (`?`) on a dimension whose condition compares a **filtered**
aggregate (`agg(x ? inner_cond) by grain`) — compiles to SQL that puts the
aggregate expression inside a `GROUP BY` clause, which DuckDB rejects:

```
(_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
LINE n:  CASE WHEN ( count("wakeful"."_virt_filter_customer_sk_8358463...
```

The inner `? inner_cond` turns the aggregate's argument into a `_virt_filter_*`
source column; when the whole filtered aggregate is used as the predicate of an
outer `?` row-filter, the renderer groups by the aggregate expression instead of
materializing it (per-`grain`) in a CTE and grouping by `<dim>`.

It is **specifically the inner filter** that triggers it — the identical query
with an *unfiltered* aggregate compiles and runs fine. This is high priority: it
silently blocks the natural "dimension values whose filtered count exceeds K"
idiom (a very common shape) and the only escape is knowing the two-step
workaround.

## Minimal, deterministic repro

Against any ingested TPC-DS workspace (`raw/customer.preql` present):

```
import raw.customer as customer;
auto z <- customer.customer_address.zip
    ? (count(customer.customer_sk ? customer.preferred_cust_flag = 'Y')
        by customer.customer_address.zip) > 10;
select z;
```

```bash
trilogy run repro.preql duckdb
```

→ `Binder Error: GROUP BY clause cannot contain aggregates!`

### What narrows it (tested against `results/20260601-175357_ingest/workspace`)

| variant | result |
|---|---|
| inner aggregate is **filtered**: `count(x ? cond) by grain` in the `?` | ❌ GROUP BY error |
| inner aggregate **unfiltered**: `count(x) by grain` in the `?` (else identical) | ✅ runs |
| SELECT adds/drops `substring(z,1,2)` etc. | irrelevant — fails either way |
| **two-step**: `auto c <- count(x ? cond) by grain; auto z <- dim ? c > k;` | ✅ runs (the workaround) |

So the trigger is precisely: **a filtered aggregate (`agg(arg ? inner_cond) by grain`)
used inline inside an outer `?` row-filter predicate.** Unfiltered aggregate in
the same position is fine; pulling the filtered aggregate out into its own
`auto` concept is fine.

## Generated SQL (smoking gun)

The filtered inner argument becomes `_virt_filter_customer_sk_*`, and the outer
filter renders as a `CASE WHEN (count(_virt_filter_...) ...) ...` that lands in a
`GROUP BY`:

```
SELECT
    "customer_customer"."c_customer_sk" as "_virt_filter_customer_sk_8358463803893042",
    "..."."ca_zip" as "customer_customer_address_zip"
FROM ...
...
LINE 20:   CASE WHEN ( count("wakeful"."_virt_filter_customer_sk_8358463...
GROUP BY ...   -- ← the aggregate ends up here
```

## Where to look

- The node/render path that lowers an outer `?` row-filter whose predicate
  contains an aggregate. For the **unfiltered** case it correctly computes the
  aggregate per `grain` (in a CTE) and groups by `<dim>`; the **filtered** case
  (the aggregate argument carries a `BuildFilter` → `_virt_filter_*` column)
  takes a path that emits the aggregate into the GROUP BY of the wrapping
  select instead. Compare the two plans — the divergence is the filtered
  argument's `_virt_filter_*` projection forcing the aggregate up a level.
- Likely in the filter-node generator / the grain handling for a comparison whose
  operand is a `BuildAggregateWrapper` over a `BuildFilter`.

## Suggested fix / regression

- The inline filtered-aggregate form should compile the same as the two-step
  form (aggregate materialized per grain in a CTE, outer filter groups by the
  dimension) — or, failing that, raise a clean modeling error rather than emit
  invalid SQL.
- Regression: the 3-line repro returns rows; assert the generated SQL has no
  aggregate inside a `GROUP BY` (and no `_virt_filter_*` in a GROUP BY).

## Affected eval queries

- **q08** (`20260601-175357_ingest` and `_enriched`): "5-digit ZIPs where > 10
  preferred customers have their address" is naturally `zip ? (count(cust ?
  preferred_cust_flag = 'Y') by zip) > 10`. Both Trilogy legs hit this, burned
  iterations, and never produced a clean answer. The canonical `query08.preql`
  sidesteps it by defining the filtered count as its own concept first
  (`auto zip_p_count <- count(customer.id ? ...) by customer.address.zip;`).

## Notes

- Observed on `trilogy 0.3.275`.
- The language reference's `in`-against-a-derived-value-set example already uses
  the two-step form, so an agent that follows it verbatim avoids this — but the
  inline form is the more natural first attempt, and emitting invalid SQL for it
  (rather than working or erroring cleanly) is the bug.
- Distinct from the other open handoffs (`bug_aggregate_render_derived_in_by_grain.md`,
  `bug_merge_in_subselect_missing_cte.md`, `recursion_bug_handoff.md`,
  `bug_select_alias_shadows_auto_concept.md`).
