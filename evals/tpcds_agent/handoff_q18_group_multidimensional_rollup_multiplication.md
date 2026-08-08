# Q18 handoff: `group(...)` measure multiplies multidimensional rollup rows

## Fresh source

This was observed in the enriched TPC-DS run
`evals/tpcds_agent/results/20260808-151955_enriched`, Trilogy 0.3.317. It is a
different issue from the existing q18 hidden-`grouping()` ORDER BY handoff.

## Problem shape

The documented line-weighting pattern projects a dimension property to the
catalog-sale line grain and then averages it:

```preql
auto row_dependent_count <-
  group(cs.pos_customer_demographic.dependent_count)
  by cs.order_number, cs.item.sk;
```

This worked under a one-dimensional rollup:

```preql
select
  cs.item.id,
  avg(row_dependent_count)
by rollup (cs.item.id);
```

Adding address levels caused repeated rows and large row-count inflation:

```preql
select
  cs.item.id,
  cs.billing_customer.current_address.country,
  cs.billing_customer.current_address.state,
  cs.billing_customer.current_address.county,
  avg(row_dependent_count)
by rollup (
  cs.item.id,
  cs.billing_customer.current_address.country,
  cs.billing_customer.current_address.state,
  cs.billing_customer.current_address.county
);
```

In the agent's diagnostics, the multidimensional version returned 110,615 rows
and repeated identical output dimension tuples. Directly averaging the source
properties avoided the multiplication and the final q18 answer passed.

## Reproduction

```powershell
.\.venv\Scripts\python.exe \
  evals\tpcds_agent\repro_q18_group_rollup_row_multiplication.py
```

The script runs the one-level control and four-level trigger against the saved
scale-factor-1 workspace. It projects `grouping(...)` for every rollup dimension,
counts repeated dimension-plus-grouping tuples, and exits 0 only when rows repeat
at that full rollup grain.

## Thesis

The planner materializes the `group(...) by order_number, item.sk` branch at
line grain, then stitches it independently to one or more rollup-level branches.
For a multidimensional rollup, the stitch does not retain a unique rollup-level
key, so rows from different levels or address combinations cross-multiply. The
one-dimensional control avoids the faulty multi-key/multi-level stitch.

## Investigation checklist

1. Compare generated CTEs for the one-level and four-level queries, especially
   joins between the grouped line-grain value and rollup aggregates.
2. Locate the first CTE where `(item, country, state, county, grouping level)`
   ceases to be unique.
3. Confirm whether `grouping_id(...)` is absent from an internal join key.
4. Reduce to a small in-memory fact with two items, two addresses, and multiple
   line rows.
5. Assert both row uniqueness at the declared rollup grain and correct weighted
   averages at every level.

## Falsification

Reject this thesis if duplicate rows are intentional even after comparing the
dimensions together with all four `grouping(...)` flags. The reproduction
already includes those flags, so natural nulls and rolled-up nulls are distinct.
