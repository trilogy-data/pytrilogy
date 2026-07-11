# Handoff: q18 hidden `grouping()` ORDER BY renders invalid `MIN(grouping(...))`

## Summary

TPC-DS q18 exposes a compiler bug in the canonical hidden-order-helper pattern.
A `grouping(dim)` indicator projected hidden from a `by rollup (...)` query and
referenced by alias in `ORDER BY` is rewritten as `MIN(grouping(dim))` in the
same grouped SELECT. DuckDB rejects that expression:

```text
Binder Error: GROUPING function is not supported here
```

Making the indicators visible executes successfully but changes the requested
output shape. Omitting them avoids the compiler bug but loses the specified
rollup tie-breaking order.

## Artifact

Current enriched candidate:

```text
evals/tpcds_agent/results/20260709-105517_enriched/workspace/query18.preql
```

Reference:

```text
tests/modeling/tpc_ds_duckdb/query18.sql
```

The candidate already has the corrected population filter:

```preql
cs.billing_customer.demographics.sk is not null
```

With visible grouping indicators, all measure values match the reference, but
the result has 14 columns instead of the requested 11.

## Trigger

The rollup query needs grouping indicators only for ordering:

```preql
select
    cs.item.id,
    cs.billing_customer.address.country,
    cs.billing_customer.address.state,
    cs.billing_customer.address.county,
    --grouping(cs.billing_customer.address.country) as g_country,
    --grouping(cs.billing_customer.address.state) as g_state,
    --grouping(cs.billing_customer.address.county) as g_county,
    avg(cs.quantity) as avg_qty
by rollup (
    cs.item.id,
    cs.billing_customer.address.country,
    cs.billing_customer.address.state,
    cs.billing_customer.address.county
)
order by
    cs.billing_customer.address.country nulls first,
    cs.billing_customer.address.state nulls first,
    cs.billing_customer.address.county nulls first,
    cs.item.id nulls first,
    g_country nulls first,
    g_state nulls first,
    g_county nulls first
limit 100;
```

The important shape is:

1. `grouping()` is a hidden SELECT output.
2. The query has `by rollup`.
3. `ORDER BY` references the hidden aliases.
4. There is no HAVING or other condition forcing a downstream wrapper CTE.
5. The source is first materialized in a filtered projection CTE.

## Actual SQL

The generated rollup itself is valid:

```sql
GROUP BY ROLLUP (1, 2, 3, 4)
```

But the hidden grouping aliases are not materialized and referenced by name.
Instead the ORDER BY renders fresh aggregate wrappers:

```sql
ORDER BY
    ...,
    MIN(grouping(country)) ASC NULLS FIRST,
    MIN(grouping(state)) ASC NULLS FIRST,
    MIN(grouping(county)) ASC NULLS FIRST
```

`grouping()` is already a grouping-set indicator evaluated for the grouped row.
It must not be nested inside `MIN()` (or any other ordinary aggregate).

## Controls

### Visible indicators

This executes:

```preql
grouping(country) as g_country
```

but returns the helper column to the user. In q18 that produces 14 columns
instead of 11.

### Hidden indicators with a downstream wrapper

Existing coverage passes when a HAVING condition forces a groupless downstream
wrapper:

```text
tests/engine/test_duckdb.py::test_projected_grouping_in_rollup_orders_by_alias
tests/engine/test_duckdb.py::test_inline_grouping_in_order_by_resolves_to_projected_alias
```

Those tests verify that the wrapper orders by a materialized alias and does not
re-emit `grouping()` after grouping. They do not cover the direct-rollup shape
with no wrapper, which is where q18 fails.

### Omitted indicators

Removing both the hidden outputs and their ORDER BY terms executes and matches
the q18 reference multiset at this dataset/limit. It is not a semantic fix:
the prompt explicitly requests the grouping indicators as ordering
tie-breakers, and another dataset may select a different limited slice.

## Expected contract

Hidden outputs participate in planning and ordering exactly like visible
outputs; `--` changes only final result visibility.

For a hidden grouping indicator ordered by alias, the compiler should either:

1. materialize `grouping(dim) AS g_dim` in the rollup SELECT and order that
   SELECT by `g_dim`; or
2. introduce a projection wrapper that removes `g_dim` from the returned
   columns while ordering by the already-materialized alias.

It must never reinterpret the hidden indicator as a responsive aggregate and
wrap it in `MIN()`.

## Likely root area

The direct grouped-query ORDER BY resolution appears to lose the projected
identity of the hidden `grouping()` concept. It then treats the referenced
concept as an aggregate that must be reduced to the SELECT grain, producing
`MIN(grouping(...))`.

Relevant areas include:

```text
trilogy/parsing/v2/select_finalize.py
    grouping projection promotion / ORDER BY expression matching

trilogy/core/processing/node_generators/group_node.py
trilogy/core/processing/v4_helper/group_behaviors.py
    grouping-set identity and aggregate co-sourcing

trilogy/dialect/base.py
    grouped output and ORDER BY rendering
```

Compare the no-wrapper path with the passing HAVING/wrapper tests. The fix
should preserve the hidden projection alias through direct group-node
rendering, rather than adding a generic aggregate wrapper.

## Required fix

1. Preserve hidden `grouping()`/`grouping_id()` outputs as native outputs of
   their owning ROLLUP/CUBE/GROUPING SETS node.
2. Resolve an ORDER BY alias to that exact projected output.
3. Never apply standard responsive-aggregate reduction (`MIN`, `MAX`, etc.) to
   a grouping-set indicator.
4. Ensure the helper remains absent from the externally returned columns.
5. Preserve the existing wrapper-path behavior and inline-expression alias
   matching tests.

## Regression coverage

Add a direct-rollup execution test with no HAVING:

```preql
select
    brand,
    class,
    sum(amount) as total,
    --grouping(brand) as gb,
    --grouping(class) as gc
by rollup (brand, class)
order by gb asc, gc asc, brand nulls first, class nulls first;
```

Assert:

```python
assert "MIN(grouping(" not in sql
assert "MAX(grouping(" not in sql
assert len(rows[0]) == 3
```

Also cover:

- direct source versus a filtered projection parent CTE;
- alias ORDER BY versus an inline `grouping(dim)` ORDER BY matched to a hidden
  projected twin;
- one and multiple grouping indicators;
- ROLLUP, CUBE, and explicit GROUPING SETS;
- queries with and without LIMIT;
- real NULL values in grouping dimensions;
- the existing HAVING-forced wrapper shape as a non-regression control.

