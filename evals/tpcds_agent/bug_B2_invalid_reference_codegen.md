# Bug B2: "Invalid reference string" codegen crash — OR-combined membership shape

**Status:** FIXED 2026-06-06 (`trilogy/core/processing/nodes/group_node.py`). Regression test:
`tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_or_membership_with_projected_aggregate`.
**Severity:** medium-high — a valid-looking query renders SQL that references a CTE never
emitted (the `cheerful`/`juicy`/random-word labels, or `INVALID_REFERENCE_BUG`). Top token
outlier on ingest q54 and q49.

## Root cause / fix

`base.py:2316` (`compile_statement`) only *detects* the dangling reference. The real gap was in
`GroupNode._resolve`: when a group node carries a **non-scalar** condition (here, made
non-scalar by the aggregate `cust_total is not null`), it lifts the condition onto a wrapper
SELECT CTE and rebuilds that CTE's source map from `base.output_concepts` only. The membership
feeder datasources (`cat_qual`/`web_qual`), which sit as datasources on `base` and supply the
`in (select ...)` subselects, were dropped — so the lifted membership rendered against a CTE
that was never emitted.

Fix: in the non-scalar branch, **relocate the existence feeder datasources** (sources whose
every output concept is an existence/membership arg) from `base` onto the wrapper SELECT and
record them in `existence_source_map`, so the subselect resolves to a real, emitted CTE. A
purely-scalar membership (no aggregate) never injects the wrapper and was unaffected.

## Two co-sourcing siblings in the SAME shape — also FIXED 2026-06-06

When the membership feeder *shares a materialized union scan* with the aggregate AND its filter
needs a join (e.g. `... and s.date.year = 1998`), two further bugs surfaced once B2 was fixed and
the feeders actually rendered. Both also reproduced in the **scalar** single-membership case, so
they were independent of B2:

- **Feeder-CTE join self-reference** → DuckDB `BinderException: Referenced table "<feeder>" not
  found`. The feeder's join ON rendered `"<feeder>"."s_date_id"` (the CTE's own name) instead of
  the FROM source: an optimization (`UnionDimPushdown` + `UpgradeConditionJoins`) merged the
  inner group that supplied the key up into the consumer, leaving the join key pair's `cte`
  pointing at the now-merged consumer itself. **Fix** (`trilogy/dialect/common.py`,
  `_render_left_concept`): when a join key's CTE resolves to the *enclosing* CTE, it's a local
  column — pin it to the consumer's FROM-base source (`base_alias`) rather than self-aliasing or
  letting the generic concept render pick the join's own right side (which produced a tautology).
- **Predicate-pushdown over-pruning**: the feeder's `channel = 'CATALOG'` pruned the shared union
  scan down to one channel, breaking a co-sourced unfiltered `STORE` aggregate. **Fix**
  (`trilogy/core/optimizations/predicate_pushdown.py`, `_prune_union_parent`): gate branch
  pruning on the same all-consumers guard `_push_into_union_branches` already uses — only prune
  when *every* consumer of the union carries the filter (via `inverse_map`).

The regression test `test_or_membership_with_projected_aggregate` now uses the full documented
repro (with `date.year`) and asserts no dangling reference, no self-referential join, all three
sales channels retained, and end-to-end execution.

## Symptom

```
ValueError: Invalid reference string found in query:
WITH cheerful as ( SELECT "s_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" ... ),
... this should never occur. Please report this issue.
```

The rendered SQL contains `FROM`/`JOIN`/`in (select ... from INVALID_REFERENCE_BUG ...)`
references to a CTE name that is absent from the `WITH` list — a dangling reference. DuckDB
would also reject it.

## Deterministic reproduction (checked-in enriched model)

Model: `tests/modeling/tpc_ds_duckdb` (`all_sales.preql`). No data — fails at SQL render.
Driver = the one in `bug_B1_recursion_rank_over_aggregate_ratio.md`
(`DuckDBDialect().compile_statement(process_query(env, parsed[-1]))`).

### Minimal repro (`repro.preql`)
```trilogy
import all_sales as s;

auto cat_qual <- s.billing_customer.id ? s.sales_channel = 'CATALOG' and s.date.year = 1998;
auto web_qual <- s.billing_customer.id ? s.sales_channel = 'WEB'     and s.date.year = 1998;
auto cust_total <- sum(s.ext_sales_price ? s.sales_channel = 'STORE') by s.billing_customer.id;

where s.billing_customer.id in cat_qual
   or s.billing_customer.id in web_qual
  and cust_total is not null
select
    round(cust_total / 50) as segment,
    count(s.billing_customer.id) as customer_count
limit 100;
```

## Minimization findings

The crash needs the FULL combination (peeling any piece → clean compile):
- **Two `in <derived filtered concept>` tests combined with `or`** in the top-level WHERE.
  A single membership (`B2single`) → OK.
- **An aggregate concept (`cust_total`) that is BOTH projected in SELECT and referenced in the
  WHERE** (`and cust_total is not null`). Drop the aggregate, or only reference it (not project
  it), → OK (`B2m2`, `B2m3`, `B2m5`).

So the trigger is: `where A in X or A in Y and <agg> is not null` where `<agg>` is also an
output column. (Note WHERE precedence parses this as `A in X OR (A in Y AND agg is not null)`.)

## Related trigger shapes (same invariant, seen in the eval — verify the fix covers them)

- **Rank/window over an aggregate-ratio in HAVING** (ingest q49):
  `having rank(...) over (order by sum(a)/sum(b)) <= 10` rendered
  `rank() over (order by INVALID_REFERENCE_BUG / INVALID_REFERENCE_BUG)`. (On the enriched
  single-fact model this particular form compiled OK — it needs the multi-fact/scoped-join
  ingest structure to surface; reproduce against an ingest-style model if extending coverage.)
- **Membership in HAVING** — the originally-tracked shape
  (`bug_invalid_reference_codegen_having_membership.md`, q02).

These share one root: a membership/aggregate **feeder subselect never gets a CTE** when it
appears OR-combined, or inside a window/HAVING. The fix should materialize the feeder CTE for
these shapes (or reject the shape cleanly up front), never emit a dangling reference.

## Provenance

TPC-DS agent eval: ingest q54 (Women/maternity catalog-or-web qualifying customers → store
spend segments) and ingest q49. The agents reached for OR-combined existence sets — a natural
phrasing — and burned their budget escaping the crash (q54 only recovered by switching to
`rowset` feeders). See also the consolidated inventory `token_burn_inventory_20260606.md`.
</content>
