# Bug B2: "Invalid reference string" codegen crash — OR-combined membership shape

**Status:** OPEN (found 2026-06-06). Same invariant as
`bug_invalid_reference_codegen_having_membership.md`, **new + simpler trigger**.
**Severity:** medium-high — a valid-looking query renders SQL that references a CTE never
emitted (the `cheerful`/`juicy`/random-word labels, or `INVALID_REFERENCE_BUG`). Top token
outlier on ingest q54 and q49.
**Area:** `trilogy/dialect/base.py` — the dangling-reference invariant at `base.py:1135` /
`base.py:2316` (`compile_statement`). The real gap is upstream: membership/aggregate feeder
CTEs are not materialized for this query shape.

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
