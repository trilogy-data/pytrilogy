# Compiler recursion: `auto X` referenced in HAVING blows the recursion limit

**Surfaced by:** TPC-DS agent eval, q57 on the enriched leg
(`evals/tpcds_agent/results/20260529-191521_enriched/agent_log.q57.jsonl`).
Run timestamp 2026-05-29.

## Symptom

`trilogy run` raises `RecursionError: maximum recursion depth exceeded` during
SQL generation for a query where:

1. An `auto X <- aggregate(...) by (...)` defines a computed column.
2. That same `X` is referenced in a `HAVING` clause.

The agent's near-final draft was structurally close to the gold reference but
produced this crash on every attempt to `trilogy run` it.

## Minimal repro (planned)

> TODO: extract the smallest .preql that triggers the recursion. Starting
> point — the enriched q57 draft below — but it imports the full enriched
> model. Trimming it to a minimal failing case requires:
>
> 1. A tiny fact table (3 columns, ~5 rows).
> 2. One `auto X <- avg(...) by (...)`.
> 3. A SELECT with `having X > <literal>`.
>
> Validate the trim still triggers the recursion before opening the issue
> against the compiler.

## Reproducer skeleton (agent's q57 draft, abbreviated)

```trilogy
import physical_sales as ss;

auto monthly_sales <- sum(ss.sales_price) by (ss.item.id, ss.date.year, ss.date.month_of_year);
auto avg_monthly_overall <- avg(monthly_sales) by ss.item.id;

select
    ss.item.id,
    ss.date.year,
    ss.date.month_of_year,
    monthly_sales,
having
    monthly_sales > 1.1 * avg_monthly_overall
;
```

The crash fires inside the compiler's resolution pass when the HAVING-referenced
`avg_monthly_overall` is itself an `auto` aggregate whose `by` clause derives
from another `auto`. Plain literal comparisons in HAVING (e.g. `monthly_sales > 1000`)
do not trigger the recursion.

## Workarounds (for agent / users hitting this today)

- Reference the bare aggregate in HAVING instead of the `auto`:
  `having monthly_sales > 1.1 * avg(monthly_sales) by ss.item.id`.
- Move the comparison into a WHERE on a derived rowset.

## Severity

Medium. Crashes the compiler on a syntactically reasonable pattern; blocks any
query that wants to compare a per-row aggregate against a higher-level
aggregated baseline using two `auto` chains. Surfaces in q57-style "monthly
sales vs annual avg" patterns common in TPC-DS-style analytics.

## Next steps

1. Build the minimal repro (TODO above).
2. Add a regression test under `tests/core/` once the fix lands.
3. Tag the offending pass / file in the trace once isolated.
