# Bug: rowset over a union-rowset, consumed by a multi-reference downstream select → uncaught `RecursionError` building the rowset's measure (q5)

**Status:** FIXED 2026-06-27. Root cause in `query_processor._find_source_target`: the
order-by carry walked the rowset handle past the materializing `rolled` rowset down to the
inner union column (`local._combined_channel`), pulling it into the query grain — splitting it
off the rolled node (disconnect) and forcing the union to be enriched with aggregates of itself
(recursion). Fix: only recurse through a handle whose own rowset IS the union/multiselect;
otherwise carry the outer handle (which the rolled node materializes). Safety net added in
`gen_union_select_node` (merge_in_progress guard). Test
`tests/engine/test_duckdb_rowset.py::test_rowset_over_union_rowset_multiref_downstream_no_recursion`;
full q5 executes (100 rows). Original report retained below.

---

**(historical)** OPEN — deterministic repro (full query below) + bisection. Uncaught `RecursionError`.
**Surfaced by:** TPC-DS q5 (run `20260627-144947`) — the run's blowup: **5.39M tokens / 75 iterations
(EXHAUSTED the cap) / 13 errors**, of which **3× `Unexpected error: Recursion error building concept
rolled.<measure>`** (uncaught → no actionable message → agent loops to exhaustion). High-token query →
framework bug, as usual.
**Severity:** HIGH — uncaught `RecursionError`; exhausts the agent's iteration budget.

## Symptom

```
RecursionError: Recursion error building concept rolled.total_gross_sales
  with grain Grain<rolled.combined.channel, rolled.combined.entity_id>
```

Note the **doubly-nested grain** `rolled.combined.channel` — the outer rowset `rolled` is built over
the union rowset `combined`, and its grain components keep the `rolled.combined.*` path.

## Reproducing query (deterministic)

A `union(...)` rowset `combined`; a second rowset `rolled` aggregating it; a downstream select that
references `rolled`'s grain columns (`rolled.channel`, `rolled.entity_id`) in CASE labels + ORDER BY
alongside the measures:

```trilogy
import raw.all_sales as all_sales;
with combined as union(
  (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.channel_dim_id is not null
   select all_sales.channel, all_sales.channel_dim_id as entity_id,
          sum(all_sales.ext_sales_price) as gross_sales, 0.0 as total_returns,
          sum(all_sales.net_profit) as sales_profit, 0.0 as return_loss),
  (where all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date and all_sales.return_channel_dim_id is not null
   select all_sales.channel, all_sales.return_channel_dim_id as entity_id,
          0.0 as gross_sales, sum(all_sales.return_amount) as total_returns,
          0.0 as sales_profit, sum(all_sales.return_net_loss) as return_loss)
) -> (channel, entity_id, gross_sales, total_returns, sales_profit, return_loss);

with rolled as
select combined.channel, combined.entity_id,
       sum(combined.gross_sales) as total_gross_sales, sum(combined.total_returns) as total_returns_sum,
       sum(combined.sales_profit) as sp_sum, sum(combined.return_loss) as rl_sum
by rollup (combined.channel, combined.entity_id);

select
  case when rolled.channel is null then 'grand total' else lower(rolled.channel) || ' channel' end as channel_label,
  case when rolled.channel is null then null when rolled.entity_id is null then null
       else concat('x', rolled.entity_id::string) end as entity_identifier,
  coalesce(rolled.total_gross_sales, 0) as total_gross_sales,
  coalesce(rolled.total_returns_sum, 0) as total_returns,
  coalesce(rolled.sp_sum, 0) - coalesce(rolled.rl_sum, 0) as net_profit
order by rolled.channel asc nulls first, rolled.entity_id asc nulls first
limit 100;
-- RecursionError: Recursion error building concept rolled.total_gross_sales
```

## Bisection (what's load-bearing)

- **`by rollup (...)` is NOT required** — removing it still recurses. (Not a rollup bug.)
- **Simplifying the downstream to bare columns COMPILES** — `select rolled.channel,
  coalesce(rolled.total_gross_sales,0)` is fine.
- The trigger is the **multi-reference downstream over the nested rowset**: referencing the grain
  columns `rolled.channel` / `rolled.entity_id` (shorthand for `rolled.combined.*`) in CASE
  expressions **and** ORDER BY, together with the measures. No single added clause reproduces in a
  trimmed model; the full nested-rowset shape is needed.

## Likely fix area

The outer rowset's grain is the doubly-nested `rolled.combined.channel` / `rolled.combined.entity_id`
(rowset-over-rowset). Building a measure concept (`rolled.total_gross_sales`) AT that grain, when the
downstream also references those grain components via the shorthand resolver
([[project_rowset_output_shorthand_resolution]]) — which maps `rolled.entity_id` →
`rolled.combined.entity_id` — appears to re-enter concept construction for the same grain → infinite
recursion. Inspect concept-build for a measure whose grain components are themselves rowset outputs
of a nested rowset, especially the interaction with shorthand-resolved grain references. Add a
recursion guard / memoize so it raises a clean error (or resolves) instead of `RecursionError`.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-144947/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('q5.preql').read())   # RecursionError: building concept rolled.total_gross_sales
```
