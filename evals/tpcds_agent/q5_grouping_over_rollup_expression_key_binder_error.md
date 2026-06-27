# Bug: `grouping(<expr>)` over a `by rollup` whose key is that same expression → DuckDB BinderException "GROUPING child must be a grouping column" (q5)

**Status:** OPEN — deterministic repro + the exact bad SQL below.
**Surfaced by:** TPC-DS q5 (run `20260627-155703`). Surfaces as
`Unexpected error: (_duckdb.BinderException) Binder Error: GROUPING child "COALESCE('CATALOG',
sparkling.returns_data__rch)" must be a grouping column` (uncaught — `ProgrammingError` /
`BinderException` from DuckDB, not a clean Trilogy error).
**Severity:** HIGH — generates invalid SQL DuckDB rejects; the agent gets a raw binder error and loops.

## Root cause (from the generated SQL)

The rollup groups by a DERIVED EXPRESSION key (a `coalesce(...)` from a full-join), which the planner
materializes as a `_virt_func_coalesce_N` column and groups by **position**:

```sql
abhorrent as (
  SELECT
    sparkling._virt_func_coalesce_8286... as _virt_func_coalesce_8286...,   -- = coalesce(reid, reid)
    sparkling._virt_func_coalesce_9650... as _virt_func_coalesce_9650...,   -- = coalesce('CATALOG', returns_data__rch)
    grouping(coalesce('CATALOG', "sparkling"."returns_data__rch")) as "_g1",          -- !! re-INLINED expr
    grouping(coalesce('CATALOG', "sparkling"."returns_data__rch")) as "_virt_agg_grouping_1635...",
    grouping(coalesce("sparkling"."returns_data__reid","sparkling"."returns_data__reid")) as "_virt_agg_grouping_4828..."
  FROM sparkling
  GROUP BY ROLLUP (2, 1))           -- groups by the COLUMNS at positions 1,2
```

`GROUP BY ROLLUP (2, 1)` groups by the materialized columns `_virt_func_coalesce_9650...` /
`_virt_func_coalesce_8286...`. But each `grouping(...)` call **re-inlines the coalesce expression**
(`coalesce('CATALOG', returns_data__rch)`) instead of referencing the grouping-key column. DuckDB
requires `grouping()`'s argument to be **exactly** one of the GROUP BY keys; the inlined expression is
not recognized as the same key → "GROUPING child must be a grouping column".

**Fix:** when a rollup grouping key is a derived/expression key materialized as a `_virt_func_*`
column (and grouped by position), the `grouping(key)` call must reference that **same column**
(`grouping("_virt_func_coalesce_9650...")` or `grouping(2)`), not re-emit the inlined expression.
(The bare `combined.channel` concept-key form works — see contrast below — because the grouping arg
and the GROUP BY key are the same column reference. Only the EXPRESSION-key rollup mismatches.)

## Reproducing query (deterministic)

Two rowsets full-joined, rolled up by `coalesce(left, right)` keys, with `grouping(coalesce(...))`:

```trilogy
import raw.all_sales as s;
with sales_data as
  where s.date.date between '2000-08-23'::date and '2000-09-06'::date and s.channel_dim_text_id is not null
  select s.channel as _sch, s.channel_dim_text_id as _eid, sum(coalesce(s.net_profit,0)) as _gross_sales;
with returns_data as
  where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date and s.return_channel_dim_text_id is not null
  select s.channel as _rch, s.return_channel_dim_text_id as _reid,
         sum(coalesce(s.return_amount,0)) as _total_returns, sum(coalesce(s.return_net_loss,0)) as _return_loss;

select
  case when grouping(coalesce(sales_data._sch, returns_data._rch)) = 1 then 'grand total'
       else coalesce(sales_data._sch, returns_data._rch) end as channel_name,
  case when grouping(coalesce(sales_data._eid, returns_data._reid)) = 1 then null
       else coalesce(sales_data._eid, returns_data._reid) end as entity_id,
  coalesce(sales_data._gross_sales, 0) as total_gross_sales
full join sales_data._sch = returns_data._rch
full join sales_data._eid = returns_data._reid
by rollup (coalesce(sales_data._sch, returns_data._rch), coalesce(sales_data._eid, returns_data._reid))
limit 100;
-- BinderException: GROUPING child "COALESCE(...)" must be a grouping column
```

## Contrast (what works)

The same intent via a `union(...)` rowset with **plain concept** rollup keys
(`by rollup` over `combined.channel`, `combined.entity_id` — not coalesce expressions) compiles and
runs. So the trigger is specifically a **rollup whose grouping key is a derived expression**
(`coalesce(...)`) with `grouping()` over that expression. Likely a gap in the new SELECT-level rollup
(`by rollup (expr, ...)`) grouping() rendering — it inlines the expr rather than pointing grouping()
at the materialized key column.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-155703/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('q5.preql').read())[-1]
eng.execute_raw_sql(sql)   # BinderException: GROUPING child must be a grouping column
```
