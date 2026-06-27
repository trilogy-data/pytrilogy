# Bug: `order by <measure passed through a nested rowset join>` → DuckDB BinderException "column must appear in GROUP BY" (q83)

**Status:** OPEN — deterministic repro + clean bisection. Surfaced in the full-99 rebaseline.
**Surfaced by:** TPC-DS q83 (run `20260627-181845`) — both a crash (×3) AND a 1.54M-token sink.
Uncaught `(_duckdb.BinderException)` → `Unexpected error`, no clean Trilogy message.
**Severity:** HIGH — invalid SQL DuckDB rejects.

## Symptom

```
Binder Error: column "combined_store_agg_store_qty" must appear in the GROUP BY clause
or must be part of an aggregate function.
```

## Trigger (bisected)

q83 builds three per-item aggregate rowsets (`store_agg`/`catalog_agg`/`web_agg`), joins them into a
`combined` rowset, then orders the final select by a **measure passed through that nested rowset**:

```trilogy
... store_agg/catalog_agg/web_agg each: select item_code, sum(return_quantity) as <ch>_qty, ... ;
with combined as
  left join store_agg.item_code = catalog_agg.item_code
  left join store_agg.item_code = web_agg.item_code
  select store_agg.item_code, store_agg.store_qty, catalog_agg.catalog_qty, web_agg.web_qty, ... ;

select
  combined.store_agg.item_code,
  case when ... then combined.store_agg.store_qty else null end as store_return_quantity,
  ...
order by combined.store_agg.item_code nulls first,
         combined.store_agg.store_qty nulls first      -- <-- THIS term triggers the crash
limit 100;
```

| variant | result |
|---|---|
| full | **ERR** (GROUP BY binder error) |
| drop `order by ... combined.store_agg.store_qty` (keep `item_code`) | **OK** |
| drop the whole `order by` | **OK** |

So ordering by the **dimension** (`item_code`) is fine; ordering by the **measure** that is passed
through the nested rowset (`combined.store_agg.store_qty`, a `sum`) is what breaks. The order-by carry
pulls `store_qty` into the query grain as a bare column, so it lands in the SELECT but not the GROUP BY
→ DuckDB rejects.

## Likely fix area

Same `_carry_order_by_concepts` / order-by-handle machinery as the FIXED q5 rowset-over-union
recursion ([[project_q5_rowset_over_union_rowset_recursion]] — `_find_source_target` in
`query_processor.py`): carrying an order-by concept that is a **measure passed through a nested
rowset** (`combined.store_agg.store_qty`) adds it to the query grain ungrouped. When the order-by
target is a non-grain (measure) output of a nested/joined rowset, it must be carried as the
materialized rowset output (already grouped), not pulled in as a raw grain column. At minimum, raise a
clean error instead of emitting invalid SQL.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-181845/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('q83.preql').read())[-1]   # succeeds
eng.execute_raw_sql(sql)   # BinderException: column "combined_store_agg_store_qty" must appear in GROUP BY
```
