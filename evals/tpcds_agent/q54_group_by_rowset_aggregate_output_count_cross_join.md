# Bug: grouping by a rowset's aggregate output + counting its key → ungrouped count cross-joined to every group (silent wrong results) (q54)

**Status:** FIXED 2026-06-27. Root cause + fix at the bottom. **SILENT wrong results** (no error), the
most dangerous kind.
**Surfaced by:** TPC-DS q54 (run `20260627-155703`) — the agent burned 1.42M tokens / 39 iterations
because every segment came back with the SAME `customer_count` (the grand total), which it (correctly)
suspected was wrong but couldn't get past.
**Severity:** HIGH — produces plausible-looking but wrong numbers; no exception.

## Symptom

Two-level aggregation: a rowset holds a per-entity aggregate (per-customer revenue); the outer query
buckets that aggregate and counts entities per bucket. **Every bucket reports the TOTAL entity count**
instead of the per-bucket count.

```trilogy
import raw.store_sales as store;
with cust_totals as
  where store.date.year = 2001
  select store.customer.id as cust_id, sum(store.ext_sales_price) as total_price;   -- per-customer aggregate

select round(cust_totals.total_price / 50) as seg, count(cust_totals.cust_id) as cnt
where cust_totals.total_price is not null
order by seg;
-- EVERY row: cnt = 37923  (== total # customers), for all ~700 distinct seg values.  WRONG.
-- (correct: customers whose round(total/50) == seg — mostly 1 each here.)
```

## Mechanism (from the generated SQL)

```sql
SELECT "questionable"."seg" as seg, "cooperative"."cnt" as cnt
FROM   "cooperative"                       -- cooperative: cnt = count(cust_id), computed UNGROUPED → 37923 (grand total)
       FULL JOIN "questionable" ON 1=1      -- questionable: the distinct seg values; CROSS-JOINED to the single count
GROUP BY 1, 2
```

The planner put the `count(cust_id)` aggregate and the `seg` group key in **separate CTEs** and joined
them with **`ON 1=1` (a cross join)**. So `cnt` is a grand-total scalar broadcast to every `seg`; the
count is never grouped by `seg`. The grouping relationship between the bucket and the count is lost.

## Trigger / scope

- Grouping by a rowset output that is itself an **aggregate** (`total_price = sum(...) by customer.id`)
  and counting another rowset key (`count(cust_id)`) → the count is the grand total for every group.
- **Both** a plain group key (`group by cust_totals.total_price`) **and** a derived one
  (`round(total_price/50)`, `round(total_price/100000)`) exhibit it — so it is NOT about the derived
  expression; it is about grouping/counting across a rowset whose grouping column is an aggregate
  measure. The planner computes the count at the rowset grain (all entities) and cross-joins.

## Likely fix area

The outer aggregate (`count(cust_id)`) must be grouped BY the bucket/segment key, not computed at the
rowset's full grain and cross-joined. Inspect how a downstream aggregate over a rowset is grained when
the GROUP BY key is derived from (or is) the rowset's aggregate measure — it is landing in a separate
node from the group key and being assembled with `FULL JOIN ON 1=1` instead of grouping in one node.
This is the canonical "histogram / segment-count over a per-entity total" shape (TPC-DS q54), so it
likely affects any two-level aggregate-then-bucket-then-count query.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-155703/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
rows = eng.execute_text(
  'import raw.store_sales as store;'
  'with cust_totals as where store.date.year=2001 select store.customer.id as cust_id, sum(store.ext_sales_price) as total_price;'
  'select round(cust_totals.total_price/50) as seg, count(cust_totals.cust_id) as cnt where cust_totals.total_price is not null order by seg limit 10;'
)[-1].fetchall()
# every cnt == total customer count (37923), not per-segment
```

## Root cause (FIXED 2026-06-27)

The grain-less rowset aggregate output `cust_totals.total_price` (= `sum(...)` with no
explicit `by`) wrongly carried `Granularity.SINGLE_ROW`. In `rowset_to_concepts_v2`
(`trilogy/parsing/v2/rowset_semantics.py`) such an output is re-grained to the rowset's
concrete grain (`{cust_id}`) at lines 205-207 — but its **granularity was never updated**;
it kept the SINGLE_ROW value copied from the inner abstract-grain aggregate at line 126.

A SINGLE_ROW concept reads as a scalar. So when `seg = round(total_price/50)` was tested by
`concept_is_relevant` (parsing/common.py), `total_price` was deemed not-relevant (single-row
short-circuit), `seg` followed, and the **select grain came out abstract**. With no group key,
the planner computed `count(cust_id)` ungrouped (grand total) in its own CTE and assembled it
with `FULL JOIN ON 1=1` against the distinct `seg` values.

**Fix:** when re-graining a grain-less aggregate output to the rowset's concrete grain, also set
`granularity = MULTI_ROW` (one row per grain key). One-line addition in `rowset_semantics.py`.
Regression test: `tests/engine/test_duckdb_rowset.py::test_rowset_aggregate_output_bucketed_and_counted_groups_per_bucket`
(asserts no `on 1=1` and per-bucket counts).
