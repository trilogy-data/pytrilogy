# Bug: generated SQL contains empty `GROUP BY ROLLUP ()`, rejected by DuckDB

Status: OPEN. Found in run `20260817-013108_ingest_deepseek_deepseek-v4-flash`,
q05 (`probe_chan.preql`). Not yet minimized; the flat-model constant+aggregate
shape does NOT reproduce it, so the trigger involves the joined-CTE shape below.

## Symptom

The agent wrote a plain constant + aggregates select (no `by rollup` anywhere),
and the engine emitted SQL DuckDB cannot parse:

```
(_duckdb.ParserException) Parser Error: syntax error at or near ")"
LINE 21:     ROLLUP ()
```

Generated SQL (abridged, from the run log):

```sql
WITH cheerful as (
  SELECT ss_ext_sales_price, ss_net_profit
  FROM store_sales
  INNER JOIN store ON ...
  INNER JOIN date_dim ON ...
  WHERE d_date BETWEEN date '2000-08-23' AND date '2000-09-06'
    AND s_store_id is not null
)
SELECT
  $1 as "part",
  coalesce(sum(ss_ext_sales_price),0) as "ext_sales",
  coalesce(sum(ss_net_profit),0) as "net_profit"
FROM cheerful
GROUP BY ROLLUP ()
```

`GROUP BY ROLLUP ()` with an empty column list is a DuckDB syntax error. The
correct grand-total grouping is `GROUP BY ()` (or omitting GROUP BY entirely
for a pure-aggregate select).

## Authored query (exact body in `agent_log.q05.jsonl`)

```
import root.store_sales as ss;
import root.store_returns as sr;   -- imported but unused in this probe
select
    'store sales' as part,
    coalesce(sum(ss.ext_sales_price), 0) as ext_sales,
    coalesce(sum(ss.net_profit), 0) as net_profit
where ss.date_dim.date between '2000-08-23'::date and '2000-09-06'::date
    and ss.store.store_id is not null;
```

(The exact authored text is truncated in the log's stdin preview; the WHERE
reconstruction above matches the generated SQL. Re-run against the run
workspace `results/20260817-013108_ingest_deepseek_deepseek-v4-flash/workspace`
to reproduce, AFTER the eval run finishes.)

## What does NOT reproduce (tried on a flat synthetic model)

- `select 'store' as part, sum(amt) as total;` renders with no GROUP BY - OK.
- Same with a leading `where` on a model property and two coalesce(sum())
  outputs - still no GROUP BY - OK.

So the ROLLUP grouping mode is selected somewhere in the multi-hop
(fact + two joined dims, filtered CTE) path, not by the constant itself.

## Root-cause leads

- Render site: `trilogy/dialect/base.py:2757` -
  `return [f"ROLLUP ({render_concepts(by)})"]` has no guard for an empty `by`
  list. Whatever the upstream cause, this line should never emit `ROLLUP ()`;
  an empty ROLLUP is semantically `GROUP BY ()`.
- Mode selection: `_get_aggregate_grouping` (same file, near
  `_render_grouping_mode` at `base.py:2736`) decided
  `AggregateGroupingMode.ROLLUP` with an empty by-list for this CTE. Why did a
  query with no authored rollup get ROLLUP mode at all?
- Note `_constant_output_group_by_fallback` (`base.py:2768`) exists exactly for
  the constant-output-over-real-source case; this CTE apparently took the
  ROLLUP branch before reaching it.

## Suggested fix shape

Two layers:
1. Hard guard at the render site: empty `by` renders `GROUP BY ()` (never
   `ROLLUP ()`), for CUBE too.
2. Find and fix the upstream mode selection that produced rollup-with-empty-by
   for an unauthored rollup; the guard alone would mask a planner defect.
