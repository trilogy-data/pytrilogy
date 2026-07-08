# q05 perf sink — return-only projection over `all_sales` joins the full SALES fact (375s/arm)

**Status:** OPEN perf bug (framework). Silent — no error; the agent's query is *correct*, the
engine just emits a pathologically slow plan and the agent times out (900s).

**Run:** `evals/tpcds_agent/results/20260707-151529` — q05 burned **1.53M tokens** and **timed
out** (`crash.q05.txt`: agent looped on `run query05.preql`). Prior runs also had q05 as the top
sink (2.29M). This is the single largest remaining token sink.

## Symptom / timing (sf=1, scored engine over the run workspace)

The agent's final `query05.preql` is a 6-arm `union()` (a sales arm + a returns arm per channel)
rolled up by channel/outlet. Breakdown:

| construct | time | rows |
|---|---|---|
| `generate_sql` (planning) whole query | 1.1s | — |
| single STORE **sales** arm (incl. `channel_dim_text_id` dim join) | **0.2s** | 6 |
| single STORE **returns** arm | **375s** | 6 |
| 6-arm union (no rollup) | 218s | 851 |
| full query (union + rollup), `EXPLAIN ANALYZE` | 369s | — |

So planning is fine, sales arms are fine, the dim-join is fine — **the returns arms are the
blowup** (~375s each × 3 channels), which is what times the agent out.

## Root cause (file:line target for the fixer)

A returns arm selects ONLY return-side columns:
```
where all_sales.channel='STORE'
  and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
  and all_sales.return_channel_dim_id is not null
select all_sales.return_channel_dim_text_id as e,
       sum(all_sales.return_amount) as r,
       sum(all_sales.return_net_loss) as l,
```
No sales measure is referenced. Yet the generated SQL still joins the **entire `store_sales`
fact** (2.88M rows) to the returns:
```sql
FROM "store_sales" AS "all_sales_store_sales_unified"
  RIGHT OUTER JOIN "wakeful"    -- store_returns, the only table we actually need
    ON 'STORE'='STORE' AND SS_ITEM_SK=wakeful.item AND SS_TICKET_NUMBER=wakeful.order
  INNER JOIN "quizzical"        -- return_date filter CTE (this part IS pushed down)
    ON wakeful.return_date_id = quizzical.return_date_id
  LEFT OUTER JOIN "store" AS "all_sales_store_dim_return_unified"
    ON wakeful.return_channel_dim_id = store.S_STORE_SK ...
GROUP BY 1
```
(full generated SQL: `scratchpad/q5_returns_arm.sql`; here inlined in the trigger below.)

The `store_sales RIGHT OUTER JOIN store_returns` on `(item, ticket)` is a 2.88M-row hash-join
probe that produces nothing the query needs — every projected column comes from `store_returns`
(`wakeful`) or the return dim (`store`). The join exists because `all_sales` declares grain
`(item.id, order_id, channel)` and the **sales** partial datasource is treated as the anchor/
row-source for that grain, so the planner anchors on `store_sales` and outer-joins the returns
onto it even when only return-side concepts are selected.

**Fix direction:** when a query over a multi-datasource unified model (`all_sales`) references
only concepts sourced from ONE non-anchor datasource (here the returns datasource) plus dims
reachable from it, the planner should source from that datasource directly and NOT join the
anchor sales fact. I.e. anchor/join-source selection should follow the concepts actually
required, and an all-optional anchor join with no referenced anchor columns should be elided.
Likely lives in the datasource/anchor-selection + join-elimination logic (the node that picks the
FROM seed for a grain and decides which partial datasources to join). Start from how the
`(item, order, channel)` grain resolves its row-source when only `return_*` outputs + a
`return_*` filter are present.

## Minimal repro
```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path; from common import scoring
ws=Path('evals/tpcds_agent/results/20260707-151529/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
body='''import raw.all_sales as all_sales;
where all_sales.channel='STORE'
  and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
  and all_sales.return_channel_dim_id is not null
select all_sales.return_channel_dim_text_id as e, sum(all_sales.return_amount) as r, sum(all_sales.return_net_loss) as l,;'''
sql=eng.generate_sql(body)[-1]        # fast; inspect: FROM store_sales RIGHT OUTER JOIN store_returns
list(eng.execute_raw_sql(sql).fetchall())   # ~375s for 6 rows  <-- the bug
```
Trigger matrix (toggle one ingredient):
- select a SALES measure instead of `return_amount`/`return_net_loss` → 0.2s (anchors on sales, correct).
- select `return_amount` but from the per-channel `raw.store_returns` model instead of `all_sales`
  → fast (no spurious sales join). So the pathology is specific to the return-only projection over
  the unified `all_sales`.

## Guidance angle (secondary)
`all_sales.preql`'s header comment steers agents to it for "multi-channel analysis," which is why
the agent used it for q05. Even once the join-elision lands, a returns-heavy multi-channel query
is cheaper off the per-channel `store_returns`/`catalog_returns`/`web_returns` models. Not a fix,
just context for why the agent landed here.

## Full agent query (`results/20260707-151529/workspace/query05.preql`)
```
import raw.all_sales as all_sales;

with combined as union(
    (where all_sales.channel = 'STORE'
       and all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       and all_sales.channel_dim_id is not null
     select 'store channel' as channel_label, all_sales.channel_dim_id as entity_id,
        all_sales.channel_dim_text_id as entity_text_id,
        sum(all_sales.ext_sales_price) as total_ext_sales, 0 as total_returns,
        sum(all_sales.net_profit) as total_net_profit),
    (where all_sales.channel = 'STORE'
       and all_sales.return_date.date between '2000-08-23'::date and '2000-09-06'::date
       and all_sales.return_channel_dim_id is not null
     select 'store channel' as channel_label, all_sales.return_channel_dim_id as entity_id,
        all_sales.return_channel_dim_text_id as entity_text_id,
        0 as total_ext_sales, sum(all_sales.return_amount) as total_returns,
        -sum(all_sales.return_net_loss) as total_net_profit),
    -- ...catalog sales arm, catalog returns arm, web sales arm, web returns arm (same shape)...
) -> (channel_label, entity_id, entity_text_id, total_ext_sales, total_returns, total_net_profit);

select
    case when grouping(combined.channel_label)=1 then null else combined.channel_label end as channel_output,
    case when grouping(combined.entity_id)=1 then null else concat(
        case combined.channel_label when 'store channel' then 'store'
            when 'catalog channel' then 'catalog_page' when 'web channel' then 'web_site' end,
        combined.entity_text_id) end as entity_output,
    coalesce(sum(combined.total_ext_sales), 0) as total_ext_sales,
    coalesce(sum(combined.total_returns), 0) as total_returns,
    coalesce(sum(combined.total_net_profit), 0) as net_profit,
    --grouping(combined.channel_label) + grouping(combined.entity_id) as _level
by rollup (combined.channel_label, combined.entity_id)
order by _level asc, combined.channel_label asc nulls first, combined.entity_id asc nulls first
limit 100;
```
(this is TPC-DS q5 — store/catalog/web sales+returns rolled up by channel/outlet; the reference
`tests/modeling/tpc_ds_duckdb/query05.sql` uses per-channel tables and runs fast.)

## Deliverable for the fixer
1. Reproduce the 375s return-only arm; `EXPLAIN ANALYZE` to confirm the `store_sales` scan/probe
   is the hot operator.
2. Elide the anchor sales-fact join when no anchor (sales) concept is referenced; source the grain
   from the returns datasource. Guard with a test that the return-only arm plans WITHOUT a
   `store_sales` scan and the 6-arm q05 union executes in <a few seconds.
3. Confirm q05 scores pass on the current engine (also unblocks the float32/rollup notes in
   `project_float32_union_placeholder_drift_no_double_type`).
