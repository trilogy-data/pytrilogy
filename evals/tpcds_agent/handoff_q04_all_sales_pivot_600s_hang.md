# Handoff: q04-shaped all_sales pivot ran past the 600s subprocess timeout

**Status: unreproduced performance cliff. From
`results/20260810-211903_enriched_noise/agent_log.q04.jsonl`. This single
query burned ~1.07M agent tokens, mostly waiting out and recovering from the
timeout.**

## What happened

A TPC-DS q04-style year-over-year query over `all_sales` (the union model
spanning store/catalog/web) hung until the agent tool's 600s subprocess kill.
The agent then rewrote it (moving customer attributes into the first rowset
and aggregating with `max(...)`) and the revised query completed and PASSED.
DuckDB at SF=1; nothing in this warehouse should take 10 minutes.

## The hanging query (verbatim)

```trilogy
import raw.all_sales as sales;

auto line_value <- sales.ext_list_price - sales.ext_wholesale_cost - sales.ext_discount_amount + sales.ext_sales_price;

with annual as
where sales.ext_sales_price is not null
select
    sales.billing_customer.id as cid,
    sales.channel as channel,
    sales.sale_date.year as yr,
    sum(line_value) as annual_value
;

with cust as
select
    annual.cid as cid,
    sum(annual.annual_value ? annual.channel = 'STORE' and annual.yr = 2001) as store_2001,
    sum(annual.annual_value ? annual.channel = 'STORE' and annual.yr = 2002) as store_2002,
    sum(annual.annual_value ? annual.channel = 'CATALOG' and annual.yr = 2001) as cat_2001,
    sum(annual.annual_value ? annual.channel = 'CATALOG' and annual.yr = 2002) as cat_2002,
    sum(annual.annual_value ? annual.channel = 'WEB' and annual.yr = 2001) as web_2001,
    sum(annual.annual_value ? annual.channel = 'WEB' and annual.yr = 2002) as web_2002
;

where sales.channel = 'STORE'
    and sales.sale_date.year = 2002
    and sales.ext_sales_price is not null
select
    cust.cid,
    sales.billing_customer.first_name as first_name,
    sales.billing_customer.last_name as last_name,
    sales.billing_customer.preferred_cust_flag as preferred_cust_flag
subset join cust.cid = sales.billing_customer.id
having
    cust.store_2001 > 0
    and cust.cat_2001 > 0
    and cust.web_2001 > 0
    and cust.store_2002 is not null
    and cust.cat_2002 is not null
    and cust.web_2002 is not null
    and cust.cat_2002 / cust.cat_2001 > cust.store_2002 / cust.store_2001
    and cust.cat_2002 / cust.cat_2001 > cust.web_2002 / cust.web_2001
order by cust.cid asc nulls first, first_name asc nulls first, last_name asc nulls first, preferred_cust_flag asc nulls first
limit 100;
```

## Repro guidance

- Model: `tests/modeling/tpc_ds_duckdb/all_sales.preql`. The eval ran against
  the physically renamed variant DB, but the rename should not affect plan
  shape.
- First determine WHERE the 600s went: compile (planning) vs execution.
  Generate SQL without executing (render path) with a wall clock on it. The
  s66 memory (`project_v4_unresolvable_path_ci_stall_s66`) fixed a 293s
  planning stall with proof-based failure paths — if this is planning, check
  whether the split-pool certificate / STATE_LIMIT machinery is engaged for
  this shape.
- If execution: inspect the generated SQL for a fan-out join — the final
  select joins the fact at line grain to `cust` at customer grain while
  projecting customer attributes from the fact side; a missed dedup before
  that join at SF=1 (~4.4M union rows) could plausibly run 10+ minutes.
  Compare with the agent's PASSING rewrite in the same log (attributes
  aggregated inside the rowsets) to see what plan shape difference the small
  authoring change caused.
- The suspiciously similar known cliff: `project_q64_marital_push_perf_cliff`
  (extra row-grain rowset is load-bearing; 3GB limit). Check the verdict index
  before deep-diving.

## Harness note (separate, cheap)

A 600s silent hang costs the agent its whole iteration budget's worth of
patience. Consider a statement-level timeout in agent mode (e.g. 120s) that
returns a structured `{"event":"error","message":"statement timed out after
120s; the generated SQL may have a fan-out join — try aggregating attributes
inside a rowset"}` so the agent can adapt after 2 minutes instead of 10.
