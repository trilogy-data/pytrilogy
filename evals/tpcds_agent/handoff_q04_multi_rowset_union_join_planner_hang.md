# Handoff: q04 multi-rowset union-join planner hang

## Severity

Fatal framework bug. `trilogy run` remains in query planning indefinitely,
blocking the single-concurrency enriched eval until the outer agent timeout.

This is not DuckDB execution or agent-provider latency: the active child process
is the local Trilogy CLI, and the agent log ends at its `trilogy run` tool call.

## Run and evidence

Run:

```text
evals/tpcds_agent/results/20260711-185953_enriched
```

Trajectory:

```text
agent_log.q04.jsonl
```

The final event is a tool call at `2026-07-11T19:05:01Z`:

```text
trilogy run answer_3863442186.preql
```

The corresponding `python -m trilogy.scripts.trilogy run` child was still alive
more than 470 seconds later without producing a tool result. Earlier candidates
for the same question returned promptly, including one guarded failure:

```text
query could not be planned; this is a bug.
```

The rewrite below converts that prompt failure into a planner hang.

## Hanging query

```preql
import raw.store_sales as store;
import raw.catalog_sales as catalog;

with s_ann as
where store.date.year in (2001, 2002)
select
    store.customer.id as cid,
    store.customer.first_name as fn,
    store.customer.last_name as ln,
    store.customer.preferred_cust_flag as pf,
    store.date.year as yr,
    sum(store.profit) as val;

with c_ann as
where catalog.sold_date.year in (2001, 2002)
select
    catalog.billing_customer.id as cid,
    catalog.sold_date.year as yr,
    sum(catalog.profit) as val;

with s01 as
where s_ann.yr = 2001 select s_ann.cid, s_ann.val as sv01;
with s02 as
where s_ann.yr = 2002
select s_ann.cid, s_ann.fn, s_ann.ln, s_ann.pf, s_ann.val as sv02;
with c01 as
where c_ann.yr = 2001 select c_ann.cid, c_ann.val as cv01;
with c02 as
where c_ann.yr = 2002 select c_ann.cid, c_ann.val as cv02;

with sc as
select
    s02.cid,
    s02.fn,
    s02.ln,
    s02.pf,
    s01.sv01,
    s02.sv02,
    c01.cv01,
    c02.cv02
union join s02.cid = s01.cid = c01.cid = c02.cid
having
    s01.sv01 > 0 and c01.cv01 > 0 and s02.sv02 > 0 and c02.cv02 > 0
    and (c02.cv02 / c01.cv01) > (s02.sv02 / s01.sv01);

import raw.web_sales as web;

with w_ann as
where web.date.year in (2001, 2002)
select
    web.billing_customer.id as cid,
    web.date.year as yr,
    sum(web.profit) as val;

with w01 as
where w_ann.yr = 2001 select w_ann.cid, w_ann.val as wv01;
with w02 as
where w_ann.yr = 2002 select w_ann.cid, w_ann.val as wv02;

select
    sc.cid as customer_id,
    sc.fn as first_name,
    sc.ln as last_name,
    sc.pf as preferred_cust_flag
union join sc.cid = w01.cid = w02.cid
having
    sc.sv01 > 0 and sc.cv01 > 0 and w01.wv01 > 0
    and sc.sv02 > 0 and sc.cv02 > 0 and w02.wv02 > 0
    and (sc.cv02 / sc.cv01) > (sc.sv02 / sc.sv01)
    and (sc.cv02 / sc.cv01) > (w02.wv02 / w01.wv01)
order by
    customer_id asc nulls first,
    first_name asc nulls first,
    last_name asc nulls first,
    preferred_cust_flag asc nulls first
limit 100;
```

## Trigger shape

The query has three features worth reducing independently:

1. filtered yearly projections derived from an aggregated rowset;
2. a four-way `union join` feeding a named rowset with HAVING expressions; and
3. that joined rowset participating in a second three-way `union join` whose
   HAVING references measures projected through the first join.

The likely loop is in recursive source/discovery planning for `sc`: downstream
requirements pull `sc.sv01`, `sc.sv02`, `sc.cv01`, and `sc.cv02` back through
the first multiway join while its own HAVING requires the same branches.

## Expected behavior

Planning must terminate. If the rowset graph cannot be resolved, return the
existing guarded `query could not be planned` error promptly. A user-authored
query must never leave the planner in an unbounded search/fixpoint loop.

## Investigation plan

Build a small in-memory fixture with three facts keyed by customer and year,
then reduce in this order:

1. retain only `s_ann`, `s01`, and `s02`;
2. add `c01`/`c02` and the first multiway join without HAVING;
3. add the first HAVING ratios;
4. project that rowset in a final select;
5. add `w01`/`w02` and the second multiway join;
6. add the second HAVING ratios.

Run SQL generation in a subprocess with a short timeout for every step. The
first timing-out reduction becomes the regression fixture. Also assert that an
unsupported/cyclic source graph raises a deterministic planning exception.

Likely areas:

```text
trilogy/core/processing/node_generators/
trilogy/core/processing/join_resolution.py
trilogy/core/processing/nodes/merge_node.py
trilogy/core/processing/utility.py
```

Compare with the q72 composite-join runaway handoff, but keep this regression
separate: q72 generated a weakened join and ran away in SQL execution, whereas
this q04 case never returns from Trilogy planning.
