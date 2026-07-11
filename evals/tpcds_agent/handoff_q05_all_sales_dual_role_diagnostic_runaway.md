# Handoff: q05 `all_sales` dual sale/return role query runaway

## Severity

Fatal performance bug for agent workflows. Small diagnostic queries over
`all_sales` take tens of seconds or run for minutes at full CPU, blocking the
agent and therefore the single-concurrency enriched eval.

This is separate from q04's nested-rowset planner hang. q05 triggers while
probing a single imported model.

## Run

```text
evals/tpcds_agent/results/20260711-185953_enriched/agent_log.q05.jsonl
```

## Slow query

This query returned 10 rows, with only 16 rows in the full filtered result, but
took 36.9 seconds:

```preql
import raw.all_sales as all_sales;

select
    all_sales.channel,
    all_sales.channel_dim_text_id,
    all_sales.return_channel_dim_text_id,
    all_sales.is_returned
where
    all_sales.channel = 'WEB'
    and all_sales.is_returned = true
limit 10;
```

Recorded execution duration:

```text
36939.54 ms
```

## Runaway query

The next diagnostic had not returned after more than four minutes:

```preql
import raw.all_sales as all_sales;

select
    all_sales.channel,
    all_sales.channel_dim_text_id,
    all_sales.return_channel_dim_text_id,
    all_sales.date.date,
    all_sales.return_date.date
where
    all_sales.channel = 'STORE'
    and all_sales.is_returned = true
    and all_sales.return_date.date between
        '2000-08-23'::date and '2000-09-06'::date
limit 10;
```

The deepest `trilogy run` Python child was continuously consuming CPU. This was
not model latency, trajectory-viewer delay, or scoring.

## Trigger

`all_sales` is a multi-channel partial/complete model combining store, catalog,
and web sales and returns. The pathological projection requests both sides of
its role structure:

- sale entity: `channel_dim_text_id`;
- return entity: `return_channel_dim_text_id`;
- sale date: `date.date`; and
- return date: `return_date.date`.

The selective channel/date predicates and `LIMIT` are not pushed early enough
to prevent expensive union/stitch expansion. The WEB query proves the problem
is present even without projecting both dates; adding both dates makes it much
worse.

## Expected behavior

Both diagnostics should plan and execute in bounded time. A channel predicate
must prune unrelated partial datasource arms before expensive role stitching.
Return-only predicates should be pushed into the return arms, and `LIMIT 10`
must not require materializing or repeatedly planning the full cross-channel
sale/return domain.

## Affected-question audit

Among current TPC-DS reference queries importing `all_sales`, q05 is the only
one that simultaneously references both sale-side and return-side entity/date
roles. It is therefore the direct benchmark reproducer.

Related but distinct cases:

- q04 in the same smoke run hangs on nested multiway rowset joins; see
  `handoff_q04_multi_rowset_union_join_planner_hang.md`.
- q80 imports `all_sales` but uses only sale-side entity/date roles, so it does
  not reproduce this exact dual-role expansion.
- other cross-channel references import `all_sales` but do not combine both
  role pairs in one projection.

Historical q05 analysis already noted high agent token/tool usage around these
fields:

```text
analysis_q05_q97_token_sinks.md
handoff_q05_q80_rollup_label_via_join.md
```

Those should be re-evaluated after the performance fix.

## Regression matrix

Use the real enriched `all_sales.preql` and a small synthetic partial/complete
fixture. Time SQL generation and execution separately for:

1. channel only;
2. channel + sale entity;
3. channel + return entity;
4. channel + both entity roles;
5. add sale date;
6. add return date;
7. add both dates;
8. repeat each with a channel predicate;
9. repeat with a selective return-date predicate;
10. repeat with `is_returned = true` and `LIMIT 10`.

Assert both a bounded planning time and SQL shape: unrelated channel arms must
be absent after a constant channel filter, and return-date predicates must be
inside the relevant return datasource branches rather than applied after a
large stitched union.

Likely areas:

```text
trilogy/core/processing/node_generators/
trilogy/core/processing/join_resolution.py
trilogy/core/processing/nodes/merge_node.py
trilogy/core/optimizations/
```
