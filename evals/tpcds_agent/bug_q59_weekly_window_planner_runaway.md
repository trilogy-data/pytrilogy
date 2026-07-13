# Bug: q59 weekly window query causes planner/execution runaway

## Summary

The latest enriched q59 trajectory timed out after 900 seconds and consumed
1.07M tokens. One `trilogy run answer_1623435181.preql` call alone took about
485 seconds. Several small diagnostic queries over the same model also took
15-17 seconds.

This is a fatal framework performance failure, not ordinary agent difficulty.
The final query is a weekly store aggregate with seven filtered weekday sums,
seven `lead` windows, guarded ratios, and a post-window year filter.

## Artifacts

- Run: `evals/tpcds_agent/results/20260712-204357_enriched`
- Trajectory: `agent_log.q59.jsonl`
- Timeout trace: `crash.q59.txt`
- Candidate: `workspace/query59.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query59.preql`
- Earlier wrong-result handoff:
  `evals/tpcds_agent/handoff_q59_nullable_rowset_measure_identity_join.md`

The timeout trace records repeated runs followed by:

```text
[eval] agent timed out after 900s
```

## Candidate shape

```preql
with weekly as
where ss.date.year in (2001, 2002)
select
    ss.store.name,
    ss.store.id,
    ss.date.year,
    ss.date.week_seq,
    sum(ss.sales_price ? ss.date.day_of_week = 0) as sun,
    # ... six more weekday sums
;

select
    weekly.store_name,
    weekly.store_code,
    weekly.wk,
    case when weekly.sun is not null
         then weekly.sun / lead(weekly.sun, 52)
              over (partition by weekly.store_name, weekly.store_code
                    order by weekly.wk)
         else null end as sun_ratio,
    # ... six more lead/ratio expressions
having weekly.yr = 2001 and (... any ratio is not null ...);
```

The reference uses a normalized-week identity and a one-row lead over the two
aligned years, but both formulations should plan and execute promptly.

## Relationship to the earlier q59 bug

The earlier handoff showed planner-generated joins using nullable aggregate
measure outputs as identity keys, silently dropping rows. The current runaway
may be another symptom of that same reconstruction path: repeated window
expressions over a multi-measure aggregate rowset can cause repeated partial
materializations and expensive measure-based stitching.

Do not assume they are identical without profiling. The required investigation
is to determine whether time is spent in Trilogy planning/SQL generation or in
DuckDB executing an explosively large generated plan.

## Required investigation

1. Time parse, concept resolution, planning, SQL rendering, and DuckDB execution
   separately.
2. Capture generated SQL and `EXPLAIN` without executing the full result.
3. Count CTEs, joins, repeated weekday aggregate projections, and repeated
   window expressions.
4. Build a trigger matrix:
   - one weekday versus seven;
   - one ratio versus seven;
   - direct `lead` versus guarded `case`;
   - window over a rowset versus equivalent inline aggregates;
   - with and without the post-window year filter;
   - partition by surrogate store key versus name/business code;
   - offset 1 normalized-week form versus offset 52 chronological form.
5. Check whether generated joins again include nullable weekday measures as
   identity predicates.

## Expected behavior

This scale-factor-one query should plan and execute in seconds, not minutes. A
rowset with seven payload measures must be materialized once and reused by the
window calculations. Payload measures must not create Cartesian/repeated stitch
paths.

## Regression coverage

Add a small execution benchmark with two years, several entities and weeks, and
seven nullable filtered measures. Assert:

- bounded planning and execution time;
- one logical aggregate materialization;
- no planner-generated identity joins on measure payloads;
- correct nullable ratios;
- equivalent results for normalized-week and offset-period formulations.

