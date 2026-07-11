# NOT-A-BUG: q02 "partitioned `lead` loses identity through pivot" — actual cause is union-arm set-semantics dedup

## Verdict (2026-07-11)

**No framework bug.** The suspected mechanism — the planner detaching the
window output from its `(ws, dow)` identity during the downstream pivot — is
disproven. The window/pivot pipeline is exact; the *inputs* to it were wrong.

The real cause: each `union(...)` arm is an ordinary Trilogy select, and a
Trilogy select is a SET at its output grain. The candidate's arms project
`(ws, dow, sales)` with no row key, so two different sales in the same
`(week_seq, day_of_week)` cell with an identical `ext_sales_price` collapse to
one row inside the arm, before `daily` sums them. Every downstream total is
slightly undersummed; 26 of 53×7 ratio cells shift across a 2-decimal rounding
boundary (±0.01, both directions).

## Proof

Rerunning the archived candidate on the run's workspace DB
(`results/20260711-185953_enriched/workspace`), probing week 5270 / Wednesday:

```text
candidate wed_total(5270)             = 5,776,939.77
candidate wed_future(5270)            = 3,626,355.80
candidate raw ratio                   = 1.5930  -> 1.59 (its own arithmetic is right)

true sum(5270, Wed)                   = 5,791,430.65
true sum(5323, Wed)                   = 3,627,660.28
```

The window fetched exactly week 5323's value — identity intact. And the
candidate's numbers are reproduced **to the cent** by per-arm
`SELECT DISTINCT (ws, dow, price)` then `UNION ALL` then sum:

```sql
with web_arm as (
  select distinct d_week_seq ws, d_dow dow, ws_ext_sales_price sp
  from web_sales join date_dim on d_date_sk = ws_sold_date_sk),
cat_arm as (
  select distinct d_week_seq ws, d_dow dow, cs_ext_sales_price sp
  from catalog_sales join date_dim on d_date_sk = cs_sold_date_sk),
stacked as (select * from web_arm union all select * from cat_arm)
select ws, sum(sp) from stacked
where ws in (5270, 5323) and dow = 3 group by 1;
-- 5270 -> 5776939.77, 5323 -> 3626355.80  (matches candidate exactly)
```

The original report's "additional clue" (denominator resembling week 5250's
total) was a numeric coincidence: 3,634,533.13 and the deduped 3,626,355.80
both round the ratio to 1.59.

## Why this is semantics, not a defect

`select item_id, val` dedupes duplicate pairs in plain Trilogy too — with or
without a union, with or without a rowset wrapper. The union TVF is consistent:
arms dedup within themselves (they are selects), while cross-arm duplicates
stack (`UNION ALL` of the arm sets — already pinned by
`test_tvf_union_named` / `test_tvf_union_renamed_output_preserves_arm_duplicates`).

Guard test added:
`tests/engine/test_duckdb_rowset.py::test_tvf_union_arm_is_set_semantic_within_arm`
pins both halves — within-arm dedup, and multiplicity preservation when the arm
carries a row key.

## Correct idioms for a multiplicity-preserving stack

Either aggregate inside each arm (what the canonical reference effectively
does):

```preql
with all_sales as union(
  (select web.date.week_seq as ws, web.date.day_of_week as dow,
          sum(web.ext_sales_price) as sales),
  (select cat.sold_date.week_seq as ws, cat.sold_date.day_of_week as dow,
          sum(cat.ext_sales_price) as sales)
) -> (ws, dow, sales);
```

or carry a distinguishing key through the arm so the arm's grain keeps one row
per sale.

## Eval guidance takeaway

Agent-facing docs/prompts should call out: "`union(...)` arms are set-semantic
selects — stacking raw measures for later aggregation requires per-arm
aggregation or a carried key." This is theme D (idiom gap), same family as the
q66/q84 verdicts.

---

Original (superseded) report follows for the record: the run, artifacts, shape,
and the sparse-partition analysis remain accurate; the "Suspected failure" and
"Expected behavior" sections attributed the miss to the wrong mechanism, and
the suggested dense regression fixture cannot reproduce the miss because it has
no duplicate `(week, weekday, value)` source rows.

## Run

```text
evals/tpcds_agent/results/20260711-185953_enriched
```

Artifacts:

```text
workspace/query02.preql
agent_log.q02.jsonl
report.json
```

Report:

```text
status: fail
reference rows: 53
candidate rows: 53
detail: result set differs from reference
```

## Candidate shape

The candidate stacks web and catalog sales, aggregates to one row per
`(week_seq, day_of_week)`, calculates the value 53 weeks ahead within each
weekday partition, and pivots the seven weekday ratios:

```preql
with daily as
select
    all_sales.ws,
    all_sales.dow,
    sum(all_sales.sales) as total;

with future as
select
    daily.ws,
    daily.dow,
    daily.total,
    lead(daily.total, 53)
        over (partition by daily.dow order by daily.ws asc) as future_total;

select
    future.ws,
    round(
        sum(future.total ? future.dow = 3)
        / sum(future.future_total ? future.dow = 3),
        2
    ) as wednesday_ratio
where
    future.future_total is not null
    and future.ws in ws_2001_all.ws
order by future.ws asc;
```

## Why the authored `lead` is valid

For the union of web and catalog sales, every weekday partition covers the
same 262 dense week sequences, and

```text
lead(week_seq, 53) over (partition by day_of_week order by week_seq)
    = week_seq + 53
```

held with zero misaligned rows, so `lead(..., 53)` is semantically equivalent
to a `week_seq + 53` self-join on this dataset. (Still true — the window was
never the problem.)
