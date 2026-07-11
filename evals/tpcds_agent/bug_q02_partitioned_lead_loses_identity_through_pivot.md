# Bug: q02 partitioned `lead` loses row identity through downstream pivot

## Severity

Silent wrong-result framework bug. A valid partitioned `lead(..., 53)` query
executes successfully and returns the expected 53-row shape, but some ratios use
the wrong denominator after the windowed rowset is pivoted with filtered
aggregates.

There is no error or warning. The agent's local validation looks plausible.

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

The complete seven-day query is preserved in the run workspace.

## Why the authored `lead` is valid

The initial suspicion was that a weekday partition might be sparse, making “53
rows ahead” differ from “week sequence + 53.” Direct inspection disproved that.

For the union of web and catalog sales:

```text
day_of_week 0: 262 distinct week sequences
day_of_week 1: 262 distinct week sequences
day_of_week 2: 262 distinct week sequences
day_of_week 3: 262 distinct week sequences
day_of_week 4: 262 distinct week sequences
day_of_week 5: 262 distinct week sequences
day_of_week 6: 262 distinct week sequences
```

For every 2001 `(week_seq, day_of_week)` row, SQL verification showed:

```text
lead(week_seq, 53) over (partition by day_of_week order by week_seq)
    = week_seq + 53
```

There were zero misaligned rows. The agent's window definition is therefore
semantically equivalent to an explicit `current.week_seq + 53 = future.week_seq`
join for this dataset.

## Concrete wrong value

For Wednesday (`day_of_week = 3`) in week sequence 5270:

```text
week 5270 total: 5,791,430.65
week 5323 total: 3,627,660.28
ratio:           1.596464443467678
rounded:         1.60
```

The candidate returned:

```text
1.59
```

That is not a floating-point tie. The correct value is comfortably above
1.595.

As an additional clue, `1.59` is the rounded result obtained using the
Wednesday total for week 5250 as the denominator:

```text
week 5250 total: 3,634,533.13
5,791,430.65 / 3,634,533.13 -> 1.59
```

This suggests the downstream plan is sourcing or stitching `future_total` from
the wrong `(week_seq, day_of_week)` identity rather than merely rounding the
correct ratio incorrectly.

## Expected behavior

The `future` rowset has grain `(ws, dow)`. Its window output `future_total` must
remain attached to that complete grain through later filtered aggregation.

For a final group `ws = 5270`:

```preql
sum(future.future_total ? future.dow = 3)
```

must select only the Wednesday `future_total` belonging to `(5270, 3)`, whose
source is `(5323, 3)`.

Projection, regrouping, filtered aggregates, or source splitting must not
detach the window output from either `ws` or `dow`.

## Suspected failure

The problematic sequence is:

```text
aggregate by (week, weekday)
  -> partitioned window over weekday ordered by week
  -> named rowset
  -> filtered aggregate by weekday
  -> regroup at week
```

The planner likely sources `total` and `future_total` through separate branches
or loses a hidden component of the window row identity during the final pivot.
The final branch then stitches a valid future value onto the wrong week.

Likely areas:

```text
trilogy/core/processing/node_generators/
trilogy/core/processing/nodes/window_node.py
trilogy/core/processing/nodes/group_node.py
trilogy/core/processing/join_resolution.py
trilogy/core/optimizations/
```

## Regression fixture

Build a small dense table with at least four weeks and two weekdays. Use a
smaller offset such as two so the expected values are obvious:

```text
week  weekday  total
1     0        10
2     0        20
3     0        30
4     0        40
1     1       100
2     1       200
3     1       300
4     1       400
```

Then:

```preql
rowset future <- select
    sales.week,
    sales.weekday,
    sales.total,
    lead(sales.total, 2)
        over (partition by sales.weekday order by sales.week asc) as future_total;

select
    future.week,
    sum(future.total ? future.weekday = 0) as current_0,
    sum(future.future_total ? future.weekday = 0) as future_0,
    sum(future.total ? future.weekday = 1) as current_1,
    sum(future.future_total ? future.weekday = 1) as future_1;
```

Expected for week 1:

```text
current_0 = 10
future_0  = 30
current_1 = 100
future_1  = 300
```

## Test matrix

Pin the first failing transition:

1. select raw `future` rows;
2. select one weekday with a WHERE filter;
3. aggregate `future_total` by `(week, weekday)`;
4. aggregate by week with one filtered weekday sum;
5. add all weekday pivots;
6. divide current by future;
7. add the `future_total is not null` predicate;
8. place the aggregate in a named rowset versus inline;
9. compare against an explicit `week + offset` self-join oracle.

For every shape assert both values and generated SQL join/window identity. Also
test sparse partitions separately; they are a legitimate semantic caveat but
are not the cause of this q02 failure.

## Workaround

Until fixed, pivot to one row per week before applying seven independent
windows, as the canonical reference does, or use an explicit self-join on:

```text
current.week_seq + 53 = future.week_seq
```

The workaround does not make the agent-authored query invalid; it only avoids
the planner path that loses the windowed row identity.
