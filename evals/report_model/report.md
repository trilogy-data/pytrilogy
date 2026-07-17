# TPC-DS Eval Health Report

Generated from `evals/eval_history.db` via the semantic model in `eval_runs.preql`.
Render with (run from `evals/report_model/`, which holds the sqlite `trilogy.toml`):

```
trilogy render report.md --to html
```

Trends cover **complete runs** (all 99 queries graded) across all four
generation paths — `enriched` (Trilogy), `sql_bare`, `sql_schema`, `ingest` —
with single-question `repeat_` runs and `scope_` A/B sweeps excluded. The setup
block below defines the handful of scopes reused across panels; per-panel
filters live in each chart's `from select`.

```trilogy
import eval_runs as e;

# Questions graded in a given run (broadcast to each of its rows).
auto run_size <- count(e.id) by e.run_timestamp, e.variant;
# A row belongs to a complete, non-experiment run — all 99 queries graded.
auto is_full_run <- e.is_experiment = false and run_size = 99;
# The most recent full-suite run per variant — used for the headline KPIs.
auto latest_full_ts <- max(e.run_timestamp ? is_full_run) by e.variant;
# Total failing runs per query across all full-suite variants — ranks the problem list.
auto question_fail_total <- count(e.id ? (e.passed = 0 and e.is_experiment = false)) by e.question;
```

## Latest pass rate by variant

Pass % of each path's most recent full-suite run.

:::row
```trilogy
chart
  layer headline ( x_axis <- enriched )
  from select round(100.0 * avg(e.passed), 1) -> enriched
    where e.variant = 'enriched' and is_full_run and e.run_timestamp = latest_full_ts;
```
```trilogy
chart
  layer headline ( x_axis <- sql_bare )
  from select round(100.0 * avg(e.passed), 1) -> sql_bare
    where e.variant = 'sql_bare' and is_full_run and e.run_timestamp = latest_full_ts;
```
```trilogy
chart
  layer headline ( x_axis <- sql_schema )
  from select round(100.0 * avg(e.passed), 1) -> sql_schema
    where e.variant = 'sql_schema' and is_full_run and e.run_timestamp = latest_full_ts;
```
```trilogy
chart
  layer headline ( x_axis <- ingest )
  from select round(100.0 * avg(e.passed), 1) -> ingest
    where e.variant = 'ingest' and is_full_run and e.run_timestamp = latest_full_ts;
```
:::

## Success rate over time

One line per generation path, pass % of every full-suite run, oldest to newest.
This is the number to watch — divergence between the Trilogy (`enriched`) line
and the raw-SQL lines is the framework gap.

```trilogy
chart
  layer line (
    x_axis <- e.run_timestamp,
    y_axis <- pass_pct,
    color <- e.variant
  )
  from select e.run_timestamp, e.variant, round(100.0 * avg(e.passed), 1) -> pass_pct
    where is_full_run
    order by e.run_timestamp asc;
```

Most recent full-suite run per variant:

```trilogy
select
  e.variant,
  e.run_timestamp,
  count(e.id) -> questions,
  sum(e.passed) -> passing,
  round(100.0 * avg(e.passed), 1) -> pass_pct
where is_full_run and e.run_timestamp = latest_full_ts
order by pass_pct desc;
```

## Top consistently failing queries

One dot per generation path, positioned by that path's **failure rate** —
`fails / runs` for the query on that path — so runs aren't over- or under-counted
when a path was benchmarked more often than another. A high enriched dot with low
raw-SQL dots is a Trilogy gap; dots that cluster high together point at the
question or the reference. Includes every query that failed on ≥24 runs, and rates
count all non-experiment runs (not just full-suite).

```trilogy
chart
  layer point (
    x_axis <- fail_rate,
    y_axis <- question,
    color <- e.variant
  )
  from select
      cast(e.question as string) -> question,
      e.variant,
      round(100.0 * (1 - avg(e.passed)), 1) -> fail_rate
    where e.is_experiment = false and question_fail_total >= 24;
```

```trilogy
select
  e.question,
  question_fail_total -> total_fails,
  round(100.0 * (1 - avg(e.passed ? e.variant = 'enriched')), 1) -> enriched_fail_rate,
  round(100.0 * (1 - avg(e.passed ? e.variant != 'enriched')), 1) -> sql_fail_rate
where e.is_experiment = false and question_fail_total >= 24
order by total_fails desc
limit 20;
```

## Failure mode breakdown

How the non-passing outcomes split across status codes (full-suite runs, all paths).

```trilogy
chart
  layer bar ( x_axis <- e.status, y_axis <- outcomes )
  from select e.status, count(e.id) -> outcomes
    where is_full_run and e.passed = 0
    order by outcomes desc;
```
