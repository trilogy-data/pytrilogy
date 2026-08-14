# Bug: q95 filtered count over `is_returned` emits `count(max(CASE ...))`

**Reproduced OPEN 2026-08-13 against HEAD.** This is distinct from
`bug_q95_explore_json_hides_fk_and_filter_scope.md`: that report covers an
exploration/guidance problem from an older run; this report is a directly
reproduced generated-SQL failure.

## Summary

The enriched q95 agent used the documented filtered-aggregate idiom to count,
per order, whether any web-sale line had a return:

```preql
count(ws.order_number ? ws.is_returned) by ws.order_number
```

`ws.is_returned` is the curated model's public helper for the nullable return
side. Trilogy accepted the query, but generated a single DuckDB aggregation
containing:

```sql
count(max(CASE WHEN WR_ORDER_NUMBER is not null
               THEN coalesce(WR_ORDER_NUMBER, WS_ORDER_NUMBER)
               ELSE NULL END))
```

DuckDB correctly rejects this as `aggregate function calls cannot be nested`.
An authored query that parses and resolves must either generate valid SQL or be
rejected with an actionable authored error; emitting backend-invalid SQL is a
framework bug.

## Artifacts

- Run: `evals/tpcds_agent/results/20260813-125008_enriched`
- Task: `task.q95.txt`
- Trajectory: `agent_log.q95.jsonl` / `agent_log.q95.conversation.txt`
- Unstaged candidate:
  `workspace/_worker_0/answer_569612608.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query95.{sql,preql}`
- Model helper: `tests/modeling/tpc_ds_duckdb/web_sales.preql:61,69,119-130`

The full candidate defined eligibility over all order lines, then applied the
report filters separately:

```preql
import raw.web_sales as ws;

auto eligible_order <- (count_distinct(ws.warehouse.sk) by ws.order_number) > 1
                   and (count(ws.order_number ? ws.is_returned) by ws.order_number) > 0;

select
  count_distinct(ws.order_number) as eligible_order_count,
  sum(ws.ext_ship_cost) as total_ext_ship_cost,
  sum(ws.net_profit) as total_net_profit
where eligible_order
  and ws.ship_date.date >= '1999-02-01'::date
  and ws.ship_date.date <= '1999-04-02'::date
  and ws.pos_ship_address.state = 'IL'
  and ws.web_site.company_name = 'pri'
order by eligible_order_count desc
limit 100;
```

The failure occurs while building the return-count side, before the report
filters or final aggregates matter.

## Minimal reproduction

Against the run workspace, using `evals.common.scoring.make_scoring_engine`:

```preql
import raw.web_sales as ws;

select
    ws.order_number,
    count(ws.order_number ? ws.is_returned) by ws.order_number as returned_count
limit 1;
```

`generate_sql` emits `count(max(CASE ...))`; executing that SQL raises:

```text
_duckdb.BinderException: Binder Error: aggregate function calls cannot be nested
```

The same failure occurs when the filtered count is assigned to an `auto` and
used as a WHERE predicate:

```preql
auto returned_count <- count(ws.order_number ? ws.is_returned) by ws.order_number;
where returned_count > 0
select ws.order_number
limit 1;
```

## Trigger matrix

| Shape | Generated SQL / outcome |
|---|---|
| `count(ws.order_number ? ws.is_returned) by ws.order_number` in SELECT | `count(max(CASE ...))`; DuckDB BinderException |
| Same filtered count in an `auto`, consumed by WHERE | `count(max(CASE ...))`; DuckDB BinderException |
| `count(ws._returned_order_number) by ws.order_number` | Plain `count(WR_ORDER_NUMBER)`; executes successfully |
| Public `ws.is_returned` without the outer count | The helper resolves; the failure requires the filtered key plus outer aggregate |

The raw-column control is diagnostic only. `_returned_order_number` is private
model implementation detail and the model explicitly tells users to use
`is_returned`; requiring agents to bypass that helper is not an acceptable fix.

## Root cause and likely fix area

`ws.order_number ? ws.is_returned` becomes a filtered concept whose per-row
rendering is a `CASE WHEN`. At the order grain, Trilogy deliberately collapses
that filtered key's `{order_number, NULL}` fan-out with `MAX`:

- `trilogy/core/models/execute.py:559-590` —
  `CTE.filter_collapses_to_grain` identifies the filtered concept as needing a
  keyed collapse.
- `trilogy/dialect/base.py:1371-1379` — the renderer applies
  `MAX(<rendered filtered concept>)`.

That collapse is valid by itself, but the outer `count(...)` is rendered in the
same SQL aggregation layer, producing the illegal nested aggregate. The planner
does not materialize the collapsed filtered value before consuming it, nor does
the same-grain aggregate simplifier reduce the count to a non-nested expression.

Fix direction: ensure an aggregate consuming a collapse-to-grain filter either:

1. stages the `MAX(CASE ...)` in an inner CTE and counts its materialized column;
2. applies a valid same-grain rewrite such as a null test around the collapsed
   value; or
3. pushes the filter predicate so the filtered content is a bare column when
   that is semantically equivalent.

Do not globally remove the `MAX` collapse: the comments in `execute.py` tie it
to the q16 mixed-key double-count fix.

## Expected behavior and regression coverage

The minimal query should execute and return `0` or `1` per order according to
whether that order has any returned line. Add tests covering:

1. filtered `count(key ? boolean_helper) by key` where the helper depends on a
   nullable partial datasource;
2. SELECT and WHERE/`auto` consumers;
3. generated SQL contains no nested aggregates;
4. result equivalence with the raw nullable return-key control;
5. preservation of the q16 filter-collapse behavior that motivated `MAX`.

