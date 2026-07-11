# Handoff — `?` filter mixing an aggregate-comparison AND a row predicate → GROUP BY leak (BinderException)

**Status:** FIXED 2026-07-11. Two changes in `trilogy/core/processing/node_generators/filter_node.py`:
1. `_can_pushdown_as_grouped_filter` now rejects a body mixing an aggregate row-argument with any non-aggregate row-argument other than the content key itself — the row predicate is invalid in HAVING at the group grain, and pre-filtering rows would corrupt the aggregate (it must compute over all rows). The planner falls back to the standard plan (aggregate materialized at its own grain, predicate evaluated at row level).
2. The fallback FilterNode now applies the predicate as a node condition when the filter concept is the sole requested output (renders as a plain WHERE over the materialized aggregate column), fixing the NULL-group row the per-row CASE alone leaked — a pre-existing wart that also affected grain-mismatch filters.

Regression tests: `tests/discovery/test_filter_mixed_aggregate_row_predicate.py`. Verified against the 20260711-042547 workspace: probe B compiles, executes, and its 1901 order numbers exactly match the `where`-form reference.

Original report follows.

---

**Status (original):** OPEN framework codegen bug. Loud (DuckDB rejects the emitted SQL). Latent in the eval — surfaced by the q95 probe (`bug_q95_explore_json_hides_fk_and_filter_scope.md`, probe B); the agent never actually wrote it, so it cost no scored fail. Filed as its own handoff because it is a genuine invalid-SQL emission from Trilogy codegen.

Per project rule: a `BinderException` from Trilogy-GENERATED SQL is ALWAYS a framework bug — the engine must either compile it or reject it with a clear authored error, never emit SQL the database rejects.

## Symptom

A `?`-filtered rowset whose filter body **combines an aggregate-comparison with a row-level predicate** via `and` emits SQL that GROUPs BY the row-predicate column but SELECTs an ungrouped, un-aggregated column:

```
(_duckdb.BinderException) Binder Error: column "ws_order_number" must appear in the
GROUP BY clause or must be part of an aggregate function.
```

## Minimal repro

Against any workspace with the ingested web_sales model (used `evals/tpcds_agent/results/20260711-042547_enriched/workspace`, `make_scoring_engine` + `generate_sql`/`execute_raw_sql`):

```trilogy
import raw.web_sales as ws;
select ws.order_number ? count(ws.warehouse.sk) by ws.order_number > 1
                        and ws.ship_address.state = 'IL' as on;
```

→ `generate_sql` succeeds, `execute_raw_sql` raises the BinderException above.

## Trigger matrix (one ingredient toggled at a time)

| probe | filter body | outcome |
|-------|-------------|---------|
| A | `count(ws.warehouse.sk) by ws.order_number > 1` (pure aggregate) | OK |
| B | `count(ws.warehouse.sk) by ws.order_number > 1 and ws.ship_address.state='IL'` (agg **and** row) | **BinderException** |
| C | `ws.ship_address.state='IL'` (pure row) | OK |
| D | `count(...) by ws.order_number > 1 and sum(ws.is_returned::int) by ws.order_number > 0` (two aggregates) | OK |

Discriminator: **exactly one aggregate-comparison AND at least one row-level predicate in the same `?` body.** Pure-aggregate, pure-row, and two-aggregate bodies all codegen correctly.

## The bad SQL (probe B, emitted)

The filter is split incorrectly: the aggregate half goes to `HAVING`, the row half is inlined into a `CASE WHEN` in the SELECT, and the `GROUP BY` is set to the row-predicate column — leaving `ws_order_number` (the actual select output) neither grouped nor aggregated.

```sql
SELECT
    CASE WHEN count("questionable"."ws_warehouse_sk") > 1
              and "questionable"."ws_ship_address_state" = 'IL'
         THEN "questionable"."ws_order_number" ELSE NULL END as "on"
FROM "questionable"
GROUP BY "questionable"."ws_ship_address_state"        -- wrong grain
HAVING count("questionable"."ws_warehouse_sk") > 1
```

Expected: the row predicate should filter (WHERE), the aggregate-comparison should filter the grouped rowset (HAVING) at the `ws.order_number` grain, and the output should be `ws.order_number` grouped by `ws.order_number` — never a GROUP BY on the row-predicate column.

## Where to look (not root-caused to a line)

The defect is in how a filtered-rowset `?` condition with mixed aggregate/row operands is decomposed into WHERE / HAVING / projection and how the resulting GROUP BY grain is chosen. Likely starting points:
- `trilogy/core/processing/node_generators/filter_node.py`
- `trilogy/core/processing/condition_utility.py` (condition decomposition)
- `trilogy/core/processing/nodes/group_node.py` / `node_generators/group_node.py` (GROUP BY grain selection)

Cross-reference the "filtered aggregate disjoint `?`-pushdown" and group-node pushdown work (see repo memory: `project_filtered_aggregate_disjoint_pushdown_drops_groups`, `project_group_node_condition_pushdown`) — this looks like the same decomposition path mishandling the mixed agg+row case.

## Definition of done

Probe B (and the general mixed agg+row `?` body) either:
- compiles to correct SQL (row predicate → WHERE, aggregate-comparison → HAVING at the select grain, GROUP BY on the output key), OR
- is rejected with a clear authored error.

Add a `tests/join_matrix` (or filtered-rowset) cell covering mixed aggregate+row `?` filters so it can't regress.
