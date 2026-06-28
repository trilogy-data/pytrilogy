# Bug: INVALID_REFERENCE_BUG in a top-N rank-filter rowset (q70 net-profit/rollup report)

**Status:** OPEN (diagnosed 2026-06-28, read-only investigation). Do NOT confuse with the OLD q70
GROUPING bug, which is **already FIXED** (see "Relationship to the old GROUPING bug" below).
**Severity:** high — valid-looking query crashes at SQL render with the internal
`INVALID_REFERENCE_BUG` sentinel ("this should never occur. Please create an issue"); the q70 agent
run burned ~715k tokens thrashing on it.
**Run:** `evals/tpcds_agent/results/20260628-175514/` (`agent_log.q70.jsonl`, file writes 1 and 3).
**Area:** sentinel emitted at `trilogy/dialect/base.py:1234` (`INVALID_REFERENCE_STRING(...)` with
empty callsite → bare `BASE_INVALID`, defined line 253/256). The missing source-map wiring is
produced upstream in the rowset / window-order-by planning path, not in base.py.

## Symptom

`trilogy run query70.preql` → `ValueError: Invalid reference string found in query: ... this should
never occur.` The offending CTE is the top-5-states rank filter, whose window **ORDER BY** is the
sentinel:

```sql
cooperative as (
SELECT
    "cheerful"."_top_states_state"       as "_top_states_state",
    "cheerful"."_top_states_state_total" as "_top_states_state_total"
FROM
    "cheerful"
QUALIFY
    rank() over (order by INVALID_REFERENCE_BUG desc ) <= 5
),
```

Note the parent CTE `cheerful` *already materializes* the aggregate as
`sum(...) as "_top_states_state_total"` — the order-by simply fails to resolve to that column.
The downstream rollup/grouping CTEs in the same query (`late`, `yummy`, `abhorrent`) render
`GROUP BY ROLLUP(2, 1)` with `grouping(...)` correctly, i.e. the rollup/grouping path is healthy.

## Failing variant (from the agent log)

The agent reached for a two-step "top-5 states, then rollup" shape. Both forms it tried crashed
identically — `rowset top_states <- ... having rank(state) over (order by sum(net_profit) desc) <= 5`
followed by membership `where store_sales.store.state in top_states.state`. The membership, the
rollup, and the `grouping()` columns are **all irrelevant** to the crash (minimized away below).

## Minimal repro (no membership, no rollup, no grouping)

Model: any with `store_sales` (used `tests/modeling/tpc_ds_duckdb`, also the run's
`results/20260628-175514/workspace/raw`).

```trilogy
import store_sales as store_sales;
rowset top_states <-
where store_sales.date.year = 2000
select
    store_sales.store.state as state,
    sum(store_sales.net_profit) as state_total          -- projected aggregate column
having
    rank(store_sales.store.state) over (order by sum(store_sales.net_profit) desc) <= 5;
select top_states.state, top_states.state_total;
```

Command (read-only generate_sql):

```python
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
eng = Dialects.DUCK_DB.default_executor()
eng.environment = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
print(eng.generate_sql(text)[-1])   # raises ValueError, SQL contains INVALID_REFERENCE_BUG
```

### Bisection (what is / isn't required)

| Variant | Result |
|---|---|
| rowset projects `state` **and** `sum(...) as state_total`; HAVING rank order-by = **inline** `sum(net_profit)` | **CRASH** |
| same, but HAVING rank order-by references the **named/projected** `state_total` concept | OK |
| rowset projects **only** `state` (no `state_total` col); inline `sum(...)` order-by in HAVING | OK |
| top-level `where state_rank <= 5` (no rowset) over a named `state_rank <- rank(...) over (order by state_profit desc)`, even with the full rollup+grouping+order-by | OK |

So the trigger is the **conjunction**: a rowset that (a) projects an aggregate output column
(`sum(...) as state_total`) **and** (b) has a `having rank(<key>) over (order by <inline aggregate>)`
where the order-by is an *inline* `sum(net_profit)` rather than the projected concept. The membership,
rollup and grouping in the real q70 are bystanders.

## Root cause

The rank's ORDER BY argument is an **inline aggregate** (`sum(store_sales.net_profit)`) whose concept
*address* is distinct from the rowset's projected `state_total` aggregate — even though both are the
**same lineage** (`sum(net_profit)`) at the **same grain** (per `state`, the rowset grain). The
QUALIFY CTE (`cooperative`) is built with a `source_map` that carries the projected
`_top_states_state_total` but **not** the anonymous inline-aggregate address the window order-by
points at. At render, `_render_concept_sql`'s basic-lookup branch finds `c.address` absent from
`cte.source_map` → `safe_get_cte_value` returns `None` → falls through to
`INVALID_REFERENCE_STRING("Missing source reference to ...")` with empty callsite →
bare `INVALID_REFERENCE_BUG` (`trilogy/dialect/base.py:1216-1236`, sentinel def line 253/256).

The "named order-by works" row proves it: when the order-by references the projected concept, the
address matches the materialized column and resolves. The two equal aggregates are simply never
collapsed to one address, so the rank-filter CTE's order-by has nothing to bind to.

## Relationship to the OLD q70 GROUPING bug

**Distinct construct; the GROUPING bug is fixed.** The old failure
(`evals/tpcds_agent/bug_q70_grouping_without_groups_binder.md`, CLOSED/FIXED 2026-06-28) was a DuckDB
`GROUPING statement cannot be used without groups` BinderException from a standalone scalar
`grouping()` CTE with no GROUP BY — handled by `_propagate_select_grouping` raising a clean
author-time error for `grouping()` in WHERE. That fix landed: this run's rollup CTEs now emit
`GROUP BY ROLLUP(...)` with co-located `grouping()` correctly, and no `GROUPING statement cannot`
string appears anywhere in `agent_log.q70.jsonl`. The new `INVALID_REFERENCE_BUG` lives in a
completely separate part of the query — the **top-5-states rank-filter rowset** — and reproduces with
**no `grouping()` and no rollup at all** (see minimal repro). It is a newly-surfaced, independent
sentinel, not a regression or a downstream effect of the GROUPING fix.

## Family relation (INVALID_REFERENCE_*)

Same sentinel family as q75 / q23 / q64 / q38 (`bug_q75_invalid_reference_membership_virt_filter.md`,
`bug_q23_invalid_reference_best_customers_membership.md`,
`bug_q64_invalid_reference_eligible_items_membership.md`,
`bug_q38_invalid_reference_intersect_combos.md`) — all emit the bare `base.py:253` sentinel because a
concept's address is missing from a CTE `source_map`.

- **q75** = an *unprojected window-only grain key* fails to resolve in a HAVING-membership left tuple
  (fix materialized the hidden key). Mechanism: un-materialized key.
- **This q70** = an *inline aggregate inside a window ORDER BY* in a rowset HAVING-rank filter is a
  different address from the rowset's *projected* same-lineage aggregate; only the projected one is
  wired into the QUALIFY CTE. Mechanism: **aggregate equivalence-collapse** (two equal aggregates not
  unified), closer to the q38/q23 collapse family than to q75's key-materialization. The likely fix
  surface is either collapsing the inline order-by aggregate onto the projected concept, or wiring the
  order-by aggregate's source into the rank-filter CTE's source_map.
```
