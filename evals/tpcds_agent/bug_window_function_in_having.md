# Bug/handoff: window predicate in HAVING → QUALIFY re-renders the window → INVALID_REFERENCE_BUG

**Status:** OPEN. Found 2026-06-07 (q75); re-confirmed 2026-06-22 on trilogy 0.3.285,
now also implicated in q51. Combined cost ~4.5M tokens (q75 2.93M + q51 1.64M).
**Severity:** medium-high — a `having` whose condition contains a window function
emits SQL with an `INVALID_REFERENCE_BUG` sentinel and `generate_sql` raises
`ValueError: Invalid reference string found in query`. The agent gets an opaque
"please file an issue" error and abandons the (correct) windowed approach.

**Minimal unit test (checked in):** `tests/test_window_in_having_qualify_render.py`
— `xfail(strict=True)` for the valid-SQL assertion, a pin on the current
`ValueError`, and a `control` test showing the wrapper-select idiom already works.
Self-contained fixture, no DB. Delete the markers when it passes.

## Root cause (this is the actionable part)

Qualify-routing already exists. In `trilogy/dialect/base.py`, the CTE-condition
render (~L1979-2050) splits the HAVING condition: atoms where
`contains_window(...)` is true are moved into a `QUALIFY` clause (DuckDB has
`SUPPORTS_QUALIFY = True`), the rest stay in `HAVING`. So detection works.

The bug is in **how** the qualify atom is rendered: it **re-renders the entire
window expression from scratch at the outer scope**, where the window's inner
aggregate is no longer resolvable. For

```trilogy
select sales.store as store, sales.day as day, sum(sales.amt) as amt,
  max(sum(sales.amt)) over (partition by sales.store order by sales.day) as rm
having max(sum(sales.amt)) over (partition by sales.store order by sales.day) > 0
```

the inner `sum(sales.amt)` is materialized in an upstream CTE as
`_virt_agg_sum_<hash>`, and the window `rm` is **already computed** in the next CTE.
But the final node re-derives the window in QUALIFY against the un-exposed
aggregate, yielding:

```sql
...
QUALIFY
    max(INVALID_REFERENCE_BUG) over (partition by ... order by ... ) > 0
```

i.e. the inner aggregate renders as the `INVALID_REFERENCE_BUG` sentinel because it
is not a resolvable column at that scope. (In the q51/q05 shapes the final node is
also a FULL-JOIN merge node, so the outer scope is `coalesce(...)` columns — the
aggregate is even further out of reach.) Reproduces with a plain `rank()` too —
any window-over-aggregate in HAVING, projected or not.

## Fix direction (two options)

1. **Reference the materialized window column instead of re-deriving.** The window
   predicate's expression is (or can be) projected as an output column in the
   inner CTE (`rm` above). The QUALIFY/HAVING filter should point at that
   materialized column (`"<cte>"."rm" > 0`), not re-render `max(sum(...)) over(...)`.
   This keeps QUALIFY for dialects that have it.

2. **Lower to wrapper-select + outer WHERE (dialect-agnostic, preferred).** Project
   the window as a (possibly hidden) column in an inner select and apply the filter
   in a wrapping select. This is exactly what the `control` test does by hand and it
   works today on every dialect; it also lets you drop the `SUPPORTS_QUALIFY`
   special-case and the "not allowed in having" rejection path. The existing
   window-projection lowering already materializes windows this way — reuse it so
   the HAVING-window takes the same path as a SELECT-window + outer filter.

Either way the rendered SQL must never contain `INVALID_REFERENCE_BUG`. If neither
lowering is feasible, the plan-time guard must reject with a clear, actionable
message (project the window as a column and filter on it) — never emit the sentinel.

## Reproductions

- **Minimal (no DB):** see `tests/test_window_in_having_qualify_render.py`. Or:
  ```python
  from pathlib import Path
  from trilogy import Dialects
  from trilogy.core.models.environment import Environment
  env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
  eng = Dialects.DUCK_DB.default_executor(environment=env)
  eng.generate_sql('''import all_sales as s;
  where s.date.year = 2001
  select s.item.id as item_id, sum(s.ext_sales_price) as amt
  having rank() over (order by sum(s.ext_sales_price) desc) <= 10
  limit 100;''')   # raises ValueError: Invalid reference string
  ```
- **q51 verbatim (FULL-JOIN-merge variant):**
  `evals/tpcds_agent/results/20260622-174304/agent_log.q51.jsonl` (the
  `max(max(web_rt)) over (...)` running-max compare in HAVING).
- **q75 (original lag-ratio variant):**
  `evals/tpcds_agent/results/20260607-005408/workspace/` (`query75_test*.preql`,
  the `having sum(net_qty)/coalesce(lag(sum(net_qty),1) over (…),…) < 0.9` form).

## Note on the "fixed → QUALIFY" claim

The 2026-06-07 work added `contains_window` + QUALIFY routing + `SUPPORTS_QUALIFY`,
but it does NOT make window-over-aggregate-in-HAVING render correctly (above). Treat
any memory/changelog that calls window-in-HAVING "fixed" as covering only the
routing, not the render.

## Provenance

Enriched eval q75 (year-over-year per-line dedup + ratio threshold) and q51
(per-(item,date) web-vs-store running-max compare). In both, the agent reached for
a windowed comparison filtered in `having`, hit the opaque error, and abandoned the
correct approach — contributing to wrong final answers and large token burn.
