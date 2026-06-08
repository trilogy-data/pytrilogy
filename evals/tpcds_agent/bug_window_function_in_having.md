# Bug: window function in HAVING → invalid SQL (not lifted / not rejected)

**Status:** OPEN (found 2026-06-07, enriched eval q75).
**Severity:** medium — a `having` whose condition contains a window function (`lag/rank/… over (…)`)
is accepted by Trilogy but compiles to SQL DuckDB rejects, OR to an `INVALID_REFERENCE_BUG`. The
agent gets an opaque engine error with no path forward. Contributed to q75 (2.93M tokens).

## Symptom (two shapes, same root)

**Shape 1 — `lag()` in HAVING → DuckDB rejects at execute:**
```
(_duckdb.BinderException) Binder Error: HAVING clause cannot contain window functions!
```
Generated SQL contains `HAVING (… ) / coalesce(lag(…) over (partition by … order by …), 1) < 0.9`.
`generate_sql` succeeds; the failure is at execution.

**Shape 2 — `rank()` in HAVING → codegen dangling reference:**
```
ValueError: Invalid reference string found in query: …
```
`generate_sql` itself raises (the window over an aggregate produces an unresolved CTE reference —
related to the B2 `INVALID_REFERENCE` family).

Either way: a window in `having` is never lifted into a wrapping `SELECT` / `QUALIFY`, and is not
rejected at plan time. SQL `HAVING` cannot contain window functions; Trilogy should either rewrite
to a wrapper-select + outer filter (the standard lowering) or reject with a clear message.

## Deterministic reproduction (checked-in enriched model)

Model: `tests/modeling/tpc_ds_duckdb` (`all_sales.preql`). Driver:
```python
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
eng = Dialects.DUCK_DB.default_executor(environment=env)
sql = eng.generate_sql(Path("repro.preql").read_text())[-1]   # Shape 2 raises here
# Shape 1: generate succeeds; execute the SQL to hit the BinderException.
```

### Shape 2 repro (`rank` in having → INVALID_REFERENCE at generate)
```trilogy
import all_sales as sales;
where sales.date.year = 2001
select
    sales.item.id as item_id,
    sum(sales.ext_sales_price) as amt
having rank() over (order by sum(sales.ext_sales_price) desc) <= 10
limit 100;
```

### Shape 1 (the original q75 form — `lag` ratio in having)
The exact crashing query is preserved at
`evals/tpcds_agent/results/20260607-005408/workspace/` (q75 `query75_test*.preql`, the
`having sum(net_qty) / coalesce(lag(sum(net_qty),1) over (…), …) < 0.9` variant). It generates SQL
with a window inside `HAVING`; executing against `tpcds.duckdb` raises the BinderException above.

## Suggested fix

A window function in a `having` condition should be lowered to a projected (possibly hidden) column
in an inner select and the filter applied in a wrapping select (or via `QUALIFY` where the dialect
supports it) — the same lowering windows already get when projected. If lowering is out of scope,
reject at plan time: "window functions are not allowed in `having`; project the window as a column
(`--rank() over (…) as r`) and filter on it." The current behavior (emit invalid SQL / dangling
reference) is the worst option.

## Provenance

Enriched eval q75 (year-over-year per-line dedup + ratio threshold). The agent reached for a
`lag()`-based prior-year comparison filtered in `having`, hit the BinderException, could not
diagnose it, and abandoned the (correct) windowed approach — contributing to the wrong final answer.
