# Bug: HAVING comparing a rollup-macro aggregate to a `by *` scalar → uncaught `INVALID_REFERENCE_BUG` crash (q14)

**Status:** FIXED 2026-06-26. The HAVING/SELECT aggregate-matching helpers in
`trilogy/parsing/v2/select_finalize.py` now (1) see through a `def`-macro's `FunctionCallWrapper`
to the inner aggregate (`_macro_inner_aggregate`, wired into `_aggregate_full_signature`,
`_collect_condition_aggregates`, `_substitute_having_aggregates`), and (2) normalize the SELECT-side
`by` (source names like `sales.channel`) against the HAVING-side `by` (output aliases like
`local.channel`, which finalize rewrites) via the pure-rename map. The HAVING rollup aggregate is then
pointed at its matching SELECT alias (materialized column) instead of being re-inlined where its
row-level inputs no longer exist. Both the macro form AND the equivalent inline form now compile and
execute. Regression test: `tests/engine/test_duckdb.py::test_rollup_macro_in_having_vs_scalar_colocates`.

---

**Original report (OPEN):** deterministic repro + bisection below. Same `INVALID_REFERENCE_BUG` sentinel family
as the (fixed) q64 nested-membership crash, but a different path (HAVING aggregate sourcing).
**Surfaced by:** TPC-DS q14 (run `20260626-212144`) — the run's **top token sink: 3.63M tokens / 61
LLM iterations / 15 errors**. This crash accounts for 2 of those 15 errors (it reappears as the agent
re-tries); it surfaces as `Unexpected error: Invalid reference string found in query` (uncaught
`ValueError`), so the agent gets no actionable message and loops.
**Severity:** HIGH — uncaught crash; contributes to the largest token burn in the set.

## Symptom

`ValueError: Invalid reference string found in query: ...` — the rendered SQL contains the
`INVALID_REFERENCE_BUG` sentinel (dialect/base.py:247 guard at :2386) where a HAVING aggregate's
source CTE alias should be.

## Bisection (minimal trigger)

From the agent's q14 query:

```trilogy
import raw.all_sales as sales;
auto overall_avg_sale <- avg(sales.quantity * sales.list_price ? sales.date.year between 1999 and 2001) by *;
def rollup_sales_filter() -> sum(sales.quantity * sales.list_price ? sales.date.year = 2001 and sales.date.month_of_year = 11)
                             by rollup sales.channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id;
where sales.date.year between 1999 and 2001
select sales.channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id
having @rollup_sales_filter() > overall_avg_sale     -- <-- removing this HAVING => compiles OK
order by ... limit 100;
```

| variant | result |
|---|---|
| full query | **ERR (Invalid reference string)** |
| **drop the HAVING** `@rollup_sales_filter() > overall_avg_sale` | **OK** |
| drop the unrelated `combo_key in qualifying_combos...` membership | still **ERR** |

So the **HAVING is the trigger**, and the membership is NOT involved. The HAVING compares a
**ROLLUP aggregate** (from a `def` macro, `sum(...) by rollup <4 dims>`) against a **`by *` scalar**
(`overall_avg_sale`). One of those HAVING operands' source CTEs is never materialized → sentinel.

## Likely fix area

HAVING-aggregate sourcing for a rollup aggregate (and/or the `by *` scalar) — the operand isn't
promoted to a materialized output/CTE before the HAVING references it, mirroring the dangling-CTE
INVALID_REFERENCE family (`v4_helper` source planning / `select_finalize` HAVING promotion). Compare
with the fixed q14 `grouping()`-in-HAVING promotion (`_promote_having_grouping_to_outputs`) and the
q64 existence-sourcing fix — this is the rollup-aggregate-in-HAVING analogue. At minimum it must
become a clean error instead of an uncaught `ValueError`.

## Repro

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-212144/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('q14.preql').read())   # ValueError: Invalid reference string found in query
```

## Context: q14's other token drivers (for whoever picks this up)

q14's 15 errors over 61 iterations also include: **HAVING-references-not-in-SELECT / HAVING-aggregate
ergonomics (6×)** — the recurring wall where a HAVING references a derived/aggregate not projected;
**rowset-output namespacing undefined (2×)** (`channel_data.sales.channel`); **`Tuple must have same
type for all elements` (1×, also an uncaught Unexpected error)**; and a union arm column-count
mismatch (1×). The HAVING ergonomics cluster + this crash are the bulk of the burn.
