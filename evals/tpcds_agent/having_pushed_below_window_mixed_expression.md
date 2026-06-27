# Bug: HAVING pushed BELOW a window function (starving it) when the projection mixes a non-windowed term with the windowed one → silent wrong/NULL results

**Status:** FIXED 2026-06-26. Root cause: `_predicate_safe_past_windows`
(`trilogy/core/optimizations/predicate_pushdown.py`) detected a CTE's window functions by checking
only the *top-level* `col.lineage` for `BuildWindowItem`. In the mixed expression `val / lead(val, N)`
the output column's lineage is a `BuildFunction` (the divide) with the window one level down, so no
window was detected, the guard returned "safe", and the having/membership filter was pushed into the
window CTE (rendered as `WHERE`, which SQL evaluates before the window). Fix: generalized
`contains_window` into a `gather_windows(...) -> list[BuildWindowItem]` collector (same tree walk) in
`trilogy/core/processing/condition_utility.py`, and `_predicate_safe_past_windows` now calls it on each
output column's lineage so nested windows are reached. Regression test:
`tests/optimization/test_having_below_window_pushdown.py`. All 106 TPC-DS queries still pass.

Original report (HIGH severity — silent wrong results, canonical TPC-DS q2 came back ALL NULL):
**Surfaced by:** TPC-DS q2 (week-over-week Sunday/Monday/... ratios). The agent's query returned 53
rows, every measure NULL, and it burned many iterations not understanding why.

## The rule being violated

In Trilogy's clause order `where → select → having → order`, `having` is AFTER `select`, and filters
the aggregated/window RESULT. So a `having` predicate must be applied ABOVE any window function in the
`select`. Here the predicate-pushdown optimizer demotes the `having` filter BELOW the window (into the
pre-aggregation WHERE), so the window's input is restricted and `lead()/lag()` has nothing to look
ahead/back to → NULL.

## Trigger (precisely isolated)

The push happens ONLY when the projected expression **mixes a non-windowed term with the windowed
term** (e.g. `sum(x) / lead(sum(x), N) over (...)`). With the window ALONE it is handled correctly.

| projection | `having k in <set>` result |
|---|---|
| `lead(sum(x), N) over (order by k)` alone | **correct** — window sees all rows, having filters output |
| `sum(x) / lead(sum(x), N) over (order by k)` | **BUG** — having pushed below window → window starved → NULL |

## Minimal repro

```trilogy
# model: key wk int; property wk.val float; datasource d(wk,val) grain(wk)
#   rows: (1,10) (2,20) (3,30) (4,40) (5,50) (6,60)
import winhaving as f;
auto small <- f.wk ? f.val <= 30;          # {1,2,3}

# BUG — mixed expression: val / lead(val)
select f.wk, f.val / lead(f.val, 2) over (order by f.wk asc) as ratio
having f.wk in small
order by f.wk;
# => (1, 0.333), (2, NULL), (3, NULL)
#    wk=2 should be 20/lead(2)=20/40=0.5, but wk=4 was filtered out of the window → NULL

# CORRECT — window alone:
select f.wk, lead(f.val, 2) over (order by f.wk asc) as nxt
having f.wk in small order by f.wk;
# => (1,30),(2,40),(3,50)   -- window saw weeks 4,5; having filtered the OUTPUT
```

`wk=2` proves it: with the bug, the window can't see `wk=4` (it was pushed out), so `lead` is NULL.

## all_sales / TPC-DS q2 repro

```trilogy
import raw.all_sales as sales;
auto wk2001 <- sales.date.week_seq ? sales.date.year = 2001;
where sales.channel in ('WEB','CATALOG')
select sales.date.week_seq,
    round(sum(sales.ext_sales_price ? sales.date.day_name = 'Sunday')
          / lead(sum(sales.ext_sales_price ? sales.date.day_name = 'Sunday'), 53)
            over (order by sales.date.week_seq), 2) as sunday
having sales.date.week_seq in wk2001
order by sales.date.week_seq;
-- 53 rows, sunday ALL NULL.  Generated SQL filters `D_WEEK_SEQ in wk2001` in the base
-- scans (cheerful/cooperative), so the lead(...,53) window only sees the 53 weeks of 2001.
```

Replacing the projection with `lead(sum(...),53) ... as nxt` (no division) → 53 non-NULL. Same
`having`, same membership set — only the mixed expression flips it.

## Correct behavior / proof a good plan exists

The window must be computed over ALL rows, then the `having` applied to its output. The rowset form
does exactly this and is correct:

```trilogy
rowset windowed <- where sales.channel in ('WEB','CATALOG')
  select sales.date.week_seq,
    lead(sum(sales.ext_sales_price ? sales.date.day_name='Sunday'), 53)
      over (order by sales.date.week_seq) as next_yr,
    sum(sales.ext_sales_price ? sales.date.day_name='Sunday') as this_yr;
where windowed.week_seq in wk2001
select windowed.week_seq, round(windowed.this_yr / windowed.next_yr, 2) as sunday;
-- (5270, 3.52), (5271, 1.07), ... correct, non-NULL
```

The `having` form SHOULD produce this same plan.

## Likely fix area

Predicate pushdown: a `having`/membership predicate on a column that a window function ORDERS BY (or
partitions by / whose frame depends on) must NOT be pushed below that window — it changes the window's
input. The pushdown only misfires when a non-windowed sibling term (`sum(x)`) shares the projected
expression with the window (`lead(sum(x), N)`); the bare-window case already keeps the filter above.
Inspect how the projection's non-windowed aggregate causes the having predicate to be attached to the
pre-window aggregate CTE instead of staying above the window node. Likely the same condition-pushdown
machinery as `[[project_filter_cte_dropped_metric_binder_bug]]` / `condition_value_implies`.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-010809/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
# (the all_sales query above) → all NULL; the generated SQL filters wk2001 in the base CTEs
```
