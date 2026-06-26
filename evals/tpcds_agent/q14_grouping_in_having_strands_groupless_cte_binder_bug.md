# Bug: `grouping()` in HAVING strands in a groupless filter CTE → BinderException "GROUPING function is not supported here" (q14)

**Status:** FIXED 2026-06-25 — `_promote_having_grouping_to_outputs` pre-pass in
`trilogy/parsing/v2/select_finalize.py` promotes each HAVING `grouping()`/`grouping_id()` to a
hidden SELECT output (materializing it inside the ROLLUP CTE) and substitutes the HAVING ref to it.
Test: `tests/engine/test_duckdb.py::test_grouping_sum_in_having_with_membership_filter_colocates`.
(Standalone `grouping() in (literal-list)` without a membership filter is a SEPARATE pre-existing
bug — membership `in` in HAVING routes to pre-agg WHERE where grouping is invalid — still open.)
Minimal trigger needs the combination below (grouping-in-HAVING alone is fine).
**Surfaced by:** TPC-DS q14 enriched eval (run `20260625-183939`, trace line ~7949).
**Severity:** HIGH — generates invalid SQL; blocks the "restrict rollup to specific grouping levels"
idiom.

## Symptom

`grouping()` used in **HAVING** (to keep only certain rollup levels) **together with a
rowset-membership filter** that forces an extra CTE layer:

```trilogy
... by rollup all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id ...
where bcc_key in qualifying_bcc.bcc_key
having total_sales > overall_avg_sale
  and (grouping(all_sales.channel) + grouping(all_sales.item.brand_id)
       + grouping(all_sales.item.class_id) + grouping(all_sales.item.category_id)) in (0, 1, 4)
```

→ `(_duckdb.BinderException) Binder Error: GROUPING function is not supported here`.

## Trigger isolation

`grouping()` in HAVING is **fine on its own** (all of these build):
- `having grouping(s.channel) = 0` over a 1-key rollup ✓
- `having (grouping(s.channel) + grouping(s.item.category)) in (0,1)` over a 2-key rollup ✓
- `having grouping(s.channel) in (0)` ✓

It breaks only in the **full q14 combination**: inline `by rollup` (4 keys) + a **rowset-membership
filter** (`bcc_key in qualifying_bcc.bcc_key`) + the grouping-sum-in-HAVING + the cross-grain
`total_sales > overall_avg_sale`. The membership filter forces a downstream filter/join CTE, and
the `grouping()` strands in that CTE — which has no `GROUP BY ROLLUP` — so DuckDB rejects the bare
`GROUPING(...)`.

## Root cause (family)

Same family as `project_b3_grouping_in_orderby_rollup` (B3): *"grouping() stranded in a downstream
groupless join/filter CTE renders as `grouping(col)` with no GROUP BY (DuckDB: GROUPING … without
groups)."* The `_fix_projection_grouping_mode` fix aligns grouping mode for SELECT/ORDER-BY
wrappers, but a `grouping()` in **HAVING**, when a membership filter (`in <rowset>`) pushes the
aggregate behind an extra CTE, is not co-located with its rollup GROUP BY.

## Suggested fix

Extend the grouping-mode co-location so a HAVING `grouping()` stays in the same CTE as its
ROLLUP/CUBE aggregate even when a downstream membership-filter CTE is introduced — or, if that
can't be guaranteed, raise a clean author-time error (as the no-rollup case now does) instead of
emitting SQL DuckDB rejects. The agent's intent (restrict rollup output to specific grouping
levels) is legitimate and matches the reference's rollup-level filtering.

## Reproducing query

Full query: `evals/tpcds_agent/results/20260625-183939/workspace` trace; saved minimal form has the
4-key rollup + `bcc_key in qualifying_bcc.bcc_key` + the grouping-sum HAVING. Harness:

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-191717/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('repro.preql').read())[-1]
eng.execute_raw_sql(sql)   # BinderException: GROUPING function is not supported here
```
