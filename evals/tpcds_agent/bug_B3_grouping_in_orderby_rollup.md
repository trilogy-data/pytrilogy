# Bug B3: `grouping()` in ORDER BY emitted into a groupless CTE (rollup + HAVING-vs-scalar)

**Status:** OPEN (found 2026-06-06). Part of the documented rollup hard-cluster
(see EVAL_LOOP_INSTRUCTIONS "Rollup family ... is the hard cluster").
**Severity:** medium — generated SQL is rejected by DuckDB. Top token outlier ingest q14.
**Area:** `trilogy/dialect/base.py` SQL render — `grouping()` ordering is placed in a downstream
join/filter CTE that has no `GROUP BY`/`ROLLUP`. (Secondary: a `:channel` bind-param leak for a
constant projection, visible in the same SQL.)

## Symptom

```
(_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups
LINE 441:     grouping("tearful"."ss_item_brand_id") desc,
```

The error only surfaces on EXECUTION (DuckDB binding the rendered SQL), not at
`compile_statement`. Use the scoring-engine path (`generate_sql` + `execute_raw_sql`) to repro.

## Deterministic reproduction

Repro query: **`evals/tpcds_agent/bug_B3_repro_query14.preql`** (preserved verbatim from the
crashing agent attempt). It needs a model with the three sales channels as **separate fact
datasources merged on `item`** plus a `by rollup` measure whose HAVING compares it to a scalar
aggregate from a different scope — the enriched single-fact `all_sales` model keeps the rollup
in one scope and does NOT reproduce it. The ingest-leg workspace model does.

```python
import sys; sys.path.insert(0, "evals")
from pathlib import Path
from common import scoring
ws = Path("evals/tpcds_agent/results/20260606-032822_ingest/workspace")   # any 3-fact ingest model
eng = scoring.make_scoring_engine(ws / "tpcds.duckdb", ws, "tpcds")
body = Path("evals/tpcds_agent/bug_B3_repro_query14.preql").read_text()
eng.execute_raw_sql(eng.generate_sql(body)[-1]).fetchall()   # -> BinderException
```
(If that workspace has been cleaned by a rebaseline, re-run the ingest leg for q14, or point at
any auto-ingested 3-channel model.)

The query shape (TPC-DS q14): qualifying (brand,class,category) combos present in all 3 channels
→ per-channel `sum(...) by rollup brand, class, category`, kept where the rollup total exceeds
the overall cross-channel average, ordered by `grouping(brand) desc, grouping(class) desc, ...`.

## Root cause (from the generated SQL)

The final statement is a plain join+filter SELECT, with **no GROUP BY**, that carries the
`grouping()` ORDER BY:

```sql
SELECT
    :channel as "channel",                      -- (secondary bug: constant leaked as bind param)
    "tearful"."ss_item_brand_id", ...,
    "tearful"."total_sales",
    coalesce("tearful"."num_sales", 0) as "num_sales"
FROM "trite" INNER JOIN "tearful" on ...        -- <- ordinary join, no GROUP BY / ROLLUP
WHERE "tearful"."total_sales" > (...) / nullif(..., 0)
ORDER BY
    grouping("tearful"."ss_item_brand_id") desc, -- <- grouping() with no groups -> BinderException
    grouping("tearful"."ss_item_class_id") desc,
    grouping("tearful"."ss_item_category_id") desc,
    ...
```

`grouping(col)` is only meaningful inside the scope that performs the ROLLUP (the inner
`tearful` CTE that groups by brand/class/category). When the HAVING comparison to the scalar
`overall_stats` forces a wrapper join+filter CTE, the `grouping()` ORDER-BY terms are emitted in
that wrapper instead of being computed in the rollup CTE (and surfaced as a plain column).

## Fix direction

- Compute the `grouping()` expressions in the CTE that owns the GROUP BY/ROLLUP and project them
  as ordinary columns; downstream ORDER BY should reference those columns, not re-emit
  `grouping()` in a groupless scope.
- Secondary: the `'store' as channel` constant rendered as `:channel` bind param (`Parser Error:
  syntax error at or near ":"` on some paths) — a constant projection should be inlined, not
  parameterized, in this CTE shape.

## Provenance

TPC-DS agent eval, ingest q14 (cross-channel brand/class/category rollup vs overall average).
Burned 41 iters / 2.4M tokens; the agent abandoned the correct single-rollup shape after this
crash and split into three per-channel SELECTs (wrong shape). Related rollup-cluster notes:
EVAL_LOOP_INSTRUCTIONS "Open / harder than a question fix"; inventory
`token_burn_inventory_20260606.md`.
</content>
