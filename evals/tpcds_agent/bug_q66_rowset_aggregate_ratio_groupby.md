# Bug q66 — rowset aggregate ÷ passthrough dimension → aggregate emitted in GROUP BY (Binder Error)

**Status:** FIXED 2026-07-03. Added a `Derivation.ROWSET` arm to `has_local_aggregate`
(execute.py:455) recursing into `c.lineage.content` — symmetric with `check_is_not_in_group`
(line 505–507). GROUP BY now excludes the `agg/dim` ratio; minimal repro executes correctly and
`test_sixty_six` passes. Guard: `tests/engine/test_duckdb_rowset.py::test_rowset_aggregate_over_dimension_ratio_not_grouped`.
The collapse-guard alternative was not needed.
**Class:** SILENT-turned-LOUD codegen bug. Surfaced (regressed loud) by the latest join
refactor (`956e7303b hacky_joins` / `dcc62ed78 union_checkpoint`); q66 enriched churn jumped
301K → 1.35M tokens in run `20260703-134501` as the agent thrashed against the DuckDB error.

## Symptom

A two-statement rowset (CTE) query whose **outer select divides a rowset's aggregate output
by a passthrough dimension** generates SQL that places the ratio expression in `GROUP BY`.
Because the ratio contains a `sum(...)`, DuckDB rejects it:

```
(_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
LINE 55:     sum(CASE ...
```

The generated tail (abbreviated) — note `monthly_sales_per_sqft` (the `sum()/nullif(dim)`
column, SELECT position 11) appears in `GROUP BY`:

```sql
SELECT ...,
  sum(CASE WHEN ... END) as "agg_monthly_sales",                    -- pos 10 (correctly NOT grouped)
  sum(CASE WHEN ... END) / nullif("w_warehouse_sq_ft",0) as "monthly_sales_per_sqft", -- pos 11
  sum(CASE WHEN ... END) as "agg_monthly_net"                       -- pos 12 (correctly NOT grouped)
FROM "cheerful" LEFT OUTER JOIN ...
GROUP BY 1,2,3,4,5,6,8,9,11      -- <-- position 11 is the aggregate ratio. BUG.
```

This is a legitimate q66 output: TPC-DS q66 reports `jan_sales/w_warehouse_sq_ft as
jan_sales_per_sq_foot` (per-sqft ratios), so the agent's shape is correct SQL intent.

## Minimal repro

Engine harness against any workspace DB (used `results/20260703-134501/workspace`):

```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path; from common import scoring
ws=Path('evals/tpcds_agent/results/20260703-134501/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
sql=eng.generate_sql(BODY)[-1]; eng.execute_raw_sql(sql).fetchall()
```

FAILING `BODY` (smallest form):

```
import raw.all_sales as sales;
with agg as
where sales.date.year = 2001 and sales.warehouse.id is not null
select
    sales.warehouse.name as warehouse_name,
    sales.warehouse.square_feet as warehouse_square_feet,
    sum(sales.quantity * sales.sales_price) as monthly_sales
;
select
    agg.warehouse_name,
    agg.warehouse_square_feet,
    agg.monthly_sales,
    agg.monthly_sales / nullif(agg.warehouse_square_feet, 0) as sales_per_sqft
;
```

## Trigger matrix

| # | Shape | Result |
|---|---|---|
| 1 | Single select (no rowset), `sum(x)` + `sum(x)/dim` in same select | **PASS** |
| 2 | Two-stmt rowset, outer = passthrough columns only (no ratio) | **PASS** |
| 3 | Two-stmt rowset, outer ratio = **agg output ÷ agg output** (`agg.sales/agg.qty`) | **PASS** |
| 4 | Two-stmt rowset, outer ratio = **agg output ÷ passthrough dimension** (`agg.sales/agg.sqft`) | **FAIL** |
| 5 | Two-stmt rowset, outer = passthrough dimension alone | **PASS** |

The bug requires **all** of: (a) the rowset/CTE boundary, (b) a BASIC expression in the outer
select that **mixes** a rowset *aggregate* output with a rowset *dimension* output. Ratio of two
aggregates (row 3) is fine; the dimension operand is what breaks it (see root cause).

## Root cause — `has_local_aggregate` lacks a `Derivation.ROWSET` arm

`trilogy/core/models/execute.py`, `CTE.group_concepts` (line 450). Group membership is decided
by `check_is_not_in_group` (line 500). For the ratio concept (`BASIC`):

1. Line 545 `all(check_is_not_in_group(arg) ...)` over its args `[agg.monthly_sales,
   agg.warehouse_square_feet]`. `check_is_not_in_group` **does** unwrap `Derivation.ROWSET`
   (line 505–507 → recurses to `.content` → aggregate → "not in group"):
   - `agg.monthly_sales` → not-in-group (True)
   - `agg.warehouse_square_feet` (dimension) → in-group (False)
   - `all([True, False])` = **False** → falls through.
2. Line 555 fallback `if has_local_aggregate(c): return True` (exclude from GROUP BY).
   **`has_local_aggregate` (line 455) has NO `Derivation.ROWSET` branch.** For the BASIC
   ratio it recurses into args (line 494–497); each arg is a `Derivation.ROWSET` concept
   (lineage `BuildRowsetItem` wrapping the aggregate), which matches none of its arms
   (AGGREGATE / METRIC+BuildFunction / FILTER / BASIC) → returns **False**.
3. So `has_local_aggregate(ratio)` = False → line 557 returns False → ratio is treated as a
   **group key** and emitted in `GROUP BY`.

Why row 3 (agg÷agg) passes: both args are aggregate rowset outputs, so step 1's `all(...)` is
True and the concept is excluded at line 551 before the buggy fallback is ever reached. The
dimension operand in row 4 is what forces the code into the ROWSET-blind `has_local_aggregate`
path.

**Asymmetry to fix:** `check_is_not_in_group` unwraps `Derivation.ROWSET` (line 505–507) but
its sibling `has_local_aggregate` does not. They must agree.

### Why the join refactor exposed it
Pre-refactor the rowset aggregate stayed a separate physical CTE, so the outer ratio read
`agg.monthly_sales` as a plain parent-sourced column (`source_map` non-empty → line 459
short-circuits) in a non-grouping select — no GROUP BY, no error (result may have been silently
wrong, but no crash). `CollapseSingleParent` (`trilogy/core/optimizations/collapse_single_parent.py`,
BASIC-fold-into-GROUP path, gated by `basic_fold_into_group_is_safe` line 125) now folds the
rowset aggregate CTE into the consumer, producing one grouping SELECT — and the misclassification
bites. `basic_fold_into_group_is_safe` only inspects the child column's **top-level** lineage
(`BuildAggregateWrapper`/`BuildWindowItem`, line 153); a division whose *operand* is an aggregate
rowset output slips through as "safe scalar projection."

## Suggested fix direction (for the implementer — verify, don't take on faith)

Primary candidate: add a `Derivation.ROWSET` arm to `has_local_aggregate` (execute.py:455)
that recurses into `c.lineage.content` (mirroring `check_is_not_in_group` line 505–507), so a
BASIC expression mixing a rowset-aggregate with a dimension is recognized as aggregate-bearing
and excluded from GROUP BY. This is the minimal, symmetric fix and keeps the collapse.

Alternative (defense in depth, likely also wanted): tighten `basic_fold_into_group_is_safe`
(collapse_single_parent.py:125) to reject folding a child column whose lineage *contains* an
aggregate anywhere (there's already a `lineage_contains_aggregate` helper in that file, line 61)
— i.e. don't fold `agg÷dim` into the GROUP parent at all, matching how row 1's single-select
form is handled.

Prefer the `has_local_aggregate` fix as the root cause; consider the collapse guard only if the
symmetric fix proves insufficient. Add a guard test at both the codegen level (assert the ratio
is not in `group_concepts`) and an execute test using the minimal repro above. Re-check q66
enriched churn drops back and confirm rows match `tests/modeling/tpc_ds_duckdb/query66.sql`.

## Repro artifacts
- Scratch repro scripts used: `$TEMP/repro66{,b,c}.py` (transient; the BODY strings above are
  self-contained).
- Failing run: `evals/tpcds_agent/results/20260703-134501/` (agent_log.q66.*, workspace/query66.preql).
