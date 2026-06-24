# BUG: bare-dimension HAVING ref escapes the "not in SELECT projection" validator when a SELECT column wraps an inline window → `INVALID_REFERENCE_BUG` leaks into rendered SQL

## STATUS: FIXED (2026-06-23)
Fixed in `trilogy/core/statements/author.py`: the `alias_source_addresses` property now
collects a SELECT-derived expression's available outer columns via a new
`_row_grain_arguments` walker that descends scalar functions but treats a window as
contributing **nothing** (its partition/order/content live at the pre-window grain and are
grouped away in the outer scope). Aggregate sources are unchanged (a `sum(x)` in HAVING
still matches the `sum(x) as total` output via the shared input `x`). The bare-dimension
HAVING/ORDER BY ref now falls through to the existing actionable `InvalidSyntaxException`
("HAVING references '…', which is not in the SELECT projection"). Regression suite:
`tests/test_having_resolution.py`.

## Severity
High — a `ValueError("Invalid reference string found in query: … INVALID_REFERENCE_BUG …")` is raised at SQL-render time (after the planner runs), instead of the clean, actionable parse-time error the same shape gets without the window. Surfaced as the q75 enriched-leg thrash (~1.88M tokens). The agent saw an internal "this should never occur. Please create an issue" sentinel error, could not act on it, abandoned the `aggregated.yr = 2002` filter entirely, and shipped a query that runs but is **wrong vs reference**.

## Symptom
A SELECT that contains a derived column wrapping an **inline window** (e.g. `agg.total_qty - lag(agg.total_qty,1) over (partition by … order by agg.yr) as qty_diff`) plus a **HAVING reference to a bare dimension that is NOT in the SELECT projection** but *is* mentioned inside that window's `partition by`/`order by` (here `agg.yr`) — renders SQL containing the literal token `INVALID_REFERENCE_BUG` and raises:

```
ValueError: Invalid reference string found in query:
  … WHERE "sweltering"."prev_qty" is not null and INVALID_REFERENCE_BUG = 2002 and …
```

The same query WITHOUT the inline-window SELECT column raises the correct, helpful error instead:

```
InvalidSyntaxException: HAVING references 'aggregated.yr', which is not in the SELECT projection (line N).
To filter output rows, add it to SELECT — prefix with `--` so it stays out of the output rows … Or move it to WHERE …
```

## Minimal repro
Engine built from `evals/tpcds_agent/results/20260623-145720/workspace` (enriched models in `raw/`):

```trilogy
import raw.all_sales as sales;

rowset aggregated <- where sales.item.category = 'Books' and sales.date.year in (2001, 2002)
select
    sales.date.year as yr,
    sales.item.brand_id as brand_id,
    sales.item.class_id as class_id,
    sales.item.category_id as cat_id,
    sales.item.manufacturer_id as mfr_id,
    sum(coalesce(sales.quantity, 0) - coalesce(sales.return_quantity, 0)) as total_qty,
    sum(coalesce(sales.ext_sales_price, 0) - coalesce(sales.return_amount, 0)) as total_amt;

select
    aggregated.brand_id, aggregated.class_id, aggregated.cat_id, aggregated.mfr_id,
    aggregated.total_qty
        - lag(aggregated.total_qty, 1) over (
              partition by aggregated.brand_id, aggregated.class_id, aggregated.cat_id, aggregated.mfr_id
              order by aggregated.yr asc) as qty_diff
having aggregated.yr = 2002;
```

`eng.generate_sql(body)[-1]` raises `ValueError: Invalid reference string found in query: … INVALID_REFERENCE_BUG …`.

Two single-change controls prove the trigger:
- Remove the inline-window SELECT column (e.g. project `aggregated.total_qty as current_qty` instead of the `… - lag(…)` diff) → clean `InvalidSyntaxException` ("…not in the SELECT projection"). The validator fires.
- Keep the inline-window column but add `aggregated.yr` to the SELECT (even hidden: `--aggregated.yr`) → planner/render succeed, no sentinel. The dimension is now genuinely carried through the window node.

## Scope (which forms trigger it)
- Requires (a) an inline window function in a SELECT-derived column, AND (b) a HAVING (or, by the same code path, ORDER BY) reference to a bare dimension that appears ONLY inside that window's `partition by`/`order by` and is otherwise absent from the SELECT outputs.
- Independent of the rowset depth (reproduces with a single aggregation rowset; the agent's two-rowset `deduped → aggregated` structure is not required).
- Independent of TPC-DS / `all_sales`; it is a general validator-coverage gap. The same leak would occur on any model.

## Root-cause hypothesis + suspected file
`SelectStatement.alias_source_addresses` (`trilogy/core/statements/author.py:381-387`) collects **all** `concept_arguments` of every SELECT `ConceptTransform.function`, which includes concepts buried inside a window's `partition by`/`order by` (here `aggregated.yr`). The HAVING-projection validator (`trilogy/parsing/v2/select_finalize.py:874-934`, mirrored in `author.py:341-349`) computes `allowed_addresses = all_in_output | alias_source_addresses` and then green-lights any HAVING `row_argument` whose address is in that set.

So `aggregated.yr` is wrongly treated as "projected" because it is a window-internal argument — the clean "add it to SELECT" guard is skipped. But the window node materializes the lag at brand/class/cat/mfr grain and **groups `aggregated.yr` away**; it is not a resolvable output column at the outer HAVING scope, so the renderer substitutes the `BASE_INVALID = "INVALID_REFERENCE_BUG"` sentinel (`trilogy/dialect/base.py:238`) into the WHERE/HAVING render, and `generate_sql` then raises on the sentinel string.

Window-internal partition/order concepts are NOT genuine outer-projection columns and must not count as "allowed" HAVING/ORDER-BY references.

## Repro harness
```python
import shutil, sys; sys.path.insert(0, 'evals')
from pathlib import Path
from common import scoring
src = Path('evals/tpcds_agent/results/20260623-145720/workspace')
mydb = Path('<SCRATCH>/q75.duckdb'); shutil.copy(src/'tpcds.duckdb', mydb)
eng = scoring.make_scoring_engine(mydb, src, 'tpcds')
sql = eng.generate_sql(BODY)[-1]              # raises ValueError on the sentinel
assert 'INVALID_REFERENCE_BUG' not in sql     # currently fails for the repro BODY
```

## Expected fix
Exclude window-function-internal `partition by` / `order by` concept arguments from `alias_source_addresses` (or, more precisely, only count a SELECT-derived function's arguments as "available outer columns" when they survive the grouping the derived column induces — i.e. not the inner args of a window/aggregate). With that, the bare-dimension HAVING ref `aggregated.yr` falls through to the existing `InvalidSyntaxException` ("HAVING references '…', which is not in the SELECT projection"), giving the agent an actionable error and a concrete fix (`--aggregated.yr` in SELECT, or move to WHERE) instead of an internal sentinel crash.

Note: even the helpful error is suboptimal idiom guidance here — the canonical solution is the two-rowset scoped-join shape (`tests/modeling/tpc_ds_duckdb/query75.preql`: `rowset curr` / `rowset prev` joined on the 4 item keys), not `lag()` over a single year-spanning aggregate. But the framework bug is the validator bypass + sentinel leak; fixing it converts a "report an issue" crash into a clean, recoverable parse error.
```
