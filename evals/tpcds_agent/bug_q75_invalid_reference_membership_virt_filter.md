# Bug: INVALID_REFERENCE_BUG in HAVING grain-key membership when a window-only grain key isn't projected (q75)

**Status:** FIXED 2026-06-28 (found TPC-DS agent eval query 75, enriched leg). Fix = option (b):
`_rewrite_having_finer_dims_to_membership` (select_finalize.py) now materializes any unprojected
select-grain key as a hidden output once a membership is built, so the membership left tuple resolves
in the final CTE. Regression: `test_non_benchmark_queries.py::test_q75_having_on_window_only_grain_key_membership`.
**Severity:** high — valid-looking query crashes at SQL render with the internal
`INVALID_REFERENCE_BUG` sentinel ("this should never occur"); agent burned ~2.5M tokens thrashing.
**Area:** `trilogy/parsing/v2/select_finalize.py` — `_build_grain_key_membership` (~line 1173-1227)
and its caller `_rewrite_having_finer_dims_to_membership` (~line 1230), interacting with the
final-CTE renderer in `trilogy/dialect/base.py` (`BASE_INVALID = "INVALID_REFERENCE_BUG"`, line 253).

## Symptom

`trilogy run query75.preql` → `ValueError: Invalid reference string found in query: ... this
should never occur.` The rendered final `SELECT ... FROM "abhorrent" WHERE (...)` contains a
row-wise membership whose **left tuple elements are the sentinel**:

```sql
WHERE
  (INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG, INVALID_REFERENCE_BUG)
  in (select sweltering."_virt_filter_brand_id_2488385400655448",
             sweltering."_virt_filter_category_id_...",
             sweltering."_virt_filter_class_id_...",
             sweltering."_virt_filter_manufacturer_id_...",
             sweltering."_virt_filter_yr_..."
      from sweltering where ... is not null ...)
  and "abhorrent"."prev_qty" is not null and "abhorrent"."prev_qty" > 0
  and ( "abhorrent"."curr_qty" / "abhorrent"."prev_qty" ) < 0.9
```

The RHS existence subselect (`sweltering`, the `_virt_filter_*` set) is emitted correctly; only the
**left-hand `ROW_TUPLE` references fail to resolve** in the outer/final CTE.

## Trigger

The agent's q75 form filters in HAVING on a rowset grain key (`yr`) that is **not projected** in the
outer SELECT and is referenced **only** by the window functions (lag partition/order-by):

```trilogy
import all_sales as all_sales;

with agg as
where all_sales.item.category = 'Books' and all_sales.date.year in (2001, 2002)
select all_sales.date.year as yr, all_sales.item.brand_id as brand_id, ... sum(...) as tq, ...;

select
    agg.brand_id as brand_id, ...,                       -- NOTE: agg.yr is NOT projected
    lag(agg.tq,1) over (partition by agg.brand_id, ... order by agg.yr asc) as prev_qty,
    ...
having
    agg.yr = 2002                                         -- finer-grain filter → semijoin rewrite
    and prev_qty is not null and prev_qty > 0 and (agg.tq / prev_qty) < 0.9;
```

## Minimal repro (smallest snippet still emitting the sentinel)

Model dir: `tests/modeling/tpc_ds_duckdb` (has `all_sales.preql`; no data needed — fails at
`generate_sql`).

```trilogy
import all_sales as all_sales;

with agg as
where all_sales.item.category = 'Books' and all_sales.date.year in (2001, 2002)
select all_sales.date.year as yr, all_sales.item.brand_id as brand_id, sum(all_sales.quantity) as tq;

select
    agg.brand_id as brand_id,
    lag(agg.tq, 1) over (partition by agg.brand_id order by agg.yr asc) as prev_qty,
having
    agg.yr = 2002;
```

Driver:
```python
from trilogy import Environment, Dialects
from pathlib import Path
env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
exec_ = Dialects.DUCK_DB.default_executor(environment=env)
sql = exec_.generate_sql(open("repro.preql").read())   # last SELECT contains INVALID_REFERENCE_BUG
```

Renders `(."agg_brand_id", INVALID_REFERENCE_BUG) in (select vacuous."_virt_filter_brand_id_...",
vacuous."_virt_filter_yr_..." from vacuous ...)` — here `brand_id` (a projected output) resolves and
only `yr` (the window-only grain key) is the sentinel, isolating the failure.

## Root-cause hypothesis (confirmed)

1. HAVING `agg.yr = 2002` filters on `yr`, which is outside the select **projection** but is in the
   select **grain** (the window `order by agg.yr` / partition pulls it into `select.grain.components`).
2. `_rewrite_having_finer_dims_to_membership` (select_finalize.py:1230) detects the finer-dim filter
   and calls `_build_grain_key_membership`, which builds the left tuple from
   `select.grain.components` (line 1225: `left = row_tuple_function(list(key_refs))`) — i.e. **every
   grain key**, including `yr`.
3. The validator at line 1453 (`allowed_for_having = allowed_addresses | set(select.grain.components)`)
   therefore **passes** `yr` through (it is a grain key), so no clean error is raised.
4. But the outer/final CTE materializes only the **projected output columns** (brand_id, prev_qty,
   curr_qty, ...), **not** the bare grain key `yr`. When the renderer resolves the membership
   left-side `ROW_TUPLE` element for `yr` against the final CTE source map, it is absent →
   `INVALID_REFERENCE_BUG` (dialect/base.py:253).

**Verification:** adding `agg.yr as yr` to the outer SELECT projection makes the bug vanish (INVALID
count 0). So the trigger is precisely "membership left tuple references a grain key that is in the
grain but not a materialized output column of the final CTE." The canonical hand-authored
`tests/modeling/tpc_ds_duckdb/query75.preql` (scoped-join + per-year rowsets, no window-only grain
key in HAVING) generates clean SQL (0 sentinels).

Likely fix direction (not attempted): either (a) restrict the membership left tuple to grain keys
that are actually projected outputs and anchor the unprojected keys differently, or (b) force the
unprojected grain key (`yr`) to be materialized (hidden output) in the outer CTE before the
semijoin — mirroring the `_expose_downstream_referenced_columns` un-hide pass used for q23.

## Resemblance to existing cases

- **Same query/family, previously fixed:** `project_having_dim_window_validator_bypass_bug.md`
  (q75, FIXED 2026-06-23) — `_row_grain_arguments` walker made the HAVING/ORDER-BY validator tolerate
  window partition/order args. That fix lets this query **past the validator**; the present bug is the
  next layer down: the membership-rewrite then emits an unresolvable left-tuple over the very
  grain key the window introduced. Effectively a sibling/follow-on of that fix.
- `bug_invalid_reference_codegen_having_membership.md` (FIXED 2026-06-19/06-22) — same sentinel and
  the same HAVING-membership + window shape, but that was about the **existence RHS** set not being
  sourced (dangling CTE). Here the RHS set is sourced fine; the **left** row-tuple is unresolved.
- Composite row-wise membership machinery (`ROW_TUPLE` / `row_tuple_function`, MEMORY q14) is the
  rendering path the left tuple flows through, but is not itself at fault.
- `bug_window_function_in_having.md` / `bug_membership_in_having_*` — same broad HAVING-on-window area.
```
