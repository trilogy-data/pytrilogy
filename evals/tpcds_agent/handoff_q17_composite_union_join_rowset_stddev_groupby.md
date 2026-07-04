# HANDOFF — q17: composite union-join-to-rowset + two-pass aggregate emits an ungrouped aggregate CTE

**Status:** OPEN, ready to implement. Root-caused, minimal repro + trigger matrix confirmed.
**Full diagnosis:** `evals/tpcds_agent/bug_q17_composite_union_join_rowset_stddev_no_groupby.md`
**Classification:** real framework codegen/planning bug (loud — DuckDB binder error). High confidence, single locus.

## Symptom
A `stddev`/`variance` on the anchor of a **composite (≥2-key) `union join` to a rowset/CTE**
generates an aggregate CTE with no GROUP BY:
```sql
young as (
  SELECT stddev_samp("kaput"."ss_quantity") as "sd",     -- aggregate
         "kaput"."ss_item_id"     as "ss_item_id",         -- bare passthrough key
         "kaput"."ss_ticket_number" as "ss_ticket_number"
  FROM "kaput")                                            -- NO GROUP BY → invalid
```
→ `(_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY clause`.

## Minimal repro
```python
import sys; sys.path.insert(0,'evals'); from common import scoring; from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260704-140355/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall()
```
```
import raw.store_sales as ss;
import raw.store_returns as sr;
with sr_filtered as
where sr.return_date.year in (2001, 2002)
select sr.ticket_number, sr.item.id as sr_item_id, sr.return_quantity ;
where ss.date.year = 2001
select
    ss.store.state as st,
    stddev(ss.quantity) as sd,
    count(sr_filtered.return_quantity) as c
    union join ss.ticket_number = sr_filtered.ticket_number
    union join ss.item.id = sr_filtered.sr_item_id
limit 100;
```

## Trigger matrix (must stay green after the fix)
| join target | keys | anchor agg | expected |
|---|---|---|---|
| rowset | 1 | stddev | PASS |
| rowset | 2 / 3 | stddev / variance | **must PASS after fix (currently FAIL)** |
| rowset | 2 | avg / sum / count | PASS |
| RAW model (no rowset) | 2 / 3 | stddev | PASS |

Both conditions are jointly required to trigger: (a) target is a **rowset/CTE**, and (b) a
**two-pass aggregate** on the anchor over a **composite** union join. Only two-pass aggregates
(`stddev`/`variance`) get split into a separate joined-back CTE; `avg`/`sum`/`count` stay in the
final grouped SELECT, which is why they're unaffected.

## Root cause
The two-pass aggregate is isolated into its own CTE so it can be FULL-joined back to the union
body on the composite keys `ss.item.id` + `ss.ticket_number` (carried as passthrough outputs).
That CTE's QueryDatasource is built with `group_required=False`:
- `trilogy/core/query_processor.py:459` maps `group_required` → `group_to_grain` (→ `False`).
- `trilogy/dialect/base.py:2091` `render_cte_group_by` short-circuits
  `if not cte.group_to_grain: return None` **before** the `_has_local_aggregate` safety net
  (`base.py:1942`, used at `base.py:2096`) can force a GROUP BY.

The single-key path builds the same CTE with `group_to_grain=True` and
`group_concepts=[ss.store.state, ss.ticket_number]` — proving the correct shape is
"grouped at its own grain = the carried join keys / select grain."

## Fix direction (choose one; prefer the first)
1. **Fix at the source (preferred):** the isolated two-pass-aggregate QueryDatasource for a
   composite union-join-to-rowset should be built with `group_required=True`, grouped at its
   carried-key/select grain — matching the single-key branch. Find where this aggregate node /
   QueryDatasource is constructed (aggregate-node construction feeding `query_processor.py:459`;
   start from the composite-union-join-to-rowset planning branch) and set `group_required=True`
   with the correct `group_concepts` (the carried passthrough join keys + any non-aggregate
   select outputs).
2. **Fix at the renderer (safety net, if #1 is risky):** in `render_cte_group_by`
   (`base.py:2091`), do not bail on `group_to_grain=False` when `_has_local_aggregate(cte)` is
   true — fall through to the aggregate path and group by the CTE's non-aggregate output columns.
   This is the same guarantee `_has_local_aggregate` already provides at `base.py:2096`, just
   moved ahead of the short-circuit. Lower-risk but treats the symptom; make sure it doesn't
   force a spurious GROUP BY onto genuinely grainless single-row aggregate CTEs.

Recommendation: attempt #1 first (the flag is wrong, not just the render); keep #2 as the
defensive backstop if the planning branch is hard to isolate cleanly.

## Test to add
Add a DuckDB codegen+execute test (alongside the existing union-join / dialect tests, e.g.
`tests/engine/test_duckdb*.py` or the union-join matrix): the minimal repro above must
`generate_sql` + execute without a binder error and return grouped rows. Include a 3-key variant
and a `variance` variant. Assert the single-key + avg/sum/count + raw-model cases still pass
(guard against a fix that over-forces GROUP BY).

## Acceptance criteria
- Minimal repro (2-key + 3-key, `stddev` + `variance`) generates valid SQL and executes.
- Every PASS row in the trigger matrix still passes.
- Canonical `tests/modeling/tpc_ds_duckdb/query17.preql` still builds + returns 23 rows.
- `ruff check . --fix && mypy trilogy && black .` clean.

## Do NOT
- Do not disable the two-pass-aggregate CTE isolation (it's needed for the FULL-join-back).
- Do not blanket-force GROUP BY on all `group_to_grain=False` CTEs — only when the CTE has a
  local aggregate; grainless single-row scalar CTEs must stay ungrouped.
