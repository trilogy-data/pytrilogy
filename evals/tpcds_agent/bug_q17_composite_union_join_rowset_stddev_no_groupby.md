# q17 token sink — composite union-join to a rowset + two-pass aggregate (stddev/variance) emits an ungrouped aggregate CTE (invalid SQL)

Run: `evals/tpcds_agent/results/20260704-140355` (q17 enriched leg, 671,283 tokens, result differs from reference).

## Classification
REAL FRAMEWORK BUG (codegen / query-planning). DuckDB rejects the generated SQL with a
GROUP BY binder error. It blocked the agent's semantically-correct approach and pushed it
onto a wrong-answer workaround.

## Symptom
```
(_duckdb.BinderException) Binder Error: column "ss_item_id" must appear in the GROUP BY
clause or must be part of an aggregate function.
```
The generated plan isolates the anchor's `stddev` into its own CTE that must be re-joined to
the rowset on the composite union-join keys. That CTE is emitted as:
```sql
young as (
  SELECT
    stddev_samp("kaput"."ss_quantity") as "sd",   -- aggregate
    "kaput"."ss_item_id"    as "ss_item_id",       -- bare passthrough join key
    "kaput"."ss_ticket_number" as "ss_ticket_number"
  FROM "kaput")                                    -- NO GROUP BY  <-- invalid
```

## Minimal repro (harness = run's own workspace)
```python
import sys; sys.path.insert(0,'evals'); from common import scoring; from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260704-140355/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall()
```
Smallest failing body (2-key union join to a rowset + `stddev`):
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

## Trigger matrix (toggle one ingredient)
| join target | keys | anchor agg | result |
|---|---|---|---|
| rowset | 1 (item.id) | stddev | PASS |
| rowset | 2 (item.id+ticket) | stddev | **FAIL (binder)** |
| rowset | 3 | stddev | **FAIL** |
| rowset | 2 | variance | **FAIL** |
| rowset | 2 | avg | PASS |
| rowset | 2 | sum | PASS |
| rowset | 2 | count | PASS |
| RAW model (no rowset) | 2 or 3 | stddev | PASS |

Both conditions are required: (a) the join target is a **rowset/CTE** (not a raw model), and
(b) a **two-pass aggregate** (`stddev`/`variance`) sits on the anchor over a **composite**
(≥2-key) union join. Single-key, raw-model, or simple aggregates (avg/sum/count) all pass.

## Root cause (planning flag → renderer emit)
Instrumenting `render_cte_group_by` shows the isolated aggregate CTE:
```
CTE young: group_to_grain=False  group_concepts=[]
  outputs: local.sd (AGGREGATE, computed locally, src=[]),
           ss.store.state, ss.ticket_number, ss.item.id  (ROOT passthroughs)
```
- The stddev node is isolated so it can be FULL-joined back to the union body on the composite
  keys `ss.item.id` + `ss.ticket_number`, so those keys are carried as outputs.
- Its QueryDatasource is built with `group_required=False`, so the CTE gets
  `group_to_grain=False` (`trilogy/core/query_processor.py:459` maps `group_required` →
  `group_to_grain`).
- `BaseDialect.render_cte_group_by` (`trilogy/dialect/base.py:2091`) short-circuits
  `if not cte.group_to_grain: return None` **before** the `_has_local_aggregate` safety check
  (base.py:1942 / used at 2096) can force a GROUP BY. So it emits `stddev_samp(...)` next to
  the bare passthrough key columns with no GROUP BY → invalid SQL.

Contrast: the **single-key** path builds the same aggregate CTE with `group_to_grain=True` and
`group_concepts=[ss.store.state, ss.ticket_number]` (proper GROUP BY); the **raw-model** path
doesn't isolate the aggregate at all. So the defect is specifically the composite-key
rowset-union planning branch producing an aggregate QueryDatasource with `group_required=False`
(should be True, grouped at its own grain = the carried join keys / select grain).

Emit site: `trilogy/dialect/base.py:2091` (bails on `group_to_grain`).
Origin: the isolated two-pass-aggregate QueryDatasource for a composite union-join-to-rowset is
built with `group_required=False` (aggregate node construction feeding
`query_processor.py:459`). `stddev`/`variance` are the trigger because only two-pass aggregates
get split into a separate joined-back CTE here; `avg`/`sum`/`count` stay in the final grouped
SELECT.

## Why it drove 671k tokens
The agent's semantically-correct plan (pre-filter returns/catalog to their year windows in
rowsets, then union-join on the shared composite grain, with `stddev` for the required CoV) hits
this binder error every time (attempt 2 onward). To dodge it, the agent abandoned filtered
rowsets and fell back to inline `?` filters + bare union joins with no intersection predicate.
That runs but returns the WRONG set: union join keeps all 2001 store-sales rows, so it returns
100 unmatched store-state rows with `cs_quantity_count = 0`, versus the reference's 23-row
3-way intersection. The agent never reached the intended `catalog_store_returns` enriched model.

## Verified references
- Canonical `tests/modeling/tpc_ds_duckdb/query17.preql` (enriched `catalog_store_returns`
  model) builds and executes cleanly on the current engine (23 rows). The bug is confined to the
  raw composite-union-join-to-rowset + stddev path the agent chose.

Scratchpad repros:
`.../scratchpad/min17b.py` (trigger matrix), `.../scratchpad/inspect17.py` (CTE flag dump).
READ-ONLY diagnosis; nothing fixed.
