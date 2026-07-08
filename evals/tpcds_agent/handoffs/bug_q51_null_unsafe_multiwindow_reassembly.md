# BUG q51 — multi-window reassembly joins on a null-unsafe `=` over a nullable measure, dropping full-outer rows

> **RESOLVED 2026-07-08.** The concurrent null-safe reassembly work fixed this: the generated SQL
> for the q51 agent query now uses `is not distinct from` (0 null-unsafe `=` joins on
> `running_total`), and the 28 web-exclusive full-outer rows are KEPT (verified against run
> `20260708-130405`). q51 still *scores* fail, but for an UNRELATED reason — after the `.id`/`.sk`
> naming swap it reports the business `item.id` (`'AAAAA…'`) where the reference reports the
> surrogate `i_item_sk` (`5`). That is a C1 grain issue (q51 is one of the rare queries that
> reports the surrogate), not this reassembly bug. Original report retained below for history.

**Type:** framework bug (silent wrong result) — FIXED. Enriched q51 fails where raw SQL passes; the
agent's query is CORRECT. Run: `evals/tpcds_agent/results/20260708-030808_enriched`.

## Symptom

q51 full-outers per-item/date cumulative running-max totals from web and store, keeping rows on
either side (web-only, store-only, both). The agent correctly wrote `union join` + `coalesce(...)`
(a full outer of the two per-channel cumulative rowsets), and the generated SQL DOES contain the
intended `FULL JOIN` producing all combined rows. But the final result silently **drops every
web-exclusive `(item, date)` row** → 100 rows, wrong set (e.g. reference row
`(5, 2000-01-25, 193.18, None, 193.18, 93.88)` is missing). Canonical `query51.preql` == reference
(100/100).

## Root cause (generated-SQL smoking gun)

Trilogy computes the two running-max windows in SEPARATE CTEs (`divergent`/`busy` =
web_running_max, `scrawny` = store_running_max) and reassembles the final projection with:

```sql
... INNER JOIN "scrawny"
      ON "busy"."item"="scrawny"."item"
     AND "busy"."date"="scrawny"."date"
     AND "busy"."combined_store_running_total" = "scrawny"."combined_store_running_total"
```

That last predicate is a **null-unsafe `=` equality on a nullable MEASURE column**
(`combined_store_running_total`). On a web-only `(item,date)`, `store_running_total` is NULL, so
`NULL = NULL` → UNKNOWN → the row is dropped by the INNER JOIN. Evidence: in the combined output
`store_running_total` is non-null on 100000/100000 rows but `web_running_total` on only 5352
(exactly the both-channel rows) — the web-exclusive rows vanished.

Two things are wrong:
1. The reassembly of independently-windowed measures back onto the shared grain should join on the
   GRAIN KEYS only (`item, date`) — NOT additionally on a measure value.
2. If a value column is used in a reassembly/self-join equality at all, it must be null-safe
   (`IS NOT DISTINCT FROM`), never bare `=`, since windowed measures over a full-outer grain are
   nullable on the non-matching side.

## Where to look

The defect is in how the planner reassembles multiple window/running-aggregate concepts computed
in sibling CTEs back onto one grain (the node that joins `busy` and `scrawny`). Find where it
builds that ON clause and (a) restrict it to grain keys, or (b) emit null-safe equality for any
value-column predicate. Related prior art: the coalescing-key null-safety work
(`[[project_q97_coalescing_presence_and_derived_key_recursion_fixed]]`) and the presence-probe
mechanism already use `is not distinct from` for coalescing keys — the same null-safety needs to
extend to multi-window reassembly joins.

## Minimal repro
```
cd evals && ../.venv/Scripts/python.exe -c "
import sys; sys.path.insert(0,'.')
from pathlib import Path; from common import scoring
ws=Path('tpcds_agent/results/20260708-030808_enriched/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
body=open(ws/'query51.preql').read()
sql=eng.generate_sql(body)[-1]        # inspect: INNER JOIN scrawny ON ... AND busy.store_running_total = scrawny.store_running_total
rows=list(eng.execute_raw_sql(sql).fetchall())
print(len(rows), 'rows; web-exclusive rows dropped')  # missing (5,'2000-01-25',193.18,None,193.18,93.88)
"
```
Canonical `tests/modeling/tpc_ds_duckdb/query51.preql` produces the correct 100 rows — diff its
generated reassembly join against the agent's to see the grain-key-only vs +measure difference.

## Deliverable
Fix the reassembly ON clause (grain-keys-only, or null-safe value equality), guard with a test
that a two-window full-outer keeps single-side rows, and confirm q51 (agent-style query) matches
the reference. This is likely a small, high-value fix (it also de-risks any query that windows
two full-outered channels independently).
