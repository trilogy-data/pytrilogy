# Bug handoff: global (no-`by`) aggregate collapses into the per-group aggregate when referenced in HAVING

**Status:** OPEN — planner bug, reproduced deterministically at sf=0.01.
**Found:** 2026-06-04, diagnosing TPC-H q11 (`ingest` + `enriched` legs fail; SQL legs pass).
**Severity:** medium. Breaks the common "rows whose total exceeds X% of the grand total" pattern. Silent wrong answer (no error).

## Summary

A derived aggregate defined with **no `by` clause** (i.e. a *global* scalar — the total over the whole filtered set) keeps its global grain when referenced in **WHERE**, but **collapses into the per-group aggregate when referenced in HAVING**. The HAVING comparison then becomes self-referential.

This is specific to **no-`by` global aggregates**. A grouped aggregate defined at a *coarser* dimension than the select grain (e.g. `avg(x) by nation` while selecting per-supplier) works correctly in BOTH WHERE and HAVING.

## Minimal repro (TPC-H, sf=0.01)

Setup an engine over any tpch workspace (the `enriched`/`ingest` eval workspaces have `raw/part.preql` with the partsupp grain):

```python
import sys; sys.path.insert(0, 'evals')
from pathlib import Path
from common import scoring
ws = Path('evals/tpch_agent/results/<run>_enriched/workspace')   # any tpch workspace
eng = scoring.make_scoring_engine(ws/'tpch.duckdb', ws, 'tpch')
nrows = lambda b: len(list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall()))

defs = '''import raw.part as part;
auto germany_total_value <- sum(part.supply_cost * part.available_quantity ? part.supplier.nation.name = 'GERMANY');
auto part_total_value    <- sum(part.supply_cost * part.available_quantity ? part.supplier.nation.name = 'GERMANY') by part.id;'''

where_form = defs + '''
where part_total_value > germany_total_value * 0.0001
select part.id, part_total_value -> total_value order by total_value desc;'''

having_form = defs + '''
where part.supplier.nation.name = 'GERMANY'
select part.id, part_total_value -> total_value, --germany_total_value
having part_total_value > germany_total_value * 0.0001 order by total_value desc;'''

print(nrows(where_form))   # 359  <- CORRECT (matches PRAGMA tpch(11))
print(nrows(having_form))  # 374  <- WRONG
```

- `PRAGMA tpch(11)` reference = **359 rows**.
- WHERE form → **359** ✓
- HAVING form → **374** ✗

## Root cause (from the generated SQL)

`generate_sql(having_form)` emits:

```sql
SELECT part_partsupp.ps_partkey AS part_key,
       sum(part_partsupp.ps_supplycost * part_partsupp.ps_availqty) AS total_value
FROM partsupp ... WHERE UPPER(n_name) = 'GERMANY'
GROUP BY 1
HAVING total_value > 0.0001 * sum(part_partsupp.ps_supplycost * part_partsupp.ps_availqty)
```

The `germany_total_value` concept (defined with **no `by`** → the global GERMANY total, ≈ 950,254,254) is compiled as the **per-group** `sum(...)` inside HAVING — the *same* aggregate as `total_value`. So the predicate reduces to `total_value > 0.0001 * total_value` → `total_value > 0` → every GERMANY part with positive value passes (374 of them), instead of comparing to `0.0001 * 950,254,254 = 95,025`.

The planner is not preserving the referenced concept's (global) grain in HAVING context; it folds the no-`by` aggregate down to the group grain.

## Scope / what is NOT broken

Verified on the same engine:

- **Global aggregate in WHERE** → correct (computed as a global scalar, cross-joined/broadcast).
- **Grouped-vs-grouped in HAVING** (per-supplier total vs `avg(...) by nation`, selecting per supplier) → **correct** (3 rows, matches a hand-written SQL CTE). So a coarser-grain grouped aggregate in HAVING is fine; only the no-`by` *global* case collapses.

This means the fix should target specifically: **resolving a no-`by` (grain `()`) aggregate referenced in HAVING** — it must retain global grain and be computed as a scalar (subquery / cross join / window over the full set), exactly as it already is in WHERE. The WHERE path already does this correctly, so the HAVING path likely diverges in how it assigns the referenced concept's grain before emitting the HAVING predicate.

## Where to look

- The HAVING-clause resolution / grain-assignment in the query planner (the stage that decides which aggregate node a referenced concept binds to). Compare the WHERE path (correct) vs HAVING path for a concept whose grain is `()`.
- Related prior context: [[feedback_where_clauses_dont_cross_filter]] (WHERE vs inline-aggregate scope) and [[project_membership_in_having_unsupported]] (HAVING-clause concept handling has known gaps).

## Workaround (already used by the canonical answer)

Compare against a global total in **WHERE**, not HAVING — `tests/modeling/tpc_h/query11.preql` does exactly this and yields 359. Agent-guidance/example steering toward the WHERE form is a viable interim mitigation (a separate decision; not applied here).

## Eval impact

- TPC-H **q11** `ingest` (374 vs 359) and `enriched` (374, then masked to 100 by a self-imposed `limit 100`). SQL legs pass because raw SQL writes the grand total as a correlated/global subquery.
- General pattern: any "total exceeds X% of the grand total" question routed through HAVING.
