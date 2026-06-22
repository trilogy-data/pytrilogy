# Brief: v4 C11 multi-fact aggregate fan-out runaway (q2.1/q2.2)

**Status:** FIXED 2026-06-12. The original root-cause framing below ("co-group
aggregates from disjoint fact tables", fix via a fact-based split guard) was
WRONG. The real cause: aggregate *input grain* derivation ignored inline
expression arguments, so the two per-fact aggregates looked identical and were
never partitioned by their true upstream input grain. No fact-based logic added.

## Actual root cause + fix

`_aggregate_input_grain` (`v4_helper/concept_graph.py`) computed an aggregate's
required input grain by iterating `function.arguments` (top-level) and only
walking args that are a bare `BuildConcept`. For
`sum(case when dow=wd then web_sales.ext_sales_price else 0) by week_seq` the
single top-level arg is an inline `BuildFunction` (the CASE), so it was skipped
entirely — both the web_sales and catalog_sales aggregates collapsed to
`input_grain = {week_seq}` (output grain only), looked identical to
`_partition_standard_aggregates`, and co-bucketed into one group → one MergeNode
joining the raw facts on `date_id` → soft cross-join fan-out.

Fix (one function, mirrors v3 `resolve_function_parent_concepts`): descend into
inline-expression args via their flattened `concept_arguments`, then walk each to
row identity with `_walk_aggregate_grain_inputs` (as the bare-concept path
already did). web agg → `{week_seq, date.id, web_sales.*}`, catalog agg →
`{week_seq, date.id, catalog_sales.*}` → distinct → partitioned into two per-fact
aggregates joined 1:1 on `week_seq`. Exec 70s/103s → 0.24s/0.25s, 53 rows.

GOTCHA (q97 regression caught in sweep): a referenced concept that is itself a
row-shape barrier (ROWSET/AGGREGATE output) has already collapsed to its own
grain and is consumed opaquely. q97's outer grand-total `sum(case ..
pair_presence.store_present ..)` reads rowset outputs; descending pulled the
rowset grain into the input grain, forcing a spurious regroup that deduped the
(customer,item) pairs (counts dropped 540709→48295). Fix: skip args whose
derivation is in `ROW_SHAPE_BARRIER_DERIVATIONS` during the inline descent.
Bare top-level concept args keep their original behavior unchanged.

Tight unit lock added: `TestAggregateInputGrain` in
`tests/core/processing/test_v4_concept_graph.py` — `INLINE_CASE_MULTIFACT_MODEL`
asserts inline-case aggregates over disjoint facts derive DIFFERENT input grains
including the fact row key (would have caught this from the get-go).

q2.1/q2.2 now reclassified `_TPCDS_SIZE` (correct rows, SQL exceeds the
v3-tuned length ceiling — generic v4 verbosity, not this bug).

---
## ORIGINAL (WRONG) DIAGNOSIS BELOW — kept for context

**Owner of diagnosis:** see `local_scripts/v4_audit.md` C11 + memory
`project_v4_c10_baseline_fails_folded.md`.

## Symptom

`tests/modeling/tpc_ds_duckdb/test_queries.py::test_two_one` and `::test_two_two`
(run `query02-one.preql` / `query02-two.preql`, the TPC-DS q2 variants 2.1/2.2 —
NOT q21/q22). Under v4 (`TRILOGY_V4_DISCOVERY=1`) the generated SQL is correct
(rows + size pass) but EXECUTION takes ~70s (2.1) / ~103s (2.2) at sf=1 for 53
output rows. The classifier mis-buckets this as TIMEOUT/OTHER. Reclassified in
`tests/v4_known_failing.py` as `_RUNAWAY`.

## Root cause

The query metric (see `query02-one.preql`):
```
weekday_sales(wd) = sum(case when dow=wd then web_sales.ext_sales_price else 0) by date.week_seq
                  + sum(case when dow=wd then catalog_sales.ext_sales_price else 0) by date.week_seq
```
Two INDEPENDENT aggregates over DIFFERENT fact tables (web_sales, catalog_sales),
each grouped to the SAME grain (`date.week_seq`), then added.

Correct plan (v3/reference): aggregate each fact to week_seq separately (1 row per
week each), then join the two aggregates 1:1 on week_seq and add.

v4 plan (wrong): builds ONE aggregate group at week_seq whose source is a MergeNode
that joins the RAW, un-aggregated web_sales and catalog_sales scans on `date_id`
(= sold_date_sk, a NON-UNIQUE key), then groups. Per-date that join is a
many-to-many product (web_d × catalog_d rows); summed over ~1823 dates at sf=1
(web_sales ~7.2M, catalog_sales ~14.4M) it's billions of intermediate rows. No
literal `1=1` — a SOFT cross join.

Evidence (CTE shape, abbreviated):
```
quizzical = SELECT CS_EXT_SALES_PRICE, CS_SOLD_DATE_SK as date_id FROM catalog_sales   -- RAW, no GROUP BY
wakeful   = SELECT WS_EXT_SALES_PRICE, WS_SOLD_DATE_SK as date_id FROM web_sales        -- RAW, no GROUP BY
... wakeful FULL JOIN quizzical on wakeful.date_id is not distinct from quizzical.date_id   -- FAN-OUT
            RIGHT JOIN highfalutin (date_dim, relevant weeks) on date_id
-> THEN a single GROUP BY date.week_seq with all 14 sum(case...) virt-aggs
```

Node build (`[v4] built` log) confirms ONE aggregate node:
```
grp:aggregate:d0:date.week_seq  AGGREGATE  parents=['SelectNode'] -> GroupNode
```
whose SelectNode/MergeNode source spans both facts.

## Why this is the same family as q29 (C10)

q29 (`test_twenty_nine`, the other genuine crash) is also multi-fact
under-aggregation: its cross-fact ROOT scan
(`physical_sales ⋈ returns ⋈ catalog_sales`) sources as None. Both are "v4 puts
two facts in one row stream when it should aggregate each independently first."
A correct general fix here may also resolve q29 — verify against both.

## Fix direction

Make the planner NOT co-group aggregates whose summed expressions originate from
DISJOINT fact tables. Each `sum(<fact>.ext_sales_price ...) by week_seq` should be
its own aggregate group sourced over ONLY that fact (+ date for the grain key),
and the BASIC `+` should JOIN the two per-fact aggregates on `week_seq` (1:1).

Likely locations (start here, in v4_helper):
- `partition_aggregates` / aggregate grouping in `concept_graph.py` — see memory
  `project_q18_rollup_merge` (union-find merges grouped aggregates whose
  stop-signatures are equal-or-nest). Suspect it merges the two week_seq aggregates
  into one group despite disjoint fact sources. The merge predicate likely needs a
  "same underlying datasource set" guard so cross-fact aggregates stay split.
- `source_planning.py` — how the single aggregate group sources its inputs (it
  currently builds a multi-fact MergeNode; it should refuse to join raw facts and
  instead expect per-fact pre-aggregation).

## Regression risk / validation

Aggregate grouping drives most TPC-DS queries. After any change, run the FULL
v4 sweep and check 0 regressions. High-signal tests to guard:
- `tests/modeling/tpc_ds_duckdb` q18 (rollup merge — legitimate aggregate merge
  that must STILL merge), q14/q11/q33/q45 (cross-grain aggregate filters), q77/q66
  (multi-channel union-of-facts).
- The q2 pair itself (this brief), and q29 (check if also fixed).
- `engine` + `complex` + `optimization` aggregate suites.

## Repro recipes

Generate + inspect SQL (fast, no data needed):
```
cd tests/modeling/tpc_ds_duckdb
TRILOGY_V4_DISCOVERY=1 ../../../.venv/Scripts/python.exe -c "
from trilogy import Environment, Dialects; from trilogy.constants import CONFIG
CONFIG.use_v4_discovery=True
from pathlib import Path
ex=Dialects.DUCK_DB.default_executor(environment=Environment(working_path=Path('.')))
sql=ex.generate_sql(open('query02-one.preql').read())[-1]
print(sql.count('1=1'), sql)"   # check for raw-fact joins
```
Time EXEC at sf=1 (loads 11.7M-row dataset; ~1.4s setup, then exec):
```
TRILOGY_V4_DISCOVERY=1 timeout 200 ../../../.venv/Scripts/python.exe -c "
import time; from trilogy.constants import CONFIG; CONFIG.use_v4_discovery=True
import conftest; from pathlib import Path
ex=conftest._make_engine(sf=1, subdir='memory')
sql=ex.generate_sql(open('query02-one.preql').read())[-1]
t=time.perf_counter(); r=ex.execute_raw_sql(sql).fetchall()
print('exec %.1fs rows=%d'%(time.perf_counter()-t,len(r)))"
```
Direct test (avoids the slow parallel-classifier path; the `engine` session
fixture loads sf=1 ONCE so a single serial run is fine):
```
TRILOGY_V4_DISCOVERY=1 ../../../.venv/Scripts/python.exe -c "
from trilogy.constants import CONFIG; CONFIG.use_v4_discovery=True
import conftest, test_queries as tq
eng=conftest._make_engine(sf=1, subdir='memory')
tq.run_query(eng, 2, sql_override=True, preql_file='query02-one.preql', label='2.1')"
```
Target: EXEC drops from ~70s to <2s; SQL shows two per-fact week_seq aggregates
joined on week_seq, no raw `web_sales`⋈`catalog_sales` on date_id.
```
