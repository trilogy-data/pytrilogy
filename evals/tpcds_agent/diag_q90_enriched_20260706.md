# Diagnosis: q90 FAIL in enriched leg (run 20260706-135542_enriched)

## Classification: AGENT error — output shape (extra visible columns; known pattern 4, same as q74)

## Symptom
Scorer: `status='fail', ref_rows=1, cand_rows=1, detail='result set differs from reference'`.
Reference emits one column (`am_pm_ratio`); candidate emits three (`morning`, `evening`, `ratio`).
Scorer hashes whole rows, so `(113, 201, 0.5621890547263682)` != `(0.5621890547263682,)`.

## Error inventory (agent_log.q90.conversation.txt)
1. msg 11: Trilogy syntax guard "Output column 'morning_count' renames 'local.morning_count' back to ... itself" — agent recovered (renamed outputs).
2. msg 15: same guard for 'ratio' vs `auto ratio` — agent recovered (`ratio_val`).
3. msg 19: final run clean, exit 0, 1 row `[113, 201, 0.5621890547263682]`. No framework errors, no binder/catalog errors, generated SQL valid.

## Numerator/denominator decomposition (DB copy of tpcds_sf1.duckdb)
- Reference SQL (`tests\modeling\tpc_ds_duckdb\query90.sql`): `0.5621890547263682`.
- Raw SQL decomposition (inner joins, hd_dep_count=6, wp_char_count 5000-5200): amc=113, pmc=201 → 113/201 = 0.5621890547263682.
- Candidate's morning=113, evening=201, ratio=0.5621890547263682 — **numerically identical to the reference**.
- Known-pattern checks that do NOT apply: implicit NULL-FK filter (pattern 1) is moot — every filter (`dependent_count`, `char_count`, `hour`) requires the joined dim, so Trilogy's LEFT joins + CASE-filters null out NULL-FK rows and counts match exactly; numeric formatting (float32 drift) does not apply — `count/nullif(count,0)` renders as DuckDB int division → DOUBLE, matching the reference decimal quotient bit-for-bit.

## Counterfactual proof (fail → pass)
Copied the workspace to scratchpad and made ONE edit to `query90.preql`: select only `ratio_val as ratio` (dropped `morning_count as morning` and `evening_count as evening`). Re-scored with the same engine/refs:
`QueryResult(id=90, status='pass', ref_rows=1, cand_rows=1)`.

## Canonical query90.preql check
Scoring the canonical workspace directly errors with `Catalog Error: Table with name "memory.web_sales" does not exist` — a harness artifact only (the curated model binds datasources under the `memory` schema used by the test attach; the scoring engine opens a file DB with no such schema). Executing the exact SQL it generated with `"memory".` stripped returns `0.5621890547263682` — canonical still matches query90.sql. Not a q90 defect.

## Root cause + evidence
Task (`task.q90.txt` line 19) is explicit: **"Report a single value: the morning count divided by the evening count as a decimal ratio."** The agent's final select (workspace\query90.preql lines 19-24) added `morning` and `evening` as visible output columns alongside `ratio`, despite the "single value" instruction. Framework, model, and question are all correct: the engine planned and executed the query correctly (LEFT-join semantics still yield the exact reference counts), the curated model has every needed concept (`ship_household_demographic.dependent_count`, `web_page.char_count`, `time.hour`, `line_item`), and the task wording matches the reference exactly.

Recommendation: none for framework/model. Agent-guidance level: reiterate "output exactly the requested columns" (recurring pattern — q74, q90).
