# Handoff — q87: `HideUnusedConcepts` prunes EXCEPT/INTERSECT output columns (compares 1 col not the tuple)

**Verification:** ⚠️ SUBAGENT-REPORTED — CONFIRM the repro before fixing (q59 lesson: a subagent
verdict was flat wrong). Detail report: `bug_q87_composite_membership.md` /
`bug_q87_composite_except_sink_20260706.md`.

## Confirmed-by-subagent bug
A multi-output `except(...)` / `intersect(...)` whose downstream consumer references only ONE of the
declared output columns has its set-op arms PRUNED to that one column. So the EXCEPT/INTERSECT
compares a single column instead of the full tuple → row-identity collapses.
- e.g. `count(sale_date)` over a `(last, first, date)` except returns **0**; referencing all three
  columns returns the correct NULL-safe count (~47,270 ≈ reference 47,298).
- SILENT (runs clean, wrong count). q87 FAIL ref 47,298 / cand 45,689.

## Root cause (locus)
`trilogy/core/optimizations/hide_unused_concept.py`, `HideUnusedConcepts.optimize`:
- L87-110 set `cte.hidden_concepts`; L118-128 propagate into each arm's `hidden_concepts`.
- The pass prunes unused `UnionCTE` outputs with NO awareness of `cte.set_operator`. Valid for
  `UNION ALL` (extra columns are harmless), UNSOUND for `EXCEPT`/`INTERSECT` where EVERY declared
  column is row-identity.

## Fix direction
In `HideUnusedConcepts`, skip hiding/propagating for any `UnionCTE` whose
`set_operator is not SetOperator.UNION_ALL` (i.e. keep all columns for EXCEPT/INTERSECT). `INTERSECT`
shares the defect; `UNION ALL` prunes harmlessly.

## Guard test
`tests/engine/` (near `test_duckdb_setops.py`): a multi-column `except`/`intersect` whose final SELECT
references only a subset of the declared columns — assert the count/rows match the full-tuple set
difference, and assert the generated SQL's EXCEPT/INTERSECT arms still project all declared columns.

## Priority note
NEW-feature latent bug: commit `4e69c5547` added the `except`/`intersect` TVFs AND rewrote AI guidance
to PREFER `except` over multi-column `not in` (with a q87-shaped example). The guidance now steers
agents straight into this — highest-urgency of the set-op fixes.
