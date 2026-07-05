# HANDOFF — q14: multi-key `subset join` onto a rowset leaks an ungrouped key into a ROLLUP CTE (BinderException); some shapes wrongly DisconnectedConcepts

**Status:** OPEN, ready to implement. Root-caused, minimal repro + trigger matrix on today's engine.
**Full diagnosis:** `evals/tpcds_agent/bug_q14_recheck_20260705.md`

**RECONFIRMED 2026-07-05:** the BinderException below **reproduces** on the current engine
(`generate_sql` + execute against the run workspace). Confirmed OPEN.

**SCOPE (read this first):** In Trilogy **no join restricts rows** — `subset`/`union`/`left`/`full`
joins only establish conceptual *bridges* so concepts can be referenced across rowsets; restriction
is done exclusively by `where`. So the "loose / row-preserving" behavior in the trigger matrix
below is EXPECTED, not a bug — do NOT try to make any join restrict. The confirmed bug is a single
codegen defect:
- a 3-key `subset join` onto a rowset under `by rollup` emits an **ungrouped key column in the
  ROLLUP CTE → DuckDB BinderException**.

(An earlier draft also listed a "DisconnectedConcepts in reduced shapes" defect. On reconfirmation
a 2-key subset join with no rollup **built and ran fine** (4 rows) — it did NOT reproduce. Treat
DisconnectedConcepts as UNVERIFIED / possibly already fixed; focus on the BinderException. If you
do hit a genuine DisconnectedConcepts where a `subset join` is present to bridge the concepts,
that's a connectivity bug — but do not make the join restrict rows.)

**Classification:** REAL framework bug (codegen). Loud (BinderException), not silent.
**Context:** all THREE previously-documented q14 bugs are VERIFIED FIXED on the current engine
(expr-RHS membership BinderException; `by rollup`+having-vs-cross-rowset-scalar subtotal drop, both
direct and `auto`-wrapped; numeric parameter). The prior "scoped INNER→FULL" primary is moot —
scoped `inner join` was removed from the language. This is a **new/distinct** driver.

## Symptom
q14 needs multi-column `(brand_id, class_id, category_id)` tuple membership: items sold in all
three channels, then channel sales rolled up where the item is in that common set and avg price
exceeds a global average. The **canonical idiom works** — encode the tuple as a single
`concat(brand,'|',class,'|',category)` key and filter `tuple_key in cross.ci_tuple_key`
(`query14.preql` → 100 correct rows). But the natural **`subset join` on the three keys** fails.

The full q14 shape — 3-key `subset join` onto a rowset + `by rollup(4 keys)` + `case grouping()`
projections + outer `having sum(...) > overall_stats.overall_avg` — generates illegal SQL:
```
BinderException: column "all_channel_bcc_category_id" must appear in the GROUP BY clause
```

## Minimal repro (BinderException) — see the bug report §"Minimal repro" for the full body
3-key subset join onto a rowset, rolled up over the join keys, with a cross-rowset scalar `having`
and `grouping()` projections. Harness:
```python
import sys; sys.path.insert(0,'evals'); from common import scoring; from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260705-142435/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
```

## Trigger matrix (today's engine)
| shape | result |
|---|---|
| canonical concat-`tuple_key` membership + rollup + having-vs-rowset-scalar | **OK (100 rows, correct)** |
| single-col `in` membership + rollup + having-vs-rowset-scalar | OK |
| 1-key subset join + rollup | OK but **loose** (row-preserving, not restricted) |
| 2-key subset join, no rollup | DisconnectedConcepts (build) |
| 2-key subset join + rollup(channel only) | DisconnectedConcepts (build) |
| 2-key subset join + rollup(keys) + case/grouping | OK but loose |
| 2-key subset join + rollup(keys) + case + having-vs-scalar | OK but loose |
| **3-key subset join + rollup(keys) + case + having-vs-scalar** | **BinderException (exec)** |

The crash needs the **3rd** join key — the extra rowset-boundary join layer it adds is what leaks
an ungrouped exposed key into the rollup CTE. (The "OK but loose" cells are NOT a defect — a join
never restricts; that's expected. The real defects are the BinderException and the
DisconnectedConcepts cells.)

## Root cause (file:line)
`MergeNode._inject_scoped_join_key_exposure`
(`trilogy/core/processing/nodes/merge_node.py:311-348`) force-adds every coalescing-join-key
**group member** (`all_channel_bcc.category_id`, …) as an output of each merged parent so
shared-canonical join inference can pair the sides. It is **grain-blind**: when the parent it
augments is (or feeds) a `ROLLUP`-grouped aggregate node, the injected key column is emitted in
that CTE's `SELECT` but is neither aggregated nor in the `ROLLUP(...)` GROUP BY list → illegal SQL
(`… all_channel_bcc_category_id as … FROM … GROUP BY ROLLUP(2,1,3)`).

The subset join lowers to a coalescing outer join via `MergeNode.create_full_joins`
(`merge_node.py:246`); stacking that across three rowset boundaries (one layer per extra key, plus
the cross-rowset-scalar `having` join) produces the ungrouped-column CTE. Coalescing-scoped-join key
handling lives in `rowset_node.py:395` (`_expose_coalesced_key_contents`).

## Fix direction
1. **Make key exposure grain-aware.** `_inject_scoped_join_key_exposure` must not add a raw
   (un-aggregated) join-key member as a bare output of a parent that is/feeds a grouped aggregate
   (ROLLUP or plain GROUP BY). Either route the exposure through the group's grouping set (so the
   key is part of GROUP BY), or expose it on a non-aggregating sibling node used only for the join
   inference — not on the aggregate's SELECT. Mirror the fix shape used for the q17 group_to_grain
   defect (the same "ungrouped key column in an aggregate CTE" symptom, different injection site).
2. **Fix the DisconnectedConcepts shapes:** a `subset join` is present precisely to bridge the two
   rowsets' concepts, yet several shapes (2-key subset join, no/partial rollup) raise
   DisconnectedConcepts — the bridge fails to register in the discovery graph. Investigate whether
   this shares the exposure-grain root above or is a separate connectivity gap. Do NOT make the
   join restrict rows (joins never restrict; `where` does) — just make the bridge connect.

## Test to add
DuckDB codegen+execute tests on the `raw.*`/`all_sales` model shape:
- 3-key subset join + rollup + grouping()/having must generate **legal SQL** (no BinderException)
  and run — pairing it with the appropriate `where` presence conditions must return the canonical
  100 rows (matching the concat-`tuple_key` result).
- 2-key subset join shapes that currently raise DisconnectedConcepts must build (the bridge
  connects). Assert the concepts resolve — NOT that rows are restricted.
- Keep the canonical concat-`tuple_key` path green.

## Acceptance criteria
- The BinderException repro compiles + executes (legal GROUP BY / ROLLUP).
- The DisconnectedConcepts shapes build (the subset join bridges the concepts).
- Canonical `tests/modeling/tpc_ds_duckdb/query14.preql` still returns 100 correct rows.
- No regression in `tests/join_matrix/`, scoped-join, and rowset-aliasing suites.
- `ruff check . --fix && mypy trilogy && black .` clean.

## Do NOT
- Do NOT try to make any join restrict rows — joins only bridge concepts; `where` restricts. The
  "loose"/row-preserving matrix cells are correct behavior, not the bug.
- Do NOT special-case q14; the fix is for multi-key `subset join` onto a rowset generally.
