# q05 pass→fail regression — run `20260706-222300` — VERDICT: NOT a codegen regression

## Bottom line
The q05 pass→fail flip is **the pre-existing float32 union-placeholder drift resurfacing**,
NOT a new codegen change from `4e69c5547` (more_fixes). The agent simply wrote a *different*
query this run: the failing run uses row-level union arms with **`cast(0 as float)`**
placeholders; the prior passing run used arm-level aggregation with untyped **`0`**
placeholders. Same undiagnosable-silent-drift token sink documented in
`bug_q05_current_engine_float32_union_placeholder.md` / `bug_q05_recheck_20260705.md`.
Classification: **(a) framework type-system gap (no 8-byte DOUBLE) + (b) guidance gap**;
NOT a framework wrong-row/regression bug. Nothing to fix in the planner.

## Proof it is NOT a regression from 4e69c5547
- `4e69c5547`'s `trilogy/dialect/base.py` changes are (1) `_order_expr_needs_group_wrap` /
  `MIN()` ORDER-BY wrapping under `group_to_grain` (q49 fix) and (2) composite-membership
  expression operands (q87 fix). **Neither fires for q05**: the generated outer `ORDER BY`
  is a plain alias (`"busy"."channel_label" asc nulls first`) with **no `MIN()` wrap**, and
  there is no composite `(a,b) in (...)` membership.
- Its `trilogy/ai/{constants,syntax_examples}.py` changes only ADD `except(...)`/`intersect(...)`
  docs — they do **not** touch union zero-placeholder guidance, so guidance did not regress
  toward `float`.
- **The OLD (passing) query form still returns exact `Decimal` values on the CURRENT engine**:
  running `20260706-135542_enriched/workspace/query05.preql` against the new workspace engine →
  `grand=(None, None, Decimal('112458734.70'), Decimal('3255243.12'), Decimal('-31584085.44'))`
  = byte-exact match to reference. So the current engine still codegens the safe form correctly;
  only the agent's newly-chosen float form drifts.

## Reproduced wrong rows (submitted query, current engine)
`SUB grand=(None, None, 112458734.68504244, 3255243.116688758, -31584085.444747422)`
vs `REF grand=(None, None, 112458734.70, 3255243.12, -31584085.44)`. 100/100 rows drift; row
values render as float32 (e.g. `catalog_page2055` returns `265.6499938964844`). `score_query`
→ `status='fail', ref_rows=100, cand_rows=100, 'result set differs from reference'`.

## Single-toggle trigger matrix (change ONLY the placeholder cast, workspace query05.preql)
| variant | grand-total row | verdict |
|---|---|---|
| submitted `cast(0 as float)` | `112458734.68504244, ...` (float32) | **FAIL (drift)** |
| `cast(0 as float)` → `0` (int) | `Decimal('112458734.70'), ...` | **exact match** |
| `cast(0 as float)` → `cast(0 as numeric)` | `Decimal('112458734.700'), ...` | **exact match** |

`by rollup`, entity-id concat, channel labels, order/limit, returns arm — all structurally
correct and match the reference; the placeholder cast is the sole driver.

## Root cause (unchanged from prior notes — file:line)
- Authored: `workspace/query05.preql` lines 12,14,22,24 — `cast(0 as float)` injects
  single-precision `REAL` into the DECIMAL union columns; the union unifies the sibling
  DECIMAL measure down to `REAL`, so the outer `sum(...)` accumulates in float32 and drifts
  past the 9-decimal scoring tolerance.
- Framework type gap: `trilogy/dialect/base.py:343` renders `DataType.FLOAT` → SQL `float`
  (DuckDB 4-byte REAL) and `base.py:377-381` folds `double`/`double precision`/`float8`/`real`
  all into the same `DataType.FLOAT` — Trilogy has no distinct 8-byte DOUBLE, and
  `cast(x as double)` has no target (`HydrationError`). Only untyped `0` or `::numeric` give
  an exact zero placeholder.

## Why it churns / recommendations (do NOT implement here)
Every artifact the agent can inspect (rows, labels, order, rollup, entity-ids) is correct;
the only signal is "result set differs" with an invisible ~8th-digit float32 error, so the
agent thrashes on structure. Highest leverage: (1) guidance — for money/quantity `union(...)`
arm placeholders use untyped `0` or `::numeric`, NEVER `float`/`0.0`/`::double`;
(2) harness — surface the first mismatched cell (value + duckdb column type) on same-shape
failures; (3) framework (larger) — add a real DOUBLE DataType / render `FLOAT`→`double`.
