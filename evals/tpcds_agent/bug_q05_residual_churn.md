# q05 residual churn (run 20260628-194910) — fan-out FIXED; failure is a float-vs-Decimal scoring gap

**TL;DR.** The nested-rowset aggregate fan-out bug (`bug_q05_nested_rowset_aggregate_fanout.md`)
is **FIXED** and verified by row-count below. The agent actually converged on a
**semantically correct** query — its per-channel subtotals and grand total match the reference
to the cent. It was still graded **fail / "result set differs from reference"** because the
candidate emits Python **`float`** cells while the reference SQL emits **`Decimal`** cells, and
the eval scorer cannot equate the two. **Classification: (c) harness scoring gap + model typing
gap. NOT a residual framework planner-correctness bug, NOT really an agent-logic bug.**

## 1. Fan-out bug is fixed (row-count evidence)

Re-ran the exact minimal repro from `bug_q05_nested_rowset_aggregate_fanout.md` (union rowset →
passthrough/relabel rowset → bare global `sum(measure)`) against the run's workspace DB
(`results/20260628-194910/workspace/tpcds.duckdb`, current engine 0.3.287):

| statement | what | row_count (this run) | row_count (when bug filed) |
|---|---|---|---|
| A | global `sum` over the single union rowset | **1** | 1 (always correct) |
| B | global `sum` over the 2-level chain (the bug) | **1** | 62,237 |

Statement B collapses to **1 row** — the fan-out is gone. Both bug files
(`bug_q05_nested_rowset_aggregate_fanout.md`, `q05_union_measure_broadcast_bug.md`) are
correctly marked FIXED.

## 2. The agent's submitted query is actually correct

`workspace/query05.preql` (union-of-two-arms → `by rollup (channel_type, entity_id)`), run with
the limit removed, produces subtotals **identical** to the reference `query05.sql`:

| channel | gross (agent) | gross (ref) | returns (agent) | returns (ref) | profit (agent) | profit (ref) |
|---|---|---|---|---|---|---|
| GRAND   | 112,458,734.69 | 112,458,734.70 | 3,255,243.12 | 3,255,243.12 | -31,584,085.44 | -31,584,085.44 |
| catalog | 38,544,639.26 | 38,544,639.28 | 1,083,573.98 | 1,083,573.98 | -4,396,139.07 | -4,396,139.07 |
| store   | 54,273,632.10 | 54,273,632.11 | 1,552,466.33 | 1,552,466.33 | -24,696,015.41 | -24,696,015.40 |
| web     | 19,640,463.32 | 19,640,463.31 | 619,202.81 | 619,202.81 | -2,491,930.97 | -2,491,930.97 |

Both produce **700 data rows + 3 channel subtotals + 1 grand total**, same ordering. The WEB
gross 19,640,463.31 is exactly the "correct" value the fan-out fix report claims — independent
confirmation the fan-out is fixed and the agent did not re-trigger it.

(Note: the message-46 intermediate the agent ran early showed grand-total returns 3,277,859.92 /
702 rows — that was a *pre-fix-to-its-own-query* version with extra null-entity leaf rows. The
agent then added an `is_returned`/entity filter and converged to the correct 700-row shape. Its
iteration was self-corrective, not driven by a framework error.)

## 3. Why it is still graded "fail" — float vs Decimal (scoring gap)

Applying the scorer's own comparison (`evals/common/scoring.py`, `_multiset` + `_round_cell`)
to the candidate vs reference rows: **0 of 100 rows match**, despite the values above.

- Candidate row 0: `(None, None, 112458734.68504243, 3255243.116688758, -31584085.444747422)` —
  cells are **`float`**.
- Reference row 0: `(None, None, Decimal('112458734.70'), Decimal('3255243.12'), Decimal('-31584085.44'))`
  — cells are **`Decimal`**.

`_round_cell` (scoring.py:228-240) rounds **floats** to 9 decimals but **leaves `Decimal`
untouched**, and `_multiset` compares cells by `repr`. A `float` and a `Decimal` can therefore
**never** compare equal — different repr, different type. The mismatch is compounded numerically:
the candidate sums money in float (full precision), the reference sums `DECIMAL(7,2)` values
(per-row 2-decimal rounding), so they also diverge in the last cent (e.g. gross
112,458,734.685 → .69 vs .70).

### Root of the float-ness (model typing gap)
The DuckDB source columns are `DECIMAL(7,2)` (e.g. `ss_ext_sales_price`, `ss_net_profit`), but
the combined **`all_sales`** model declares the money fields as **`float?`**:
- `raw/all_sales.preql:42` `ext_sales_price float?`, `:47` `net_profit float?`,
  `:51` `return_amount float?`, `:53` `return_net_loss float?`.
- The per-channel models keep precision: `raw/catalog_sales.preql:36`
  `ext_sales_price numeric(15,2)::usd`, `:40` `net_profit numeric::usd`.

Any query routed through `all_sales` (the model the task steers the agent to, and the natural
one for q05) therefore yields floats. **The canonical/blessed Trilogy answer
`tests/modeling/tpc_ds_duckdb/query05.preql` imports the same `all_sales`, whose model
(`tests/modeling/tpc_ds_duckdb/all_sales.preql:39-53`) also declares these as `float?` — so the
blessed query would produce floats too and ALSO fail this scorer.** The q05 task is effectively
ungradeable as currently wired.

## 4. Classification & recommended action

- **(a) residual framework correctness bug:** NONE found. Fan-out fixed (1 row), union measure
  broadcast fixed, rollup grain / return attribution / web-SCD all correct to the cent.
- **(b) agent difficulty/logic:** minor. The ~1.05M-token churn was the agent second-guessing an
  ambiguous prompt clause ("use a non-null return store identifier by grouping") and the
  union-vs-coalesce modeling choice, plus re-running to eyeball plausibility. It had no in-loop
  oracle, so it could not know it was already correct — and could not have passed regardless.
- **(c) guidance / harness gap (PRIMARY):** two independent, either-one-fixes-it seams:
  1. **Scorer** `evals/common/scoring.py:_round_cell` (228-240) only normalizes `float`; it
     should coerce `Decimal` and `float` to a common rounded representation (e.g. cast both to
     `round(float(x), 2)` for money, or quantize) before the `repr` multiset compare. Today a
     correct float answer can never match a Decimal reference.
  2. **Model typing** `raw/all_sales.preql:39-53` (and the mirror
     `tests/modeling/tpc_ds_duckdb/all_sales.preql:39-53`) declare `DECIMAL(7,2)` source columns
     as `float?`. Declaring them `numeric`/`::usd` (as the per-channel models do) would preserve
     Decimal end-to-end and let an otherwise-correct query pass.

**Recommended:** fix the scorer's Decimal/float normalization (1) — it almost certainly causes
silent false-fails on other money/rollup queries in this same run (q14, q18, q30, q35, q47,
q51, q56, q67, q69, q72, q79, q80 are all 100/100-row "differs" candidates worth re-checking
under a type-normalized comparison). Optionally also retype `all_sales` money columns to numeric.
Do **not** treat q05 as a planner bug.

## Repro provenance
- Engine: pytrilogy 0.3.287, branch `post_join_followup_four`, workspace
  `evals/tpcds_agent/results/20260628-194910/workspace/` (`trilogy.toml` + `tpcds.duckdb`, sf=1).
- Fan-out repro file used: union → `formatted` passthrough → `sum(formatted.gross)` (section 1).
- Scorer logic: `evals/common/scoring.py` `_score_one` / `_multiset` / `_round_cell`.
