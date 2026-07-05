# q77 (run 20260704-140355, 545k tok) — NO framework bug; agent-semantics + reference-quirk

## Verdict
**Not a framework bug.** The engine compiled and executed every construct the
agent wrote correctly, and it also runs the *correct* all_sales formulation
cleanly (see trigger matrix). The 43-vs-44 row gap and the wildly-wrong values
are fully explained by (a) an agent WHERE-clause semantics error and (b) the
canonical reference's `FROM cs , cr` cartesian quirk that the agent didn't
replicate. Zero engine errors were ever emitted — every `run` "succeeded" with
silently-wrong numbers, which is exactly what produced the self-doubt token loop.

Classification: **guidance / model-defect (agent)** + **reference quirk**.

## Symptom
Agent final `workspace/query77.preql` (unified `raw.all_sales` model, single flat
`where ... rollup(channel, outlet_id)`) returns **43 rows**; reference SQL returns
**44**. Beyond the row count, nearly every value is wrong: returns ~6–750× too
low per channel, catalog sales ~5× too low.

## The exact missing row (the off-by-one)
Reference row #44 = the **catalog NULL-call-center LEAF**, not a subtotal:

```
channel='catalog channel'  id=NULL  grouping(channel)=0 grouping(id)=0
  sales=379,488.65  returns=2,008,328.89
```

It shares its printed key `(catalog channel, None)` with the catalog *subtotal*
row (`grouping(id)=1`, sales=399,769,334.40), so a naive `(channel,id)` diff
collapses them — that is why the gap looks invisible. The agent's clause
`and all_sales.outlet_id is not null` filters this NULL-outlet leaf out → 43 rows.

Cross-check: `379,488.65 = 75,897.73 × 5`, where 75,897.73 is the un-inflated
all_sales catalog-NULL sales and ×5 is the cs×cr cartesian factor (below).

## Root causes (both agent/reference, not engine)

1. **Sale-date WHERE gates the return measure.** The agent wrote
   `where all_sales.date.date between period` in the *outer* WHERE of a unified
   line-grain model. On that model WHERE runs before aggregation, so it also
   restricts the return measure to lines *sold* in the window. A return in the
   window almost always belongs to a sale from an earlier window → the return
   measure collapses. Correct pattern: NO outer date WHERE; filter each measure
   independently (`sum(ext_sales_price ? sold_date...)` and
   `sum(return_amount ? return_date...)`).

2. **`outlet_id is not null` over-filter** drops the catalog NULL-call-center
   leaf (row #44). The reference's catalog arm has no such filter.

3. **Reference cs×cr cartesian (`FROM cs , cr`, no join predicate)** multiplies
   catalog sales/returns by the number of catalog-returns call-center groups
   (=5 here). The canonical `.preql` replicates this via a `cr_n_groups`
   broadcast multiplier; the agent's un-inflated catalog numbers are ~5× smaller
   and are arguably the "true" values, but score as wrong vs the reference.

## Trigger matrix

Returns probe — STORE outlet 10 (reference = 557,423.15):
| variant | returns |
|---|---|
| agent final (outer sale-date WHERE gates returns) | 86,121.30  (~6.5× low) |
| remove outer date WHERE; filter each measure separately | 575,629.38  (correct magnitude) |
| raw `store_returns` summed by return store | 557,423.15  (exact = reference) |

Catalog sales probe — outlet 1 (reference = 131,267,647.10):
| variant | sales |
|---|---|
| all_sales true value | 26,253,529.42 |
| reference cs×cr cartesian ×5 | 131,267,647.10 = 26,253,529.42 × 5.0 |

Missing-leaf probe — catalog id=NULL leaf (reference sales = 379,488.65):
| variant | leaf present? |
|---|---|
| agent final (`... and outlet_id is not null`) | dropped → 43 rows |
| same query minus `outlet_id is not null` | present (id=NULL sales=75,897.73 un-inflated) |
| reference | present (75,897.73 × 5 = 379,488.65) |

**Engine-soundness control:** the *correct* all_sales formulation (no outer date
WHERE, both measures filtered, `rollup(channel, oid)`) runs cleanly — 76 rows,
sensible per-outlet returns near the reference magnitude. The filtered-aggregate
+ rollup + coalesce path is not broken. (residual mismatch vs reference is the
sale-store-vs-return-store attribution + the catalog cartesian, both semantic.)

## Why 545k tokens (no engine error involved)
- Agent committed to `all_sales` at turn ~[20] and never tried a per-channel
  union. It looped 5× (`file write` → `run` → re-examine) on the same query.
- Every `run` returned exit-0 with a full 43-row table; the agent had no
  expected-answer to diff against, so it re-validated *structure* against the
  prompt and kept second-guessing silently-wrong numbers.
- Harness amplifiers (not bugs): each `run` dumps the full result table and
  `agent-info` is ~26KB, so the replayed prompt grew to ~45k tok/turn × 16 turns.

## Repro harness
```python
import sys; sys.path.insert(0,'evals'); from common import scoring
from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260704-140355/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
# agent: rows(( ws/'query77.preql').read_text()) -> 43
# ref:   eng.execute_raw_sql(Path('tests/modeling/tpc_ds_duckdb/query77.sql').read_text()) -> 44
```
Canonical `tests/modeling/tpc_ds_duckdb/query77.preql` **builds** on the current
engine (compiles to SQL); executing it to confirm 44 rows requires its own
memory-schema DB, not the agent workspace DB.

## Files
- agent attempt: `evals/tpcds_agent/results/20260704-140355/workspace/query77.preql`
- model: `evals/tpcds_agent/results/20260704-140355/workspace/raw/all_sales.preql`
- reference: `tests/modeling/tpc_ds_duckdb/query77.sql`, `query77.preql`
