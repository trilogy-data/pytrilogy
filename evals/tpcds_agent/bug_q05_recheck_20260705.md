# q05 re-check — run `20260705-142435` (906,805 tokens, FAILED). Trilogy v0.3.289

## CURRENT VERDICT

- **The documented `by rollup`-silently-dropped bug is NOT what fails q05 this run.**
  The agent used the fresh-`sum(...)` idiom (M5/M6), so `GROUP BY ROLLUP` IS emitted
  and the result carries the grand total + per-channel subtotals in the correct
  nulls-first order. Structurally the submitted output is row-for-row identical to
  the reference (same 100 rows, same order, same rollup rows).
- **The 20260704 ORDER-BY-`_level`/`LIMIT` defect is also gone** — this run orders by
  `channel_type nulls first, entity_id nulls first` (the reference ordering), no
  `_level`, no `--` footgun.
- **CURRENT CAUSE = a NEW, distinct AUTHORING / TYPE-PRECISION error:** the agent's
  `0::float` placeholder casts in the `union(...)` arms coerce two measure columns to
  single-precision `REAL`, producing float32 rounding drift vs the reference's exact
  `DECIMAL(7,2)`. Pure authoring defect, **not** a framework bug.

Classification: **new-distinct authoring error** (type/precision), with a **secondary
guidance gap** (silent "result set differs" gives the agent no numeric-diff signal,
which is what burned the tokens). The documented framework passthrough-rollup-drop is
**still latent (M4/M7 below) but was not exercised.**

## Fresh trigger matrix — `by rollup` on today's engine (v0.3.289)

| # | Construct | ROLLUP/GROUPING SETS in SQL? |
|---|-----------|:---:|
| M1 | base-model `select … sum(x) by rollup (a,b)` | **YES** |
| M4 | single rowset, PASSTHROUGH `coalesce(agg.sales,0)` + rollup | **NO — still dropped** |
| M5 | single rowset, FRESH `sum(agg.sales)` + rollup | **YES** |
| M6 | `union(...)` rowset, FRESH `sum(combined.x)` + rollup (= this run's shape) | **YES** |
| M7 | `auto tot <- sum(x) by …` referenced as passthrough + rollup | **NO — still dropped** |

So the passthrough drop documented in
`bug_q05_by_rollup_silently_dropped_over_passthrough_aggregates.md` is **UNCHANGED /
still open** — but the agent side-stepped it with fresh sums, so it did not fire.

## The actual failure — minimal repro & proof

Submitted `query05.preql` = `union(sale_arm, return_arm) -> combined`, then
`select … sum(combined.ext_sales) … sum(combined.returns_amt) … sum(combined.profit)
by rollup (combined.channel, combined.entity_text_id)`.

The union arms use zero placeholders cast to `::float`:
```
# sale arm            # return arm
coalesce(sum(ext_sales_price),0) as ext_sales     0::float          as ext_sales
0::float              as returns_amt              coalesce(sum(return_amount),0) as returns_amt
coalesce(sum(net_profit),0) as profit             0 - coalesce(sum(return_net_loss),0) as profit
```
Because one arm's `ext_sales` / `returns_amt` is `0::float` (single-precision `REAL`),
the unified union column type is `FLOAT`, so the OTHER arm's exact `DECIMAL(7,2)` sum
is coerced to `REAL`. The final `sum(combined.ext_sales)` then accumulates in float32.

Grand-total row, submitted vs reference SQL (`tests/modeling/tpc_ds_duckdb/query05.sql`):
```
SUB:  (None, None, 112458735.48859596,   3255243.1506177187,  Decimal('-31584085.44'))
REF:  (None, None, Decimal 112458734.70, Decimal 3255243.12,  Decimal('-31584085.44'))
```
Leaf example: `catalog_pageAAAAAAAAAAABAAAA` sales `163753.9375` (float32) vs `163753.93`.

**Internal control that pins the cause:** the `profit` column has NO `::float`
placeholder (`0` / `0 - …`), so it stays `DECIMAL` and is an EXACT match to the
reference in every row. Only the two `::float`-placeholder columns (`ext_sales`,
`returns_amt`) drift. Scoring rounds to 9 decimals (`evals/common/scoring.py`
`_round_cell` L286-312); the ~0.79 absolute drift at the grand total and per-leaf
float32 noise far exceed that → `_multiset` mismatch → "result set differs".

### Proof (harness on the run's own workspace)
```python
import sys; sys.path.insert(0,'evals'); from common import scoring
from common.scoring import _multiset
from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260705-142435/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
ref=list(eng.execute_raw_sql(Path('tests/modeling/tpc_ds_duckdb/query05.sql').read_text()).fetchall())
sub=(ws/'query05.preql').read_text()
_multiset(rows(sub))                      == _multiset(ref)  # -> False  (submitted, ::float)
_multiset(rows(sub.replace('::float','::numeric'))) == _multiset(ref)  # -> True   (one-token fix)
```
Changing ONLY `::float` → `::numeric` (nothing else) makes the query match the
reference exactly (grand total `Decimal('112458734.70')`, all 100 rows). `::double` is
not a valid Trilogy cast target (`HydrationError: Unknown cast target 'double'`).

## Reference sanity (current engine)
`tests/modeling/tpc_ds_duckdb/query05.preql` builds and returns the 100 reference rows
(grand total + subtotals, nulls-first), matching `query05.sql` exactly. Reference is fine.

## Root cause (file:line)
- **No framework bug fires here.** The failure is authored `0::float` in
  `workspace/query05.preql` L11 & L20 (single-precision `REAL` coercion of a
  `DECIMAL(7,2)` union column). The model's `raw/all_sales` declares the fact columns
  as `float?` (agent_log L1158-1188), which likely nudged the agent toward `::float`;
  but the underlying DuckDB columns are `DECIMAL(7,2)` and the reference keeps decimal.
- **Latent, not fired:** the documented passthrough rollup-drop lives in
  `trilogy/parsing/v2/select_finalize.py` — `_is_ungrouped_aggregate` requires a bare
  `AggregateWrapper` with `not node.by`, so a pre-aggregated (`by`'d) rowset/`auto`
  measure is never stamped and `_propagate_select_grouping` discards the spec (M4/M7
  above). Unchanged since the prior doc; not on this run's path.

## Why it burned 906k tokens (guidance gap)
The submitted query runs clean, returns 100 rows, correct labels, correct rollup
structure, correct order — every artifact the agent can inspect looks right. The only
scoring feedback is "result set differs from reference" with no numeric diff, and the
divergence is a ~7th-significant-digit float32 error invisible to eyeballing. The agent
cannot see that two columns are `REAL` vs `DECIMAL`, so it churns on structure. Same
"silent, undiagnosable, correct-looking output" failure mode as the 20260704 run — the
specific defect moved (rollup-drop → order/limit → float-precision) but the token-sink
mechanism (no diff signal on a silently-wrong result) is identical.

## Recommendations (do NOT implement here)
- **Guidance:** for money/quantity measures use `::numeric`/`::decimal` (or an untyped
  `0` / `0.0` literal placeholder), never `::float`, in `union(...)` arm placeholders —
  `::float` is single-precision `REAL` and drifts past the 9-decimal scoring tolerance.
  Better: derive the two arms without needing zero placeholders, or cast placeholders to
  match the fact column type.
- **Harness/diagnostic (optional):** when a candidate is same-shape/same-cardinality but
  fails, surface the first mismatched cell (value + type) so precision/type errors are
  self-evident instead of a 906k-token guessing game.
- **Framework (separate, still open):** close the M4/M7 passthrough rollup-drop
  (raise on a `by rollup` with no stampable aggregate, or re-aggregate the passthrough).
