# bug q05 — silent wrong money sums from `::float` union placeholders (float32 drift)

Bucket: **A (silent wrong-rows, no error)**. Known-open issue
(`float32_union_placeholder_drift_no_double_type`), re-confirmed as the *sole* cause of the q05 FAIL.

## Symptom
Agent's final `query05.preql` compiles and runs cleanly, returns 100 rows in correct order with
correct row membership — but the exact-value score FAILS. 7 of 100 rows differ from the reference,
all on the higher-magnitude subtotal / grand-total / large-entity rows:

| row | metric | cand (`::float`) | ref |
|-----|--------|------|-----|
| grand total | sales | 112458735.49 | 112458734.70 |
| grand total | net profit | -31584085.56 | -31584085.44 |
| catalog subtotal | sales | 38544639.24 | 38544639.28 |

Drift up to ~$0.79 on the grand total. No error, no warning — the agent had no signal its money
sums were corrupted, and submitted it as final.

## Root cause
The agent modeled each union arm's "other channel's" measures as typed zero placeholders:
`0::float as r`, `0::float as rnl` (and symmetric). Trilogy `DataType.FLOAT` renders to DuckDB
`float` = REAL = 4-byte single precision (`trilogy/dialect/base.py:359`, `DataType.FLOAT: "float"`;
DOUBLE→`"double"` is 8-byte). The union column's type is then FLOAT, so the rollup `SUM()`
accumulates in float32 and drifts on large partial sums. Dotted literal `0.0` parses to DOUBLE
(base.py:251 comment) and accumulates cleanly.

## Trigger matrix (single ingredient flips PASS/FAIL)
Same file, only the placeholder literal changed:

| placeholder | grand total sales | rows mismatched vs ref | verdict |
|-------------|-------------------|------------------------|---------|
| `0::float`  | 112458735.49      | 7                      | FAIL |
| `0.0`       | 112458734.70      | 0                      | PASS (exact) |

Repro (workspace `20260715-153056`):
```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path; from common import scoring
ws=Path('evals/tpcds_agent/results/20260715-153056/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
base=open(ws/'query05.preql').read()
rows(base)[0]                       # (None,None,112458735.49,...)  FAIL
rows(base.replace('0::float','0.0'))[0]  # (None,None,112458734.70,...) exact match
```

## Why the canonical answer avoids it
`tests/modeling/tpc_ds_duckdb/query05.preql` never builds typed placeholder columns. It uses the
merged `all_sales` model directly with a `def windowed(...)` macro whose fallback literal is `0.0`
(double) and gates each measure by its own dim — so there is no FLOAT-typed union column to
accumulate in float32.

## Fix options (in priority order)
1. **Framework**: give Trilogy a real 8-byte type or make `::float` on DuckDB emit `DOUBLE`
   (or default numeric-literal / untyped-0 unions to DOUBLE). Today `float` silently means
   float32 and quietly corrupts money math — the exact hazard the operating assumption flags.
2. **Guidance (cheap, immediate)**: model comment / agent-info note: "use `0.0` or `::numeric`, never
   `::float`, for measure placeholders — `float` is 4-byte and drifts money sums." Would have
   collapsed this to a PASS with zero extra turns.

## Note on the 1.22M token burn
Not driven by this bug (the agent never saw the drift). Trajectory was clean/non-thrash (~29 tool
calls). Tokens went to discovery: 7 `explore` calls over huge fact models (all_sales 318, store_sales
328, catalog_sales 545, web_sales 515 concepts) re-sent each turn — several redundant since
`all_sales` already merges store/catalog/web. The prior "rollup-label + FK-bridge idiom gap" is
**resolved**: the agent's `union((...),(...)) -> (...)` + `by rollup(...)` + channel-prefix CASE
compiled and ran on the first structurally-correct attempt.
