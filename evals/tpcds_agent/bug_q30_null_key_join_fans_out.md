# **P1** q30: a NULL join key matches NULL under `is not distinct from`, fanning out 20 rows into 6,220

**Filed 2026-08-16 against `a65b13c9c`.** Extracted from `bug_q30_scoring_hang.md`
(deleted — the P0 hang it described is fixed). **The root cause in that file's
FOLLOW-UP section was wrong; it is corrected here.**

## Not the bug: preserving the address side

The old diagnosis blamed `RIGHT OUTER JOIN customer_address` for preserving GA
addresses that are no customer's `c_current_addr_sk`. That preservation is
**expected and arguably declared**: the ingest model binds
`c_current_addr_sk: ~customer_address.address_sk`, a partial binding, so
`customer_address` holds the complete domain and `customer` is a partial
contributor. A query that wants only real customers has to say so
(`... and wr.returning_customer.customer_sk is not null`). Preservation alone
would add **311** rows and would not be worth a report.

## The actual bug: NULL pairs with NULL, so 311 x 20 = 6,220

Measured on `results/20260813-125008_ingest/workspace/query30.preql`
(limit removed), sf=1:

| quantity | value |
|---|---|
| GA addresses with no customer | **311** |
| distinct NULL-customer output rows | **20** |
| NULL-customer rows returned | **6,220** |
| multiplicity of every one of those 20 distinct rows | **exactly 311** |
| non-NULL rows (the real answer, matches the reference row-for-row) | 153 |
| total | 6,373 |

`311 x 20 = 6220`, and the per-row multiplicity is uniformly 311 — there is no
other reading. The 311 unmatched address rows and the 20 NULL-key aggregate
groups are **cross-joined**.

Both sides carry a NULL on the join key for different reasons:

- the address side, because the outer join found no customer (NULL = *absent*);
- the aggregate side, because `cust_state_amt_2002` is grouped
  `by wr.returning_customer.customer_sk, wr.returning_addr.state` and
  `web_returns` genuinely has 3,203 rows with a NULL `wr_returning_customer_sk`
  (NULL = *a real group whose key is unknown*).

The FINAL merge joins them with `is not distinct from`, which treats NULL as a
matchable value, so every unmatched address pairs with every NULL-key group.

## Why `is not distinct from` is not simply wrong here

It is the correct operator for a nullable join key in general, and the codebase
already knows the exception. The q05 rollup fix states the principle exactly:

> a grouping-set NULL is padding, not a value, so it can never find a partner on
> a side that does not pad the same key

An outer-join NULL is padding in the same sense. The rule is already implemented
for ROLLUP padding (`get_node_joins` tracks `rollup_padded_addresses`); it is not
implemented for outer-join-extended NULLs.

## Repro

```python
import sys; sys.path.insert(0, 'evals')
from pathlib import Path
from common import scoring
ws = Path('evals/tpcds_agent/results/20260813-125008_ingest/workspace')
eng = scoring.make_scoring_engine(<scratchpad copy of ws/'warehouse.duckdb'>, ws, 'tpcds')
rows = eng.execute_raw_sql(
    eng.generate_sql((ws/'query30.preql').read_text().replace('limit 100;', ';'))[-1]
).fetchall()
nulls = [r for r in rows if r[0] is None]
len(nulls)              # 6220
len(set(nulls))         # 20      <- 311 copies of each
```

Scored effect: the `nulls first` ORDER BY fills the whole LIMIT 100 with these
duplicates, so the query grades `fail` even though its 153 real rows match the
reference exactly (`AAAAAAAAADMABAAA / Sir / Rory / Smiley / ... / 3008.81` is the
reference's first row and the candidate's first non-NULL row).

## Fix direction

Do not change the preserving direction — that is the declared partial-binding
semantic and changing it has a wide blast radius. Make a **padding NULL
non-matchable** on the merge key: a NULL introduced by outer-join extension is
absence, not a value, so it must not satisfy `is not distinct from` against a
NULL that came from a real group. Extend the existing `rollup_padded_addresses`
machinery in `get_node_joins` / `get_join_type` to outer-extended keys, or emit
`a = b` rather than `a is not distinct from b` when one side's NULLs are
padding.

Needs a row-asserting test and a corpus A/B — `is not distinct from` is emitted
broadly, so this touches more than q30.
