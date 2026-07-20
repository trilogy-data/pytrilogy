# Bug: q74 — pruned unique join key silently re-correlated on non-unique name columns

**Status:** OPEN — confirmed framework bug (silent wrong result, no error)
**Class:** SILENT wrong-result (loud detector: ingest eval fail, enriched pass)
**Leg:** ingest (auto-ingested model); enriched passes
**Severity:** high — a join key the *authored* query carried is dropped from an
intermediate CTE, and a downstream join falls back to a non-unique column
(`first_name, last_name`) with no authored basis. Wrong rows, no error.

## Symptom

TPC-DS q74 (year-over-year growth: customers whose web-sales 2002/2001 ratio
exceeds their store-sales 2002/2001 ratio) returns the wrong customer set.
Reference = 92 rows; generated query = 100 rows (LIMIT-truncated), 65 rows
present only in the candidate, 57 only in the reference.

Root observation: a customer's web totals fan out to **every other customer
sharing the same `(first_name, last_name)`**. The TPC-DS customer table has
**7,079** `(first_name, last_name)` groups spanning more than one distinct
`customer_id`, so the fault is live and large.

## The authored query is correct

`evals/tpcds_agent/results/20260717-173332_ingest/workspace/query74.preql`

The two per-customer conditional-sum CTEs are grouped by the **unique surrogate
key** and the final join is on that key:

```
with st as ... select ss.customer.customer_sk as csk, ss.customer.customer_id as cid,
                      ss.customer.first_name as fn, ss.customer.last_name as ln, ... ;
with wb as ... select ws.bill_customer.customer_sk as csk, ... ;

where st.cid is not null
select st.cid as customer_code, st.fn as first_name, st.ln as last_name
  subset join st.csk = wb.csk      -- <-- joins on the UNIQUE surrogate key
  subset join st.yr = wb.yr
having ... (ratio comparison over year-conditional sums) ...
```

The author never joins on name anywhere. `csk`/`cid` are present in both CTEs.

## The generated SQL drops the key and re-correlates on name

Reproduce (venv python):

```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path
from common import scoring
ws=Path('evals/tpcds_agent/results/20260717-173332_ingest/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
print(eng.generate_sql(open(ws/'query74.preql').read())[-1])
```

Three self-joins appear in the generated SQL (CTE alias names below are the
engine's randomized adjectives — they vary run to run; the **structure** is
stable):

1. **Correct** — web sums attached to store sums on the surrogate key:
   ```
   INNER JOIN "questionable" ON "late"."wb_csk" is not distinct from "questionable"."st_csk"
                            AND "late"."wb_yr"  is not distinct from "questionable"."st_yr"
   ```
2. **Offending** — the web conditional-sum CTE is re-attached to the identity
   CTE on **name only**, because the intermediate CTE projected away
   `st_csk`/`st_cid` and kept only `st_fn`/`st_ln`:
   ```
   FROM "scrawny"
   INNER JOIN "questionable" ON "scrawny"."st_fn" is not distinct from "questionable"."st_fn"
                            AND "scrawny"."st_ln" is not distinct from "questionable"."st_ln"
   WHERE "questionable"."st_cid" is not null
   ```
3. Final ratio comparison re-joins on `st_cid` **plus** name (name redundant
   here, but shows the identity key *was* available and simply wasn't carried
   into join #2).

The unique key exists upstream (join #1 uses `st_csk`) but is pruned from the
projection of the intermediate aggregation CTE, so join #2 — which needs to
re-associate the web aggregate with a customer identity — silently falls back to
the only surviving correlator, `(st_fn, st_ln)`, via `IS NOT DISTINCT FROM`.

## Causal isolation (proof it is THIS join, nothing else)

Editing only the generated SQL — carry `st_cid` through the intermediate CTE and
key join #2 on `st_cid` instead of `(st_fn, st_ln)` — makes the output
**byte-identical to the reference: 92 rows, 0 diff**. No other change. This
isolates the name-based re-correlation as the sole cause.

## Trigger matrix

| Condition | Result |
|---|---|
| Generated SQL as-is (name re-correlation) | 100 rows, 65/57 diff vs ref — FAIL |
| Same SQL, join #2 re-keyed on `st_cid` | 92/92, 0 diff — PASS |
| Data has >1 `customer_id` per `(first_name,last_name)` | 7,079 such groups — fault is live |

## Root cause (subsystem; exact line to be pinned by fixer)

Query generation prunes CTE output columns to those "needed" downstream, but the
unique correlation key required by a *later* self-join is not counted as needed
by the intermediate aggregation node — so it is pruned, and the downstream join
condition is then built from the surviving projected columns, which include the
non-unique `(first_name, last_name)`.

Two things to check when fixing:
1. **Projection minimization** on aggregate/CTE nodes must retain any column that
   a downstream join uses as a correlation key (here the grain/identity key
   `customer_sk`/`customer_id`), not just columns that appear in a final output
   or filter.
2. **Join-condition construction** should fail loudly (or refuse to degrade)
   rather than silently substitute a non-unique available column when the
   intended key is absent from one side's projection. A join that was authored on
   `st.csk = wb.csk` must never be emitted on `(fn, ln)`.

Suggested starting points (grep): the CTE/node output-column selection during
generation and the self-join key resolution for inline-aggregate (`having` over
`? `-filtered sums) reconstructions. The trigger is specifically a **multi-stage
inline aggregate** (the `having` ratio comparison forces nested aggregation CTEs);
single-stage joins on the same key are fine (join #1 above keeps `csk`).

## Repro artifacts

- Agent query: `evals/tpcds_agent/results/20260717-173332_ingest/workspace/query74.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query74.sql`
- Enriched passes because its consolidated model + hand-authored `query74.preql`
  avoid the pruned-key path.

## Not to be confused with

This is NOT the intended NULL-matches-NULL semantics seen in q01/q17. Those are
`IS NOT DISTINCT FROM` on an authored membership/union-join or a nullable-key
grain rejoin (intended; author adds `is not null`). Here the defect is the
**choice of join columns** — a unique key silently replaced by a non-unique one —
independent of NULL handling. The `IS NOT DISTINCT FROM` operator is incidental.
