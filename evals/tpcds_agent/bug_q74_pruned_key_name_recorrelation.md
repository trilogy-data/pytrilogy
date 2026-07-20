# Bug: q74 — pruned unique join key silently re-correlated on non-unique name columns

**Status:** FIXED 2026-07-19 — rowset sourcing now follows the standard contract
across SUBSET joins. Root cause chain: rowset group-mate enrichment was
restricted to COALESCING (union) key groups, so the st rowset declined to source
the wb-side measures across the authored `subset join` and returned bare; the
wb-side filter concept was then sourced by a separate loop iteration that (by
the attempted-exclusion rule) was never offered the already-sourced customer id;
the loop's completion fuse joined the two siblings on the only surviving overlap
— non-unique `(first_name, last_name)`. Fix (two pieces):
1. `_relation_key_group_mates` (node_generators/rowset_node.py): subset-group
   members the AUTHOR never references (`statement_authored_addresses` — the
   statement-level set, so the gate cannot flip between loop iterations; a
   per-request gate did, replanning q44 into a disconnect) are pure
   correlation keys, so enrichment sources the other side's member + remaining
   concepts and merges over the declared relation locally — one complete node
   per sourcing, restoring the loop's single-sourcing contract.
   Author-referenced members (q44's projected rank alias, q35's
   OR-of-member-null-tests) keep deferring to the outer loop's presence-probe
   machinery — enriching would fuse them onto the anchor, never NULL. Blocking
   a group only stops mates being REQUESTED; value-used members both sides
   expose anyway (q74's year inside `? yr = 2001`) still pair in join
   inference by pseudonym.
2. `_relation_keys_fully_covered` (rowset_node.py): the local merge must carry
   EVERY authored key group binding the two scopes (raw
   `scoped_join_key_groups`, since derived subset keys leave no
   distinct-identity members) — a member on the sourced side must be a
   requested mate or already carried by the value request; otherwise decline
   to the outer path. A partial key set under-correlates (composite
   `subset join p = p and r = r` joined on region alone; mixed derived+plain
   fanned out on the underived period).
3. `_prune_subsumed_stack_nodes` (concept_strategies_v3.py): loop completion
   drops a stack node that a sibling directly extends (the sibling's own
   parent resolves to the same source and covers its outputs) — fusing the
   pair re-joined on incidental overlap, resurrecting subset-anchor rows or
   re-correlating on non-unique columns.
The earlier fix retaining row-grain keys through filter-node output narrowing
(`_row_grain_outputs`, commit `more_fixes`) is KEPT alongside the above: the
sourcing-contract fix covers the provable cases with a single-scan plan, while
grain retention keeps the fallback fuse sound when
`_relation_keys_fully_covered` declines and sourcing splits — the one topology
where this failure class could otherwise resurface. Piece 3 (the prune) is
plan hygiene / defense in depth; instrumented sweeps show no current test
depends on it for correctness.
Regression test: `tests/discovery/test_subset_rowset_enrichment_contract.py`
(minimal 3-customer model with a shared `(first_name, last_name)`). Verified the
ingest-leg `query74.preql` renders SQL returning exactly the 92 reference rows.
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
