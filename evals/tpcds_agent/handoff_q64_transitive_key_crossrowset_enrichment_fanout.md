# HANDOFF — q64: 2-hop transitive-property cross-rowset enrichment fans out (incomplete coalescing-join fix)

**Status:** OPEN, ready to implement. Root-caused, minimal repro + trigger matrix confirmed.
**Full diagnosis:** `evals/tpcds_agent/bug_q64_customer_address_crossrowset_enrichment_fanout.md`
**Classification:** genuine framework **silent** wrong-result bug (dup rows, no error). INCOMPLETE
part of the 2026-07-03 coalescing-join subordinate-key fix. NOT a regression.

## Scope note (read first)
This handoff is for the **framework defect only** (defect #2 in the bug report). The q64 *eval
failure itself* was an agent column-choice slip (`ext_wholesale_cost`/`ext_list_price`
line-extended vs per-unit `wholesale_cost`/`list_price`) — that is a guidance/model issue, NOT
in scope here and NOT to be "fixed" in code. Fixing this framework bug will not by itself flip
the q64 eval to pass; it removes a latent fan-out that any agent following the canonical
"aggregate on id keys, enrich descriptive text later via cross-rowset coalescing join" strategy
will hit.

## Symptom
The canonical q64 strategy — per-year aggregates keyed on ids, then enrich descriptive TEXT via
a cross-rowset coalescing `full join` — FANS OUT to duplicate rows (all values correct, just
repeated) when the enriched key is a **2-hop transitive property** reached through a chained key
(`store_sales → customer → address`). Direct-FK enrichment is clean.

## Minimal repro
```python
import sys; sys.path.insert(0,'evals'); from common import scoring; from pathlib import Path
ws=Path('evals/tpcds_agent/results/repeat_q64_20260704-144003_enriched/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
```
Canonical `query64.preql` adapted to `raw.*` imports → **9 rows** (reference row1 ×5, row2 ×4);
`PRAGMA tpcds(64)` → 2 rows. (Adapted body / worker finals under
`.../repeat_q64_20260704-144003_enriched/workspace/_worker_*/query64.preql`.)

## Trigger matrix (toggle one enrichment join; base = both per-year aggs + item+store enrichment)
| variant | enrichment joins present | rows |
|---|---|---|
| V0 | item + store only | **2** ✅ |
| V1 | item + store + `ss.sale_address.id` (direct FK) | **2** ✅ |
| V2 | item + store + `ss.customer.address.id` (2-hop via customer) | **9** ✗ fan-out |

Fan-out is caused **solely** by enriching a text property reached through a chained key
(`full join agg_99.c_addr_99 = ss.customer.address.id`). Direct-FK `sale_address` enriches 1:1.

Model-shape dependent: the SAME canonical query PASSES the suite
(`pytest tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_four` → 2 rows) because the
test's `store_sales`/`customer` models don't trigger it; it fans out on the ingested `raw.*`
models. **Reproduce against `raw.*` models**, not the test models.

## Root cause
To project a transitive descriptive property (`ss.customer.address.city`) whose coalesced join
key is `ss.customer.address.id`, the planner walks **back through the base `store_sales` fact**
(grain item×ticket) instead of collapsing to the **address grain**. One aggregate row then
matches every base line whose customer's current address = that id → multiplicity = #base rows
sharing the address (5 and 4 here). The direct-FK `sale_address` path projects the property
straight off the address dimension by id (1:1), so no fan-out.

The 2026-07-03 coalescing-join fix makes the *coalesced key itself* referenceable across the
rowset boundary but does not constrain how a **transitive property hanging off that key** is
sourced:
- `trilogy/core/processing/node_generators/rowset_node.py:322` `_expose_coalesced_key_contents`
- `trilogy/core/processing/node_generators/merge_node.py:311` `_inject_scoped_join_key_exposure`
- `trilogy/core/build.py:2473` `_scoped_join_key_groups`

## Fix direction
The transitive property must be sourced by **collapsing to the property's own grain (the address
dimension), keyed by the coalesced join key**, rather than re-entering the base fact. Concretely,
when a coalesced/exposed join key is itself a chained key (`X → A → B`, key `A.B.id`) and a
descriptive property `A.B.<prop>` is projected across the rowset boundary, source `<prop>` from
the `A.B` grain by `A.B.id` (a 1:1 dimension lookup) instead of from the base `X` fact.

Start at `_expose_coalesced_key_contents` (`rowset_node.py:322`): today it exposes the key
contents; it likely needs to also expose (or force sourcing of) any projected transitive property
**at the key's grain**, so the downstream enrichment join is 1:1. Compare the sourcing plan of
the V1 (direct-FK, clean) vs V2 (2-hop, fan-out) cases to see where V2's property acquires the
base-fact grain instead of the address grain, and constrain it to the latter.

Guardrail: the direct-FK path (V1) already sources at the dimension grain — do not change it.
Only the multi-hop transitive case needs the grain correction.

## Test to add
Add a scoring/codegen test on the **ingested `raw.*` model shape** (the test-model shape does not
trigger it, so a test on the existing `tpc_ds_duckdb` models would pass vacuously). Options:
- A focused engine test that builds the V2 minimal body against a raw-shaped fixture and asserts
  2 rows (no fan-out), plus V0/V1 stay at 2 rows.
- If a raw-shaped fixture is heavy, add a synthetic 2-hop-transitive fixture (fact → dim1 → dim2)
  reproducing the "aggregate on id keys, enrich a dim2 text property via cross-rowset coalescing
  full join" shape, asserting 1:1 enrichment.

## Acceptance criteria
- V2 minimal repro returns 2 rows (no duplicate fan-out); all values still correct.
- V0 + V1 still return 2 rows.
- `test_sixty_four` still passes.
- No regression in the coalescing-join / rowset-boundary suites
  (`tests/join_matrix/`, the scoped-join and rowset-aliasing matrices).
- `ruff check . --fix && mypy trilogy && black .` clean.

## Do NOT
- Do NOT touch the agents' `ext_*` column choice — out of scope (guidance/model, not code).
- Do NOT change the direct-FK (V1) sourcing path.
- Do NOT expect this to flip the q64 eval to pass on its own.
