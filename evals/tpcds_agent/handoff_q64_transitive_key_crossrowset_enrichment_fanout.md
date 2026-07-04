# HANDOFF — q64: 2-hop transitive-property cross-rowset enrichment fans out (incomplete coalescing-join fix)

**Status:** ✅ FIXED 2026-07-04. See "2026-07-04 resolution" at the bottom.
**(original) Status:** OPEN, ready to implement. Root-caused, minimal repro + trigger matrix confirmed.
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

## 2026-07-04 addendum — repro re-confirmed live + trigger sharpened + one lever ruled out
Still OPEN. Re-verified on current HEAD (after the q17 grain-match fix). A stripped
V0/V1/V2 repro against the `raw.*` models reproduces the relative fan-out: V0=79, V1=79,
**V2=245** (~3.1× = #customers sharing an address). Absolute counts differ from the
canonical 2/9 only because the repro drops the color/marital/cs_ui filters; the fan-out
signal is identical. Repro: `scratchpad/repro_q64.py`.

Sharpened root cause (exact trigger isolated):
- The concepts resolve **1:1 with no fan-out in isolation**: `select ss.customer.address.id,
  ss.customer.address.city` → `customer_address` alone (50000 rows, address grain). The
  customer hop is NOT inherent to the concepts.
- The fan-out trigger is **inclusion of `ss.customer.id` in the enrichment source set**:
  `select ss.customer.id, ss.customer.address.id, ss.customer.address.city` →
  `customer INNER JOIN customer_address` (100000 rows, customer grain). `ss.customer.address.id`
  has `purpose=KEY, keys={ss.customer.id}`, so wherever the coalescing-join enrichment sources
  it *with its declared grain key*, it pulls `ss.customer.id` → the `customers` scan →
  customer-population multiplicity. The V2 `wakeful` CTE is exactly
  `customer FULL JOIN customer_address ON C_CURRENT_ADDR_SK = CA_ADDRESS_SK`
  with `coalesce(CA_ADDRESS_SK, C_CURRENT_ADDR_SK)`.
- Contrast V1: `ss.sale_address.id` has `keys={ss.item.id, ss.ticket_number}` (a direct FK at
  fact grain, no intermediate ENTITY dimension), so its enrichment sources the property straight
  off `customer_address` by the address id, 1:1. Only a key routed through an intermediate
  *entity* (customer) whose own grain is coarser than the fact fans out.

Lever RULED OUT: `strategy_builder._projection_root_concepts` (line ~1367) expands each concept
to include `concept.keys`, which *looks* like the customer.id injector — but monkeypatching it to
skip the keys-expansion for a dimension grain-key (a concept that is the sole grain component of
some datasource) did NOT change V2 (still 245). So the customer.id enters through a DIFFERENT path
in the coalescing-join enrichment (the `wakeful` dimension node is not built via that root
projection). Next investigation should instrument the actual builder of the `wakeful`
`{city, address.id}` node (it is customer+customer_address, no store_sales) to find where
`ss.customer.id` / the customer-grain sourcing is injected — likely in the scoped-join key
exposure / merge-completion enrichment, not the root projection.

Domain-graph lever (still the recommended fix shape): the graph already binds
`ss.customer.address.id` as the GRAIN of `customer_address` (`binding_sources` / `determines`
gives the FD `ss.customer.address.id → ss.customer.address.city` at the address population). The
enrichment of a coalesced/exposed scoped-join key should source the key + its FD-dependent
properties at the key's own dimension grain (1:1) via that binding, instead of expanding to the
key's declared `keys` (the intermediate entity). The precise injection site is not yet located.

## 2026-07-04 resolution (FIXED)

**Masking corrected:** the raw-vs-test "model difference" was a red herring — the
`store_sales`/`customer`/`address` models are BYTE-IDENTICAL between the two trees (only the
`memory.` catalog prefix differs) and the generated SQL is byte-identical across scale factors.
`test_sixty_four` masks the fan-out because it runs at **sf=0.01** (`engine_sf001`), where the
query's own filters yield **0 result rows** — a fan-out multiplier × 0 = 0, passing vacuously vs a
0-row reference. At sf=1 the real 2 rows appear, each duplicated 5× and 4× (= #customers sharing
the address) → 9 rows. So no raw fixture is needed to reproduce or guard it — the committed test
models do, at sf=1.

**Injection site located:** `inject_property_key_terminals`
(`trilogy/core/processing/node_generators/node_merge_node.py`). It force-injects a requested
property's key as a mandatory Steiner terminal WHEN that key itself has keys (a finer FK), to stop
a coarse-grain bridge fan-out. For the enrichment set `{ss.customer.address.id, city, zip,
street_*}`: `ss.customer.address.id` has `keys={ss.customer.id}`, and `ss.customer.id` (in the
store_sales context) has `keys={ss.ticket_number, ss.item.id}` — so the guard `not key.keys`
passes and it force-injects `ss.customer.id`, routing address.id through the `customers` table
(where it is a coarse non-grain FK) → `customer FULL JOIN customer_address` +
`coalesce(CA_ADDRESS_SK, C_CURRENT_ADDR_SK)` → the fan-out. But `ss.customer.address.id` is ALSO
the 1:1 GRAIN of `customer_address`; its `keys={customer.id}` is only an FK-path artifact.

**Fix:** `inject_property_key_terminals` now skips promoting the keys of any concept that is
itself a **sole datasource grain key**, via a new `DomainGraph.sole_grain_keys()` (reads the
single-determinant BINDING FDs — a 1-column datasource grain mints an FD whose determinant set is
exactly that lone address). Surgical: still forces `city → address.id` (first hop, preserves the
anti-coarse-bridge purpose), but stops `address.id → customer.id` (harmful second hop).
Generalizes to any dimension grain key wrongly promoted to its fact/entity FK.

**Guard test:** `tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_four_no_transitive_key_fanout`
(default `engine` fixture = sf=1): generated q64 → 2 rows == 2 distinct (was 9/2).

**Validated clean:** full `tpc_ds_duckdb` (107), `tpc_h` (28), `tests/join_matrix` +
`core/test_domain_graph` + all scoped-join / rowset matrices (298), rowset/processing/engine
(528); ruff + mypy (305 files) + black. Per scope note, this does NOT by itself flip the q64 eval
(the eval failure was the agent's `ext_*` column slip — out of scope).
