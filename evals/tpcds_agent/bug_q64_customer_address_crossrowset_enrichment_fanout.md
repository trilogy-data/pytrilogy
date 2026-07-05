# q64 token sink (410k–1.03M tok, 3/3 fail) — investigation

Run: `evals/tpcds_agent/results/repeat_q64_20260704-144003_enriched` (3 reps, all
FAIL "result set differs from reference"), plus crashed top-10 rerun
`.../20260704-140355`. Model deepseek-chat, sf=1. Reference = `PRAGMA tpcds(64)` = 2 rows.

## TL;DR

Two distinct things, only one of them a framework bug:

1. **The agents' actual FAIL is an agent column-choice error, NOT a framework
   bug.** All three reps' final queries return the CORRECT 2 rows with the
   correct grouping keys and counts (1,1). The *only* discrepancy vs reference is
   that the agents summed `ss.ext_wholesale_cost` / `ss.ext_list_price`
   (line-extended = per-unit × quantity) instead of `ss.wholesale_cost` /
   `ss.list_price` (per-unit). The reference SQL uses `ss_wholesale_cost` /
   `ss_list_price` (per-unit). This inflates two of the three sums by the line
   quantity (20× on row 1's 1999 line, 6× on 2000); the coupon sum, which the
   agents took as plain `coupon_amt` (matching `ss_coupon_amt`), matches the
   reference **exactly**. Substituting the plain columns makes worker_2's query
   reproduce the reference row-for-row (verified below). The framework computed
   exactly what was asked — this is a semantic slip nudged by the model exposing
   both `wholesale_cost` and `ext_wholesale_cost`.

2. **There IS a genuine framework silent-wrong-result defect in the q64 pattern
   family, but the agents dodged it.** The hand-authored canonical
   `query64.preql` strategy ("aggregate on id keys, enrich descriptive TEXT later
   via a cross-rowset coalescing `full join`") FANS OUT to 9 rows (exact
   duplicates, all values correct) on the eval workspace's `raw.*` ingested
   models, vs the correct 2 rows. Isolated to a single enrichment join:
   enriching the customer's *current-address* text (`ss.customer.address.*`, a
   2-hop transitive key) fans out; the direct-FK `ss.sale_address.*` enrichment
   does not. This is an INCOMPLETE part of the 2026-07-03 coalescing-join
   subordinate-key fix. The agents avoided it because their `union join` /
   `subset join` formulations enrich all text INSIDE the aggregating rowset
   (grouped), so there is no post-hoc enrichment join to fan out.

## Churn mechanism (why 500k–1M tokens with almost no hard errors)

r01 had only 2 tool errors in 28 iterations; r00 9 runs, r02 15 iters. The churn
is NOT a single framework wall. It is:
- No reference to self-validate — the agents could not tell their ext_-column
  answer was wrong; r02 explicitly self-assessed its 2-row output as "correct"
  and returned. (dominant cost — the model keeps rewriting a query that already
  runs and looks plausible.)
- Exploration fan-outs from under-grouped self-pairs: r00 saw row_count 10 → 247
  → 5 → 2 while toggling filters/joins. The 247-row blow-up is a self-pair on
  (item, store_name, store_zip) over a base aggregate that (at that intermediate
  stage) still had multiple rows per (item,store,year); a legit consequence of
  the agent's grouping, not an engine defect.
- Legit clean errors the agent had to work around: `rowset ... as` vs `<-` parse
  error; and the correct "2 disconnected subgraphs {cs.item.id,
  cat_ext_list_price_by_item}; {cat_refund_sum_by_item}" when it defined the
  catalog sale-sum and refund-sum as two separate `sum() by` aggregates over
  `cs` and `cr` without a join. Both are correctly reported.

## Minimal repro (framework defect #2)

Harness against the run workspace (has the DB):
```python
import sys; sys.path.insert(0,'evals'); from common import scoring
from pathlib import Path
ws=Path('evals/tpcds_agent/results/repeat_q64_20260704-144003_enriched/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
```

Canonical `query64.preql` adapted to `raw.*` imports (per-year aggregates on id
keys, then `full join agg_99.k = agg_00.k = ss.<base_key>` enrichment):
`rows(canonical)` → **9 rows** (reference row 1 ×5, row 2 ×4; every value
correct). Reference `PRAGMA tpcds(64)` → 2 rows.

### Trigger matrix (toggle one enrichment join; base = both per-year aggs + item+store enrichment)

| variant | enrichment joins present | rows |
|---|---|---|
| V0 | item + store only | **2** ✅ |
| V1 | item + store + `ss.sale_address.id` (direct FK) | **2** ✅ |
| V2 | item + store + `ss.customer.address.id` (2-hop via customer) | **9** ✗ fan-out |

So the fan-out is caused **solely** by `full join agg_99.c_addr_99 =
ss.customer.address.id` — enriching a text property reached through a chained
key (`store_sales → customer → address`). `sale_address` (a direct FK on
store_sales) enriches cleanly.

Note: the SAME canonical query PASSES in the test suite
(`pytest tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_four` → 1
passed, 2 rows). So it is model-shape-dependent — clean on the tests'
`store_sales`/`customer` models, fan-out on the ingested `raw.*` models.

### Confirmation that the agents' path is framework-correct

worker_2's final query, with `ss.ext_wholesale_cost`→`ss.wholesale_cost` and
`ss.ext_list_price`→`ss.list_price`, `rows(...)` → **2 rows matching the
reference exactly** (95.24/140.95/1488.38 … 52.89/97.31/338.38 …). All three
workers' unmodified finals already return the correct 2-row shape and counts.

## Root cause (defect #2)

The enrichment sources the transitive property `ss.customer.address.city` by
re-joining the base `store_sales` fact (grain item×ticket) rather than
collapsing to the address grain: the coalesced join key is expressed as
`ss.customer.address.id`, so to project `ss.customer.address.city` the planner
walks back through `store_sales`, and one aggregate row matches *every*
store_sales line whose customer's current address = that id → multiplicity =
#base rows sharing the address (5 and 4 here). The direct-FK `sale_address` path
projects the property straight off the address dimension by id (1:1), so no
fan-out. The coalescing-join exposure fix
(`trilogy/core/processing/node_generators/rowset_node.py:322`
`_expose_coalesced_key_contents`, + `merge_node.py:311`
`_inject_scoped_join_key_exposure`, + `build.py:2473` `_scoped_join_key_groups`)
makes the *coalesced key itself* referenceable across the rowset boundary, but
does not constrain how a **transitive descriptive property** hanging off that
key is sourced — for a 2-hop key it re-enters the base fact and fans out.

## Classification

- Defect #1 (agents' actual failure = `ext_*` vs per-unit columns):
  **agent/model error**, not framework. Clear, correctly-computed output.
- Defect #2 (customer.address cross-rowset enrichment fan-out): **genuine
  framework silent-wrong-result bug; INCOMPLETE 2026-07-03 coalescing-join
  fix** (handles direct-FK subordinate keys, not 2-hop transitive-property
  enrichment). Reproducible on the agent's own models. NOT a regression of a
  previously-passing test (the suite test still passes; it exercises the test
  models, which don't trigger it), and NOT the direct cause of this run's
  failures (the agents' `union join`/`subset join` formulation enriches inside
  the aggregate and dodges it).
- Token sink overall: dominated by the agent being unable to self-validate the
  `ext_` mistake plus exploration fan-outs from under-grouped self-pairs — a
  guidance/model-verification gap, with defect #2 latent for any agent that
  follows the canonical "aggregate keys, enrich text later" strategy.

## Files
- Canonical: `tests/modeling/tpc_ds_duckdb/query64.preql` (+ `query64.sql`)
- Suite test (passes): `tests/modeling/tpc_ds_duckdb/test_queries.py::test_sixty_four`
- Fix code touched by prior q64 work: `rowset_node.py:322`,
  `merge_node.py:311`, `build.py:2473`
- Agent finals: `.../workspace/_worker_{0,1,2}/query64.preql`
