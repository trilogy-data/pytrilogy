# q78 token sink (1,389,210 tok, fail) — union()-rowset join false-disconnect

Run: `evals/tpcds_agent/results/20260708-135136_enriched` (executed ~09:51–10:25 Jul-8,
engine at/around commit `88f182edb`). q78 = store vs web+catalog per-(year,item,customer)
never-returned sales ratio.

## Symptom
Agent log shows the same **Resolution / Discovery error 4×** (messages 30, 36, 42, 46):

```
Resolution error in query78.preql: Discovery error: cannot merge all concepts into
one connected query (statement at line 72). The requested concepts split into 2
disconnected subgraphs:
  {_combined_other_cust_id, _combined_other_item_sk, _combined_other_qty,
   _combined_other_sprice, _combined_other_wcost, _combined_other_yr};
  {store_agg.cust_id, store_agg.item_sk, store_agg.store_qty,
   store_agg.store_unit_sprice, store_agg.store_unit_wcost, store_agg.yr}.
Are you missing a join or merge statement to relate them?
```

The query DID contain the joins that relate the two groups:
```
union join store_agg.yr      = combined_other.yr
union join store_agg.item_sk = combined_other.item_sk
union join store_agg.cust_id = combined_other.cust_id
```
where `combined_other` is a `union(...) -> (yr,item_sk,cust_id,qty,wcost,sprice)` rowset
stacking `web_agg` + `catalog_agg`. Discovery reported the two sides as disconnected even
though a join edge per key was declared — a **framework false-disconnect** of the
"union-join-onto-rowset" family (cf. q14 / q59 / q84). This churn (agent flailing through
`union join` → reorder clauses → `left join` → drop the union entirely) is what burned
1.39M tokens.

## Reproduction / trigger matrix (current HEAD `d8cc3d1a7`, workspace model + duckdb)
Harness: `make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')` over the run's own
`workspace/` (raw model files mtime 09:51:37, stable throughout the run).

| variant | eval result | current HEAD |
|---|---|---|
| `union join` store_agg = combined_other(union rowset), filter in HAVING (msg39) | Discovery disconnect | **GEN-OK, executes, 100 rows** |
| same, `left join` alias (msg43) | Discovery disconnect | GEN-OK |
| same, filter `combined_other.qty>0` in WHERE | (not tried) | GEN-OK |
| direct 6-way `left join` to web_agg + catalog_agg (final workspace file) | ran, 207 rows | GEN-OK, 207 rows |
| canonical `tests/.../query78.preql` (`all_sales` model) | n/a | `test_seventy_eight` PASSES |

The msg39 union-join query, run on HEAD, returns other_qty `87,65,39,26,81` for items
`119,415,439,565,577` — **identical to the reference SQL** (`query78.sql` executed on the
same db). Re-run 3× through the real `trilogy run` CLI: clean every time (deterministic,
not order-jitter).

**Verdict: the union()-rowset join false-disconnect was a genuine framework bug, and it no
longer reproduces on the current source tree.** No code commit lands after the eval's
engine state except doc-only `d8cc3d1a7` ("remove_fixes", zero `.py` changes), so the fix
was in the working tree the eval narrowly missed (last code commit `88f182edb` 09:51:58,
q78 ran ~10:1x against that or an immediately-prior state). Either way HEAD is clean.

## Root cause (where it lives)
Discovery connectivity is decided by `disconnected_components(environment, concepts, g,
island_rowsets=...)` in `trilogy/core/processing/discovery_utility.py` (the
`>1 subgraph → DisconnectedConceptsException` gate at lines 696–705 and 749–759; message
built by `format_disconnected_subgraphs_error`, line 817). A scoped join must register a
reference-graph edge between the two key concepts so they land in one component; the bug
was that a `union(...)`-derived rowset's output concepts (rendered `_combined_other_*`,
`VIRTUAL_CONCEPT_PREFIX`) did not receive the join edge from the `union join
store_agg.k = combined_other.k` clause, so the store component and the union-output
component stayed split. This is the same edge-registration gap fixed previously for
q14/q59; it now resolves for the union-rowset case as well.

## Residual (NOT framework) — why q78 still scored fail
The agent's final workspace query runs and its year/item/ratio/store_qty/store-unit
columns match the reference row-for-row, but two columns diverge — both agent modeling
choices, not engine faults:
1. **customer identifier**: reference outputs `ss_customer_sk` (surrogate int, e.g.
   `51419`); agent projected `ss.customer.id` (business key string
   `AAAAAAAALNIMAAAA`). Same customer, wrong column → value mismatch. (`.id` vs `.sk`
   ambiguity in the renamed enriched model — a guidance nit, not a bug.)
2. **other-channel wholesale/sales price**: reference `other_chan_wholesale_cost =
   sum(ws_wholesale_cost)+sum(cs_wholesale_cost)` (per-unit column summed); agent summed
   **`ext_wholesale_cost` / `ext_sales_price`** (extended = per-unit×qty) → values ~qty×
   too large (ref `25.62` vs agent `2228.94`). Agent picked the wrong measure column.

Neither residual is a framework obstacle; both are recoverable with correct column
choices, and the correct path (union rowset OR direct dual join) now builds and executes.

## Classification
- **Framework false-disconnect (union-rowset join): REAL bug, already fixed on HEAD.**
  Guard opportunity: add a `tests/join_matrix` cell = plain rowset `union join`ed onto a
  `union(...)`-derived rowset on a composite key (this exact shape) so the fix stays
  pinned. No new fix needed.
- Residual scoring fail = agent modeling (`.id` vs `.sk`; `ext_` vs plain measure) +
  minor guidance ambiguity on which customer id to report. Not framework.
