# q25 — multi-leg scoped-join aggregate branches recombine on output grain, dropping the shared correlation key (SILENT wrong-result)

Run: `evals/tpcds_agent/results/20260720-140600` (INGEST leg, auto-ingested `workspace/raw/*.preql`).
q25 burned **1,220,559 tokens** (baseline 332,812, ~4x) and FAILS. Reference returns **1 row**; the agent's final query returns **2 rows** (one spurious).

## Business shape
Store sales in Apr 2001 that (a) have a matching store *return* by the same customer+item+ticket (return in Apr–Oct 2001) AND (b) have a matching catalog *sale* by the same billed-customer+item (sold Apr–Oct 2001). Sum ss net profit / sr net loss / cs net profit per (item, store). It is a three-way correlated join anchored on `store_sales`; the returns leg and the catalog leg are tied together by the **shared customer_sk / item_sk**.

## Symptom (silent)
The agent's final `workspace/query25.preql` uses two `union join` legs off the `ss` anchor plus `?`-filtered aggregates and a null-guard HAVING. It runs clean, no warning, returns 2 rows. The 2nd row (`item AAAAAAAAKOCAAAAA / store ought / -2738.17 / 74.72 / -88.42`) does not exist in the reference — it is a (item, store) pair where *some* store sale had a return and *some* (different) store sale had a catalog match, with **no single customer satisfying both**.

## Root cause
A single SELECT whose measures come from two different fact tables reached via two scoped-join legs (`ss ⋈ sr` and `ss ⋈ cs`) is decomposed into **one source branch per measure**, each independently grouped to the SELECT output grain, then recombined by a `MergeNode`. The merge joins the branches only on their shared **output concepts** — i.e. the projected group-by dims `(item_id, item_desc, store_id, store_name)`. The join-correlation keys established by the scoped joins (`customer_sk`, `item_sk`, `ticket_number`) were consumed *inside* each branch and collapsed by that branch's GROUP BY, so they are gone before the merge and can never become merge join keys. The three-way customer-correlated join silently degrades to two independent two-way joins glued on (item, store).

The catalog branch is even re-derived from `store_sales` **alone** (`abundant`/`questionable` CTE = store_sales grouped by customer/item/store), never intersected with the store-returns branch — see generated SQL below.

### Generated SQL (agent final), abridged
```
young      := ss FULL JOIN sr ON (cust,item,ticket) ... grouped, keeps item/store dims + item_sk/ticket
sweltering := SELECT item dims, store dims, sum(ss?), sum(sr?) FROM young GROUP BY (item,store) HAVING sr not null
questionable := store_sales GROUP BY (cust,item,date,store)           -- ss re-scanned ALONE
uneven     := questionable FULL JOIN catalog_sales ON (cust,item) ... -- cs tied to ss re-scan, NOT to sr leg
yummy      := SELECT item dims, store dims, sum(cs?) FROM uneven GROUP BY (item,store) HAVING cs not null
FINAL      := sweltering INNER JOIN yummy
                ON item_id, item_desc, store_id, store_name          -- <<< only the 4 output dims; customer dropped
```

## Minimal repro + trigger matrix
Harness: `scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')`, `ws = <run>/workspace`. Body header imports `raw.store_sales as ss; raw.store_returns as sr; raw.catalog_sales as cs;`.

| # | Form | Rows | Verdict |
|---|------|------|---------|
| ref | `tests/modeling/tpc_ds_duckdb/query25.sql` (INNER 3-way) | **1** | ground truth |
| final | agent's `query25.preql` (2 union joins, `?` sums, HAVING null) | **2** | WRONG (+1 spurious) |
| A | 2 union joins, **plain** sums, `group (item,store)` | **8** | WRONG — plain sums, defect not caused by `?` |
| B | A **+ project `ss.customer.customer_sk`, `ss.item.item_sk`** | **1** | CORRECT — projecting the correlation keys restores them as merge join keys |
| C | single leg `ss ⋈ sr` only, `group(item,store)` | 100 | sane (all valid ss/sr pairs) |
| D | same as A but `subset join` instead of `union join` | 33 | WRONG — defect is independent of union vs subset |

Key finding: **A→B** is the trigger. The moment the shared join keys appear in the SELECT they survive into each branch's grain and the merge joins on them; when they are *not* projected (the natural phrasing, since the report grain is only item+store) they are silently dropped and the legs cross-multiply. This is the load-bearing framework defect — it is not the agent's `?`/HAVING pattern (A with plain sums is equally wrong; D with subset joins is equally wrong).

## Classification
REAL FRAMEWORK BUG — silent wrong-result (no error, no warning; token bar is the only detector). Not guidance/idiom.
- Memory "membership in HAVING fully supported" / "WHERE doesn't cross-filter into inline-aggregate scope" do NOT apply — the query never needed either; the defect is in multi-leg scoped-join merge planning.
- The agent's mid-run tuple-membership attempt (`(ss.cust,ss.item,ss.ticket) in (sr.*)`) legitimately errored with `Discovery error: 3 disconnected subgraphs` (message 51) because membership `in` is filter-only and cannot also project the rowset's aggregate measure — that error is correct, but it pushed the agent onto the union-join path that then silently miscomputed.

## Root cause file:line
- `trilogy/core/processing/node_generators/select_merge_node.py:979` — `gen_select_merge_node` wraps the per-measure branches in a `MergeNode` with `output_concepts = all_concepts` (the projected outputs). No shared scoped-join correlation key is added to the merge grain when it is absent from the projection.
- `trilogy/core/processing/join_resolution.py:655` `get_node_joins` / `:698-704` — branch datasources are connected in the join graph only through their **output concept** nodes; the merge join key is the intersection of branch outputs = the 4 projected dims. Keys collapsed by each branch's pre-merge GROUP BY are not present, so the customer/item correlation cannot be enforced.
- Upstream cause: `_source_concepts_via_graph` (`select_merge_node.py:318-514`) resolves each measure's source branch and grouped it to the request grain independently; the two legs sharing the `ss` anchor are never joined at the correlated (customer,item,ticket) grain before aggregation.

Fix direction (NOT applied, read-only): when multiple scoped-join legs share an anchor, the keys used by those joins must be retained in the merge grain (or the legs joined at the correlated grain before per-measure aggregation) so the recombination enforces the shared correlation, even when those keys are not in the SELECT projection.
