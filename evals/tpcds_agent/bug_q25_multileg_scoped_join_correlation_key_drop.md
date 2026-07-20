# q25 — multi-leg scoped-join aggregate branches recombine on output grain, dropping the shared correlation key (SILENT wrong-result)

## VERDICT 2026-07-20: DISPROVEN — NOT A FRAMEWORK BUG (NULL-identity semantics, not key drop)

The engine's 2-row result is **value-exact (to the cent)** with a row-level correlated
flat-SQL oracle of the authored query — a chain of null-safe (`IS NOT DISTINCT FROM`)
joins on exactly the authored keys, aggregated at the authored output grain:

- The "spurious" row (`KOCAAAAA / ought / -2738.17 / 74.72 / -88.42`) is REAL under
  trilogy semantics: ss ticket 94126 has **NULL ss_customer_sk** and cs order 113506 has
  **NULL cs_bill_customer_sk** (net profit exactly -88.42, item 747, Sep 2001). The
  authored `ss.customer.customer_sk = cs.bill_customer.customer_sk` pairs them by the
  unified NULL-matches-NULL identity semantics; the SQL reference's plain `=` silently
  rejects the NULL pair. Same-key oracle with `=` → 1 row; with INDF → the engine's 2
  rows exactly. Form A (plain sums + HAVING) also matches its INDF oracle exactly (8/8
  rows, values to the cent). **No correlation key is dropped anywhere.**
- The trigger matrix conflated semantic query changes with planner changes:
  - **A→B is not a planner trigger** — projecting the correlation keys moves the HAVING
    guards to the finer grain, where they legitimately enforce per-customer correlation.
  - **E (WHERE equalities) is not a no-op** — `a = b` in WHERE null-rejects per standard
    ternary logic, silently dropping every NULL-key row and thereby matching the
    reference. It is a strictly narrower query than the join-clause declaration.
- **One predicate closes the whole gap**: agent-final + `where ss.customer.customer_sk
  is not null` returns the reference row exactly. SQL parity = explicit not-null, as
  documented for the membership identity semantics.

Guard test: `tests/join_matrix/test_multileg_scoped_join_null_identity.py` pins the
multi-leg branch-merge == null-safe row-correlated oracle (including the NULL-identity
row) and the not-null SQL-parity form. The root-cause analysis below is retained for
the record but its file:line attributions are NOT defects.

---

## Original report (root cause DISPROVEN above)

Run: `evals/tpcds_agent/results/20260720-140600` (INGEST leg, auto-ingested `workspace/raw/*.preql`).
q25 burned **1,220,559 tokens** (baseline 332,812, ~4x) and FAILS. Reference returns **1 row**; the agent's final query returns **2 rows** (one spurious).

## Business shape
Store sales in Apr 2001 that (a) have a matching store *return* by the same customer+item+ticket (return in Apr–Oct 2001) AND (b) have a matching catalog *sale* by the same billed-customer+item (sold Apr–Oct 2001). Sum ss net profit / sr net loss / cs net profit per (item, store). It is a three-way correlated join anchored on `store_sales`; the returns leg and the catalog leg are tied together by the **shared customer_sk / item_sk**.

## Authored query (agent final, verbatim — `workspace/query25.preql`)
```
import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where
  ss.date_dim.year = 2001
  and ss.date_dim.moy = 4
select
  ss.item.item_id as item_code,
  ss.item.item_desc as item_description,
  ss.store.store_id as store_code,
  ss.store.store_name as store_name,
  sum(ss.net_profit ? (sr.net_loss is not null and sr.date_dim.year = 2001 and sr.date_dim.moy >= 4 and sr.date_dim.moy <= 10)) as store_sale_net_profit,
  sum(sr.net_loss ? (sr.date_dim.year = 2001 and sr.date_dim.moy >= 4 and sr.date_dim.moy <= 10)) as store_return_net_loss,
  sum(cs.net_profit ? (cs.sold_date.year = 2001 and cs.sold_date.moy >= 4 and cs.sold_date.moy <= 10)) as catalog_sale_net_profit,
union join ss.ticket_number = sr.ticket_number
  and ss.item.item_sk = sr.item.item_sk
  and ss.customer.customer_sk = sr.customer.customer_sk
union join ss.customer.customer_sk = cs.bill_customer.customer_sk
  and ss.item.item_sk = cs.item.item_sk
having
  store_return_net_loss is not null
  and catalog_sale_net_profit is not null
order by
  ss.item.item_id, ss.item.item_desc, ss.store.store_id, ss.store.store_name
limit 100;
```

**The agent DID author the correlation equalities** (customer_sk / item_sk / ticket_number on
the sr leg; customer_sk / item_sk on the cs leg). They match the reference's join predicates
exactly (`ss_customer_sk=sr_customer_sk AND ss_item_sk=sr_item_sk AND ss_ticket_number=sr_ticket_number`,
`sr_customer_sk=cs_bill_customer_sk AND sr_item_sk=cs_item_sk`). So this is **not** a
"no equality specified, merge at store grain" case — the engine drops equalities that were
explicitly authored **in the join clause**.

**Decisive check — the same equalities in a `WHERE` clause are honored.** Repeating the join
keys as WHERE predicates (`... and ss.ticket_number = sr.ticket_number and ss.item.item_sk =
sr.item.item_sk and ss.customer.customer_sk = sr.customer.customer_sk and
ss.customer.customer_sk = cs.bill_customer.customer_sk and ss.item.item_sk = cs.item.item_sk`)
returns **1 row that matches the reference exactly** (`AAAAAAAABJECAAAA / able / -143.48 /
791.34 / 156.24`). Identical authored constraints, but the WHERE form correlates and the
join-clause form does not — that is the framework defect in one line, and it validates the fix
direction (the join keys must survive into the correlated/merge grain, which is what the WHERE
form forces).

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
| E | agent-final **+ the same join-key equalities repeated in `WHERE`** | **1** | CORRECT — exact reference match (`-143.48 / 791.34 / 156.24`); WHERE-form equalities are honored, join-clause equalities are not |

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
