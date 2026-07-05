# q97 — FRAMEWORK bug(s): two-rowset presence join is impossible (hard crash + silent wrong result)

> **RESOLVED 2026-07-05.** Both bugs fixed forward:
>
> - **Bug 1** (`trilogy/core/models/build.py`): a coalescing (∦ — query
>   `full`/`union` join, NOT an EQUAL `merge`) pair whose endpoints are BOTH
>   derived BASIC keys (`cast(a)=cast(b)`) now takes the identity path
>   (`_rowset_outer_pair`), so each side keeps and materializes its own key
>   column instead of one side's expression being destroyed by substitution.
>   Plus a structural backstop in `_enrich_rowset_node` (rowset_node.py): any
>   BASIC computed purely from a rowset node's own producible addresses is
>   excluded from enrichment sourcing (it could only re-enter the rowset).
> - **Bug 2** (`Factory._coalescing_presence_probe`, build.py): `member is
>   [not] null` on a coalescing key-group member now binds to a per-side
>   presence probe materialized on the member's own rowset BEFORE the merge, so
>   presence tests see each side's real NULLs. Projecting a member is unchanged
>   (coalesced group axis).
>
> Trigger matrix rows A–E all pass; the literal two-rowset translation of q97
> (rows B/D) now returns the canonical `(540709, 286686, 171)`. Guards:
> `tests/join_matrix/test_coalescing_presence_matrix.py` + the fuzzer family
> `coalescing_presence` (16/16).

TPC-DS q97 = "customers present in store-only / catalog-only / both channels" over
(customer, item) pairs in a date window. Canonical SQL = two grouped subqueries
`FULL OUTER JOIN`-ed on (customer_sk, item_sk), then `SUM(CASE WHEN a IS NOT NULL AND
b IS NULL ...)` presence counts.

Freshest run: `evals/tpcds_agent/results/20260705-002825` — status **exhausted**
(exit 2, `crash.q97.txt` = "Agent exhausted 75 iterations without returning control").

## Symptom (what exhausted the agent)

The agent translated query97.sql literally: two `rowset`s (store_set, catalog_set) over
the two separate facts, joined on the composite (customer, item) key, then presence
`SUM(CASE ...)`. It cycled 22+ `query97_check*.preql` rewrites against a **double bind**:

- Every attempt using an **expression / composite key** (`concat(...)`, `hash(...)`,
  even a bare `::string` cast) → `Resolution error: query could not be planned; this
  is a bug.` (a `RecursionError`, surfaced under that literal "this is a bug" text).
- Every attempt using **plain composite keys** wrote & ran with exit 0 but returned
  **silently wrong** counts — store_only and catalog_only always 0.

The canonical Trilogy answer (`tests/modeling/tpc_ds_duckdb/query97.preql`) sidesteps
BOTH by never joining: it imports the unified `all_sales` model and uses
`max(case when sales.channel='STORE' then order_id else 0 end)` presence flags grouped
by (customer, item). That reframe is non-obvious; the agent stuck with the SQL shape and
hit a genuine wall. **Classification: real framework bug (two of them). Not agent error.**
Canonical `query97.preql` builds and runs on the current engine: `(540709, 286686, 171)`.

## Repro harness

```python
import sys; sys.path.insert(0,'evals'); from common import scoring
from pathlib import Path
ws=Path('evals/tpcds_agent/results/20260705-002825/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
```

Common preamble (two independent rowsets + presence counts):
```
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
rowset store_set <- where store_sales.date.year = 2000
select store_sales.customer.id as store_cust_id, store_sales.item.id as store_item_id;
rowset catalog_set <- where catalog_sales.sold_date.year = 2000
select catalog_sales.bill_customer.id as cat_cust_id, catalog_sales.item.id as cat_item_id;
select
 sum(case when store_set.store_cust_id is not null and catalog_set.cat_cust_id is null then 1 else 0 end) as store_only,
 sum(case when store_set.store_cust_id is null and catalog_set.cat_cust_id is not null then 1 else 0 end) as catalog_only,
 sum(case when store_set.store_cust_id is not null and catalog_set.cat_cust_id is not null then 1 else 0 end) as both
<JOIN CLAUSE>
limit 100;
```

## Trigger matrix (toggle the `<JOIN CLAUSE>`)

| # | join clause | result |
|---|-------------|--------|
| A | `union join concat(store..::string, store..::string) = concat(cat..::string, cat..::string)` | **RecursionError** ("could not be planned; this is a bug") |
| A2 | `union join store_set.store_cust_id::string = catalog_set.cat_cust_id::string` (single **cast** key) | **RecursionError** — minimal trigger |
| A3 | `union join concat(store..::string) = concat(cat..::string)` (1-arg concat) | **RecursionError** |
| A4 | `full join concat(...)=concat(...)` (full instead of union) | **RecursionError** |
| B | `union join store_cust_id = cat_cust_id and store_item_id = cat_item_id` (plain composite) | exit 0, **`(0, 0, 827566)`** — SILENT WRONG |
| C | `union join store_cust_id = cat_cust_id` (single plain key) | plans OK |
| D | `full join <plain composite>` | exit 0, **`(0, 0, 827566)`** — SILENT WRONG |
| E | `full join <single plain key>` | plans OK |

Toggle isolation: it is the **derived expression** on the join key (any of cast/concat/hash),
not the composite-ness, that trips the recursion — a single `::string` cast is enough (A2).
And it is the **coalescing join semantics** (both `full` and `union`), not the count
expressions, that produce the silent (0,0,N).

## Bug 1 (hard — this is what drove the loop): derived-expression join key between two independent rowsets → infinite recursion

Cycle (captured, repeats ~228x until stack blows):
```
concept_strategies_v3.py:108 search_concepts
 -> discovery_node_factory.py:558 generate_node -> :256 _generate_rowset_node
 -> rowset_node.py:880 gen_rowset_node -> :773 _enrich_rowset_node
 -> discovery_node_factory.py:326 _generate_basic_node -> basic_node.py:173 gen_basic_node
 -> search_concepts ...
```
Root cause: `trilogy/core/processing/node_generators/rowset_node.py`.
The two dedicated derived-key handlers do NOT cover a cast/concat key in a **two-rowset
union/full** join, so control falls through to the **generic** enrichment at
`rowset_node.py:773`, which `source_concepts(mandatory_list=possible_joins + ...)` on the
derived `store_cust_id::string` key. That BASIC key's only input is the rowset's own
output, so sourcing it re-enters `gen_rowset_node` → `_enrich_rowset_node` → same key →
loop.
- `_materializable_derived_join_keys` (rowset_node.py:162) — would materialize the key
  locally (no re-sourcing) but is gated to `domain_graph.outer_relation_keys()` and does
  not fire for these inline cast/concat keys (nothing materialized at line 859-863).
- `_producible_derived_join_keys` (rowset_node.py:108) — the enrichment-side guard at
  line 732 that would route around the generic path also returns empty here, so the
  gate at line 733 is skipped and we reach the recursive line 773.

Same family as the prior derived-rowset-join recursion fixes (see MEMORY
`project_left_derived_rowset_join_recursion`, `project_composite_derived_join_drops_equality_key`),
but for the coalescing (`full`/`union`) two-independent-rowset case with a cast/concat key —
not covered by those.

## Bug 2 (silent wrong results): coalescing join fuses both side-keys into one column, defeating presence detection

Generated SQL for B/D (the coalescing-join CTE):
```
juicy as (
 SELECT
   coalesce("thoughtful"."catalog_set_cat_cust_id","yummy"."store_set_store_cust_id") as "catalog_set_cat_cust_id",
   coalesce("thoughtful"."catalog_set_cat_cust_id","yummy"."store_set_store_cust_id") as "store_set_store_cust_id"
 FROM "yummy" FULL JOIN "thoughtful"
   on "yummy"."store_set_store_cust_id" is not distinct from "thoughtful"."catalog_set_cat_cust_id"
  AND "yummy"."store_set_store_item_id" = "thoughtful"."catalog_set_cat_item_id")
```
Both output columns `store_set_store_cust_id` AND `catalog_set_cat_cust_id` are aliased to
the **same** `coalesce(catalog, store)`. On a store-only row (catalog side NULL) the
coalesce yields the store value for BOTH names, so `catalog_cust_id` is non-null when it
should be NULL. Therefore `store IS NOT NULL AND catalog IS NULL` is never true →
`store_only = catalog_only = 0`, `both` = everything. The coalescing-join key collapse
(domain-graph phase-3: full/subset/union collapse a join-key group onto ONE canonical
body column, other side kept only as a pseudonym — see MEMORY q64 entry) is
fundamentally incompatible with per-side NULL presence, which is the whole point of q97.

This means even if Bug 1 were fixed (plain keys plan fine, rows C/E), the presence-count
pattern the agent needs still cannot be expressed via a coalescing join — it silently
miscounts. The engine currently has no cross-rowset FULL-OUTER shape that preserves both
sides' NULLs for presence testing; the only working path is the unified-model
`max(case when channel...)` reframe used by the canonical .preql.

## Recommendations (do NOT fix here)
1. Bug 1: extend the derived-key materialization gate (`_materializable_derived_join_keys`
   / `_producible_derived_join_keys`, rowset_node.py:108/162) to cover cast/concat/hash
   join keys in the two-independent-rowset `full`/`union` case, OR add a recursion guard
   at the generic enrichment (line 773) when the sole input of a `possible_joins` member
   is a producible output of `node` itself. At minimum, turn the RecursionError into a
   clean planner error instead of "this is a bug".
2. Bug 2: either (a) a genuine presence/outer join shape that keeps each side's key column
   un-coalesced (so `side IS NULL` is testable), or (b) explicit guidance/agent-info
   steering channel-presence problems to the unified-model `max(case when ...)` idiom and
   away from cross-fact `full`/`union join`.
