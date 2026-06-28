# q64 self-pair resolution churn (925k tokens) — NOT a framework bug; cross-import disconnection + agent modeling

**Run:** `evals/tpcds_agent/results/20260628-194910/agent_log.q64.jsonl` (HIGH, ~925k tokens).
**Status:** Investigated 2026-06-28. **No framework bug.** Old `INVALID_REFERENCE_BUG`
sentinel confirmed FIXED. The churn is a mix of (b) a correct-by-design cross-import
disconnection whose error never points at the conformed-source idiom, and (c) ordinary
agent modeling mistakes. The agent eventually succeeded (runs at log lines 75/81/84 exit 0).

## Symptom

q64 = catalog-eligible items (cumulative cat list-price > 2× cumulative cat refund) with a
store sale + matching store return, aggregated over many dims, then self-paired across
sale-year 1999 vs 2000. The agent thrashed across two resolution errors (plus 2 parse errors
and 1 undefined-concept error) before landing on the canonical self-pair-via-scoped-join idiom.

## Failing constructs + exact errors

### Error A (log line 39) — agent modeling (c), self-corrected
Write (line 35) expressed the store_sales↔store_returns join as **WHERE column equality**:
```
where ... and sr.item.id = ss.item.id and sr.ticket_number = ss.ticket_number ...
```
→ `Discovery error: cannot merge all concepts into one connected query ... split into 2
disconnected subgraphs: {<all ss.* concepts>}; {sr.item.id, sr.ticket_number}. Are you
missing a join or merge statement to relate them?`

Cause: Trilogy does **not** treat a WHERE `col = col` as a join (TPC-DS SQL idiom). `sr` is a
separate model, so its keys stay disconnected. The agent self-corrected in the next write
(line 41) by switching to `inner join ss.item.id = sr.item.id`. Working as designed; error is
accurate and helpful.

### Error B (log line 45) — correct-by-design cross-import disconnection (b), the real churn driver
After fixing the store-returns join, write (line 41) modeled the catalog leg with **two
separate imports** of the catalog facts:
```
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
auto cat_list_by_item   <- sum(cs.ext_list_price) by cs.item.id;
auto cat_refund_by_item <- sum(coalesce(cr.refunded_cash,0)+...) by cr.item.id;
auto qualifying_item    <- cs.item.id ? cat_list_by_item > 2 * cat_refund_by_item;
```
→ `Discovery error: couldn't source all these concepts into one query; you may need a join or
merge to relate them across models. Sourced individually but not joinable from model:
{cs.item.id, local.cat_list_by_item, local.cat_refund_by_item}`

Cause: `cs.item.id` (from `catalog_sales`) and `cr.item.id` (from `catalog_returns`) are
**distinct concepts in distinct namespaces** with no defined relationship at the item grain
(they relate only at the line grain order_number+item). `cat_list_by_item` is grain `cs.item.id`,
`cat_refund_by_item` is grain `cr.item.id`; comparing them needs an explicit merge/conformed
source. The planner correctly refuses. Same family as the q02/q97 cross-import disconnections.

### Other agent errors (c)
- Lines 51/54: `Syntax [104]` — agent put `auto`/`rowset` definitions **after** a where/select
  block. Friendly error already exists. Agent error.
- Line 66: `Undefined concept: agg_rows_1999.item_id` (with good suggestions incl.
  `qualifying_items.item_id`) — agent referenced a rowset member it never selected. Agent error.

## Minimal repro (Error B) — `evals/tpcds_agent/results/20260628-194910/workspace`
```python
from pathlib import Path
from trilogy import Environment, Dialects
env = Environment(working_path=Path(".../workspace"))
ex  = Dialects.DUCK_DB.default_executor(environment=env)
ex.generate_sql('''
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
auto cat_list_by_item   <- sum(cs.ext_list_price) by cs.item.id;
auto cat_refund_by_item <- sum(coalesce(cr.refunded_cash,0)) by cr.item.id;
auto qualifying_item    <- cs.item.id ? cat_list_by_item > 2 * cat_refund_by_item;
select qualifying_item;
''')   # raises DisconnectedConceptsException (couldn't source all these concepts)
```
Trigger matrix (same workspace):
| variant | result |
|---|---|
| two imports `cs` + `cr`, compare per-item aggregates | **DisconnectedConceptsException** (correct) |
| just `select cs.item.id, cat_list_by_item, cat_refund_by_item` | DisconnectedConcepts: `{cs.item.id, cat_list_by_item}; {cat_refund_by_item}` (correct) |
| **conformed**: reach catalog_sales through `cr.sales` (single item grain) | **clean SQL** |

The conformed form is exactly the canonical idiom
(`tests/modeling/tpc_ds_duckdb/query64.preql` lines 14-23: `sum(cr.sales.ext_list_price) by
cr.sales.item.id`). It generates valid SQL on the current engine.

## Classification

**(b) correct-by-design disconnection with an unhelpful error** is the dominant churn driver
(Error B), wrapped in **(c) agent modeling mistakes** (Error A WHERE-as-join, the `[104]`
parse errors, the undefined-concept ref). **There is no framework planner bug.** The planner
correctly rejects relating two independent imports of the catalog facts at item grain, and the
single-conformed-source idiom (`cr.sales`) works.

## Old sentinel: FIXED (verified)
The earlier `INVALID_REFERENCE_BUG as eligible_items_*` defect
(`bug_q64_invalid_reference_eligible_items_membership.md`) is **gone**. Re-running its exact
minimal repro (two per-item agg CTEs, inner-joined, HAVING comparing the two aggregates,
projecting the sales-side id) now returns clean SQL with **no `INVALID_REFERENCE_BUG`** — the
`_grain_key_membership_redirect` fix (`trilogy/dialect/base.py`) is effective. Canonical
`query64.preql` also generates a single clean SQL statement.

## Root cause locus (not a bug, for reference)
The disconnection is emitted by Trilogy's discovery/merge layer — grep
`couldn't source all these concepts` / `cannot merge all concepts` in `trilogy/` (discovery
utility). The behavior is correct; only the **message** is generic.

## Recommended action (no code fix to planner)
1. Do **not** change the planner — the disconnection is correct.
2. Consider enriching the cross-import disconnection message (or `agent-info`) with the
   conformed-source hint for sibling facts: "to relate `catalog_sales` and `catalog_returns`
   per item, reach one through the other (`import catalog_returns as cr; ... cr.sales.item.id`)
   or add a `merge`." This is the same documentation gap flagged for q02/q97 and is the
   highest-leverage way to cut the q64 token churn.
3. The successful path the agent eventually used matches the canonical structure (per-year
   `agg_1999`/`agg_2000` rowsets self-paired via scoped `inner join`), confirming the idiom is
   reachable — the cost was discovery, not capability.
