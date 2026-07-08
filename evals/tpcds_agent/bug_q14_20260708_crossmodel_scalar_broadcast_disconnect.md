# q14 (1,294,828 tok, fail) — cross-model single-row scalar broadcast false-disconnect

Run: `evals/tpcds_agent/results/20260708-135136_enriched`. Investigated READ-ONLY on
current HEAD (branch `more_bugfixes`). No engine/test edits.

## Verdict

REAL framework bug (NOT the q78 union-join-onto-rowset false-disconnect family, and NOT
already fixed on HEAD). A grainless single-row scalar aggregate (e.g. an "overall
average") that resolves into a **different model component** than the query's grained
outputs cannot be broadcast (cross-joined) onto them. v3 discovery
(`search_concepts`) fails to build a plan and the query dies with a
`DisconnectedConceptsException`, even though single-row/constant concepts are documented
as freely cross-joinable. This is the q14/q44 "overall avg is a scalar" family.

The canonical passes because it imports the **unified `all_sales`** model, so its
`avg_sales` scalar and the per-channel rowset share one component (cross-join within a
single model works). The agent instead imported the three separate channel models
(`raw.store_sales`, `raw.catalog_sales`, `raw.web_sales`) and needed a cross-channel
average — which is exactly the unsupported cross-model broadcast. It hit the disconnect
on every attempt (8 tool errors, 13 `run`s), finally gave up and **hard-coded** the
threshold `4305.507157965538`. That workaround runs but is scored `fail` (see below).

Canonical `tests/modeling/tpc_ds_duckdb/query14.{sql,preql}` builds and matches the
reference on HEAD: `pytest ...::test_fourteen` → 1 passed.

## Minimal repro (workspace engine)

FAILS — grained output from model A + single-row scalar from model B:
```
import raw.store_sales as store;
import raw.web_sales as web;
auto g <- sum(web.quantity*web.list_price);          # Granularity.SINGLE_ROW, Grain<Abstract>
select store.item.brand_id, sum(store.quantity*store.list_price) as ts, g;
```
→ `DisconnectedConceptsException: ... split into 2 disconnected subgraphs: {g}; {ts, store.item.brand_id}.`

## Trigger matrix

| case | shape | result |
|------|-------|--------|
| two grainless scalars, 2 models, NO grouping | `select sum(store..), sum(web..)` | OK (cross-join of all-single-row query) |
| grained output + SAME-model scalar | `select store.brand_id, sum(store..) ts, g(=sum(store..))` | OK |
| grained output + FOREIGN single-model scalar | `... g(=sum(web..))` | **FAIL** (disconnect `{g};{ts,brand_id}`) |
| grained output + CROSS-model scalar | `... g(=sum(store..)+sum(web..))` | **FAIL** (terminal `couldn't source ... not joinable`) |
| canonical unified `all_sales` avg in rowset HAVING | one model | OK |

The discriminator is purely single-vs-cross model, given a grained output. When the whole
query is grainless the scalars cross-join fine; add one grouping key from another model
and the foreign single-row scalar can no longer be broadcast.

Agent's two live errors both reduce to this:
- attempt 1 (`auto overall_avg_sale <- sum(store..)+sum(catalog..)+sum(web..) / ...`):
  `... 2 disconnected subgraphs: {channel_groups.*}; {_virt_agg_sum_*}` (pre/diagnostic).
- attempt 2 (`rowset overall_stats <- where store.. or catalog.. or web.. select (sum)/(sum)`):
  `Could not resolve connections for query with output [_overall_stats_total_all, _overall_stats_count_all]` (terminal).

## Root cause (file:line)

- Raised at `trilogy/core/query_processor.py:816`, re-raising from
  `source_query_concepts` (`trilogy/core/processing/concept_strategies_v3.py:677`).
- Inside `source_query_concepts`, `search_concepts` returns `None` (line ~697): the v3
  planner/merge-node builder **cannot produce a cross-join node** to broadcast a
  `SINGLE_ROW` scalar that anchors in a different weakly-connected model component than
  the grained outputs. The `disconnected_components` partition (line 710) then names the
  groups and raises at line 712 → `format_disconnected_subgraphs_error`
  (`discovery_utility.py:817`).
- The exemptions for single-row/constant concepts live only in the **diagnostic /
  relevance** code paths — `_crossjoinable` (`discovery_utility.py:542-545`, keys on
  `Granularity.SINGLE_ROW`) and `calculate_graph_relevance`
  (`trilogy/core/processing/utility.py:69-71`). Those keep the *pre-gate* from
  false-raising, but the **plan construction** in `search_concepts` never adds the
  cross-join across model components, so discovery dead-ends and the terminal partition
  reports the disconnect anyway. So the gap is in the merge/cross-join planner, not the
  connectivity diagnostic.

All three scalars above are `Granularity.SINGLE_ROW` / `Grain<Abstract>` (verified on the
built concepts), i.e. genuine broadcast constants that *should* cross-join into any grain.

## Why the final answer is `fail` (not a second silent bug)

Agent's hard-coded-threshold query runs (100 rows). Grand total
`673,483,747.73 / 155,603` vs reference SQL on the same DB `673,409,655.64 / 155,567`
(~0.01% high; 36 extra kept groups). This is a `>`-boundary effect of the hard-coded
average differing slightly from the reference `avg(quantity*list_price)` over the unified
union — a direct consequence of the forced workaround, NOT an independent silent
wrong-result in `union(...)` + `by rollup`. The union/rollup structure is correct.

## Fix direction (NOT applied)

Make v3 discovery (`search_concepts` / the merge-node planner) treat a `SINGLE_ROW` /
`CONSTANT` concept that resolves in a foreign model component as a broadcastable
cross-join input (mirror the `_crossjoinable` / `calculate_graph_relevance` exemption in
the actual plan builder), so a grainless scalar can be attached to a grained output
across model boundaries. Guard with the grained+foreign-scalar snippet above.
