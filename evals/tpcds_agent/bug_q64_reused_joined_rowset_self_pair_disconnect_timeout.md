# q64: reusing a joined rowset in a self-pair disconnects or explodes the plan

**Status:** FIXED 2026-07-15
**Classification:** FRAMEWORK BUG (resolver failure and execution hang)  
**Run:** `evals/tpcds_agent/results/20260715-033011_enriched`  
**Impact:** q64 consumed 1,821,642 tokens. A valid rowset composition timed out
after 600 seconds; a reduced form currently fails discovery before SQL generation.

## Fix

Root cause was narrower than the disconnect symptom suggested: when a rowset
body `union join`s on a projected key (`sa.item_sk = ci.item_sk`), the coalescing
join collapses `sa.item_sk` onto the canonical `ci.item_sk` and *hides* that
canonical in the body (only the collapsed side was selected). The body CTE then
stopped projecting the key column entirely, so any downstream `filtered.item_sk`
reference had no backing source — `Missing source reference to ss.item.sk` (on
HEAD), or repeated re-discovery of the shared aggregate in the full query (the
23-join / >600s timeout).

`_unhide_referenced_body_locals` (in `rowset_node.py`) only matched a referenced
rowset content address *directly*, never the canonical that content collapsed
onto via a coalescing scoped join. Fix: also un-hide a hidden body output whose
*pseudonym* is a referenced content, so the body projects the coalesced key.
Minimal trigger is just `select filtered.item_sk` after the union join; the
per-year slice / self-pair only exercised the same gap downstream.

Regression: `tests/modeling/tpc_ds_duckdb/test_q64_reused_joined_rowset_slice.py`
(renders without sentinel, shared aggregate materialized once, executes; both
build-cache modes).

## Summary

q64 builds an item-level catalog aggregate, joins it to a store aggregate, slices
that joined rowset into 1999 and 2000 arms, and self-pairs the arms by item and
store. Each operation works independently. Reusing the joined rowset through two
year slices is unstable:

- the full agent attempt generates 37,978 characters of SQL with 23 joins and
  times out after 600 seconds;
- the generated SQL repeats the same catalog aggregate CTE (`cooperative`) in
  four joins before performing the year self-pair;
- a reduced version of the same shape now raises
  `DisconnectedConceptsException` while sourcing the first year slice;
- directly aggregating each year and joining the two arms works in 0.224 seconds.

This is not the agent's two earlier connectivity error. Those attempts used an
equality predicate in `where` without a join declaration and were correctly
rejected. The framework defect begins after the query uses explicit `union join`.

## Original artifact

- Conversation: `results/20260715-033011_enriched/agent_log.q64.conversation.txt`
- JSONL, including every overwritten candidate:
  `results/20260715-033011_enriched/agent_log.q64.jsonl`
- Final candidate: `results/20260715-033011_enriched/workspace/query64.preql`
- Canonical query: `tests/modeling/tpc_ds_duckdb/query64.preql`

The timed-out body is the `file write` tool call immediately preceding the
`timed out after 600s` result in the JSONL. It is 5,148 characters / 149 lines.

## Reduced reproduction

Run against the q64 eval workspace database:

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;

with ci as
select
    cs.item.sk as item_sk,
    sum(cs.ext_list_price) as amount;

with sa as
select
    ss.item.sk as item_sk,
    ss.store.sk as store_sk,
    ss.date.year as yr,
    count(ss.line_item) as n;

with filtered as
select
    sa.item_sk,
    sa.store_sk,
    sa.yr,
    sa.n
union join sa.item_sk = ci.item_sk
having ci.amount is not null;

with y99 as
where filtered.yr = 1999
select filtered.item_sk, filtered.store_sk, filtered.n;

with y00 as
where filtered.yr = 2000
select filtered.item_sk, filtered.store_sk, filtered.n;

select y99.item_sk, y99.store_sk, y99.n, y00.n
union join y99.item_sk = y00.item_sk
union join y99.store_sk = y00.store_sk
where y00.item_sk is not null;
```

Current result:

```text
DisconnectedConceptsException: couldn't source all these concepts into one
query; ... {y99.filtered.sa.n, y99.item_sk, y99.store_sk}
```

The stack reaches:

- `trilogy/core/processing/node_generators/rowset_node.py::_enrich_rowset_node`
- `rowset_node.py::_enrich_via_group_mate_keys`
- `trilogy/core/processing/discovery_utility.py::get_loop_iteration_targets`
- `discovery_utility.py::get_priority_concept`, which raises the disconnect

## Passing control

Remove the shared joined parent and define the year aggregates directly:

```trilogy
import raw.store_sales as ss;

with y99 as
where ss.date.year = 1999
select ss.item.sk as item_sk, ss.store.sk as store_sk,
       count(ss.line_item) as n99;

with y00 as
where ss.date.year = 2000
select ss.item.sk as item_sk, ss.store.sk as store_sk,
       count(ss.line_item) as n00;

select y99.item_sk, y99.store_sk, y99.n99, y00.n00
union join y99.item_sk = y00.item_sk
union join y99.store_sk = y00.store_sk
where y00.item_sk is not null and y00.store_sk is not null;
```

Observed: SQL generation succeeds, the two equalities become one composite join
condition, and execution returns 54,858 rows in 0.224 seconds.

## Trigger matrix

| Shape | Result |
|---|---|
| Direct 1999 aggregate + direct 2000 aggregate + composite join | Pass, 0.224s |
| Catalog aggregate joined once to store aggregate | Pass, ~0.4s in agent run |
| Shared joined rowset sliced once | Pass |
| Shared joined rowset sliced twice, then self-paired | Disconnect in reduced repro |
| Full descriptive shared rowset sliced twice, then self-paired | 23-join SQL; >600s timeout |
| Canonical sk-only per-year rowsets with explicit enrichment joins | Pass |

## Likely root cause

Rowset enrichment does not preserve a stable, independently reusable key graph
when a rowset already containing a join is referenced by multiple descendant
rowsets. During discovery, group-mate enrichment loses the route from the sliced
rowset's keys to its inherited aggregate. In the larger query, discovery instead
materializes the shared lineage repeatedly, producing duplicate joins to the
same aggregate CTE and a catastrophic plan.

The fix locus is rowset reuse/enrichment in `rowset_node.py`, particularly
`_enrich_rowset_node` and `_enrich_via_group_mate_keys`, plus loop-target
selection in `discovery_utility.py`. Add a regression covering both SQL shape
(the shared joined parent is not duplicated) and execution.

