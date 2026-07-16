# Bug candidate: q17 tuple membership loses the rowset connection

## Resolution (2026-07-13): claimed planner regression does NOT reproduce; diagnostic gap fixed

Investigated on HEAD (branch `agent-scope-feedback`).

**The candidate shape below plans correctly.** The isolated two-rowset
formulation (filtered `ss_ci_pairs` + `cs_stats` with composite tuple
membership) generates a proper composite semi-join against the real eval
workspace model — and in the trajectory itself it RAN successfully (events
90–100 return rows). There is no regression: tuple membership across facts,
with a filtered source rowset and downstream aggregation, works and is now
locked in by regression tests (`tests/engine/test_duckdb_tuple_membership.py`,
`test_cross_fact_*`).

**The disconnected-graph error at event 89 came from a different query** (the
write at event 84): a SINGLE select blending ss-grain outputs
(`ss.item.id/desc`, `ss.store.state`, ss aggregates) with catalog aggregates,
related only by the membership predicate. That disconnect is semantically
correct: a membership semi-join filters its left side; it does not attribute
catalog rows to store dimensions, so `count(cs.quantity)` at ss grain has no
join path (consistent with the earlier
`bug_q17_model_discovery_idiom_no_framework_bug.md` verdict). The null
`cs_stats` columns in the successful run are the agent's item-only final join,
not a framework wrong-result.

**Confirmed framework defect (fixed): the diagnostic.** The membership
predicate's right side plans as a separate existence island, so its concepts
appeared in NEITHER reported subgraph — the error read as if the authored
predicate had been silently dropped, which is exactly what misled the agent
(and this report). Investigation step 6 implemented: the disconnected-subgraph
error now detects membership predicates spanning the reported components and
appends a note naming the predicate and explaining that membership filters but
does not join, on both v3 and v4 discovery paths
(`trilogy/core/processing/discovery_utility.py::membership_span_note`).

## Summary

The latest enriched q17 trajectory contains both agent mistakes and a narrower
framework regression candidate. An early query requested independent store and
catalog aggregates without a join; Trilogy correctly diagnosed disconnected
subgraphs. A later query explicitly projected eligible `(customer_sk, item_sk)`
pairs and used tuple membership to filter catalog sales, but discovery still
split the catalog aggregate from the store-derived pair rowset.

This report is intentionally limited to that later formulation. It does not
classify every q17 failure as a framework bug.

## Artifacts

- Run: `evals/tpcds_agent/results/enriched_scope_v2_full_20260713-173444`
- Trajectory: `agent_log.q17.jsonl`
- Final candidate: `workspace/query17.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query17.preql`
- Earlier scoped-property issue: `q17_scoped_join_handoff.md`
- Earlier model-discovery diagnosis:
  `bug_q17_model_discovery_idiom_no_framework_bug.md`

The relevant disconnected-graph error appears at event 89. The full run used
31 iterations, 12 writes, 11 executions, and 1.89M tokens, then returned a
result mismatch.

## Candidate shape

```preql
with ss_ci_pairs as
where ss.date.year = 2001
  and ss.return_customer.sk = ss.customer.sk
  and ss.return_date.year in (2001, 2002)
select
    ss.customer.sk as cust_sk,
    ss.item.sk as item_sk
;

with cs_stats as
where cs.sold_date.year in (2001, 2002)
  and (cs.billing_customer.sk, cs.item.sk)
      in (ss_ci_pairs.cust_sk, ss_ci_pairs.item_sk)
select
    cs.item.id as item_id,
    cs.item.desc as item_desc,
    count(cs.quantity) as cs_qty_count,
    avg(cs.quantity) as cs_qty_avg,
    stddev(cs.quantity) as cs_qty_stddev
;
```

Discovery reports two components: the catalog inputs and aggregates in one,
and the store-derived pair rowset in another. The tuple-membership predicate is
therefore not contributing its authored dependency edge.

## Why this is distinct from the invalid attempt

The earlier candidate computed store and catalog aggregates independently in a
single select. Its disconnected-graph error was correct because no relationship
was authored.

Here, both sides of the relationship are explicit:

- the rowset projects `cust_sk` and `item_sk`;
- the catalog predicate references both projections; and
- the membership predicate defines the required semi-join dependency.

An older q17 investigation found tuple membership worked in isolation. If that
still holds, the trigger is likely the combination of a filtered rowset,
composite membership, and downstream catalog aggregation rather than tuple
membership generally.

## Expected behavior

`cs_stats` should be planned as catalog rows filtered by a composite semi-join
to `ss_ci_pairs`. The pair rowset does not need to appear in the selected
output, but it must remain in the discovery graph as a predicate dependency.

## Actual behavior

Discovery omits or loses that dependency and reports the two sides as
disconnected. The agent then abandons the intended membership formulation and
constructs independent statistics joined only by item, producing incorrect
catalog values for the requested store-state grain.

## Investigation

1. Re-run the existing tuple-membership isolation test.
2. Add filters to the source rowset, then add a two-column tuple.
3. Aggregate the filtered outer branch and check when the dependency edge is
   lost.
4. Inspect whether rowset aliases in membership predicates are registered as
   discovery inputs and condition dependencies.
5. Verify both tuple columns survive concept canonicalization; do not collapse
   the relationship to item alone.
6. Improve the disconnected-graph diagnostic to list ignored authored
   predicates when a predicate spans the reported components.

## Regression coverage

Add a compact three-table case with:

- an eligible-pairs rowset projected from one fact;
- a second fact filtered by composite tuple membership;
- an aggregate over the filtered second fact; and
- a final join of that aggregate to another rowset.

Assert that the composite membership produces a semi-join, the query plans,
and changing either tuple key changes eligibility. Preserve the earlier q17
tests that distinguish genuine missing joins from framework failures.

