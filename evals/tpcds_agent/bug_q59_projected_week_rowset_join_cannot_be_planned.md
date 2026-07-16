# Bug: q59 projected week rowset join cannot be planned

## Resolution (2026-07-13) — FIXED

Root cause: unbounded discovery recursion (RecursionError surfaced as the
generic "could not be planned" error). The subset-side rowset marks its own
projected member key (`this_yr.ss.store.id`) partial via `scoped_partial`, so
`unsatisfied_optionals` treated the node's own output as unsatisfiable when it
appeared in `local_optional`. Enrichment then sourced the "full" column through
the anchor rowset, whose symmetric enrichment requested it back — ping-ponging
forever. Trigger depends on priority-concept ordering: the member key must land
in the optional list (here `ss.date.week_seq` sorts before `ss.store.id`),
which is why flat single-namespace reductions passed.

Fix: `_enrich_rowset_node` (trilogy/core/processing/node_generators/
rowset_node.py) now treats a subset-join member among the node's OWN outputs as
served — it is partial by declaration (the anchor holds the fuller group axis),
not by source; the completion merge above pairs it with the anchor over the
group pseudonym. Regression:
tests/join_matrix/test_aggregate_rowset_offset_join.py (fails pre-fix with
RecursionError; asserts the join carries exactly the authored store-equality +
offset-week predicates, measures never join keys, anchor-preserving rows).

## Summary

The latest enriched q59 run eventually passed through a window-based
workaround, but the direct and semantically natural formulation fails during
planning. Two aggregate rowsets project store and week keys, then join on the
same store and a 52-week offset. Trilogy returns:

```text
Resolution error ... query could not be planned; this is a bug.
```

This is a confirmed discovery/planning defect. The join keys are explicitly
projected by both rowsets and explicitly related by the query.

## Artifacts

- Run: `evals/tpcds_agent/results/enriched_scope_v2_full_20260713-173444`
- Trajectory: `agent_log.q59.jsonl`
- Final candidate: `workspace/query59.preql`
- Reference: `tests/modeling/tpc_ds_duckdb/query59.preql`
- Related performance report: `bug_q59_weekly_window_planner_runaway.md`
- Related wrong-result report: `handoff_q59_nullable_rowset_measure_identity_join.md`

The failures occur repeatedly in the trajectory, including events 25 and 79.
The run required 49 iterations, 24 writes, and 17 executions before finding a
workaround.

## Minimal failing shape from the trajectory

```preql
rowset this_yr <-
where ss.date.year = 2001
select
    ss.store.id,
    ss.store.name,
    ss.date.week_seq,
    sum(ss.sales_price ? ss.date.day_of_week = 0) as sun_sum
;

rowset next_yr <-
where ss.date.year = 2002
select
    ss.store.id,
    ss.store.name,
    ss.date.week_seq,
    sum(ss.sales_price ? ss.date.day_of_week = 0) as sun_sum
;

select
    this_yr.name,
    this_yr.id,
    this_yr.week_seq,
    this_yr.sun_sum / next_yr.sun_sum as sun_ratio
subset join this_yr.id = next_yr.id
subset join this_yr.week_seq + 52 = next_yr.week_seq
;
```

The actual candidate contains seven weekday measures, but the defect should be
minimized from this shape.

## Expected behavior

The planner should materialize each aggregate rowset once and join them using:

```text
this_yr.id = next_yr.id
this_yr.week_seq + 52 = next_yr.week_seq
```

Aggregate payloads must not be required as identity keys. The projected
`week_seq` property must remain discoverable after rowset materialization.

## Actual behavior

Planning terminates with the generic internal-bug error. In other attempts the
same family lost the projected rowset week identity and reported that it could
not resolve a `_subquery_...weekly.ss.date.week_seq` concept.

The agent therefore switched to `lead` over a combined rowset. That path also
produced zero rows when the year filter entered the window input scope, making
the discovery failure substantially more expensive to diagnose.

## Investigation

1. Minimize from one weekday measure, then expand to all seven.
2. Compare equality joins with expression joins such as `left + 52 = right`.
3. Inspect rowset concept lineage for both projected `week_seq` properties.
4. Confirm the join graph uses only authored store/week keys, not nullable
   measure payloads.
5. Replace the generic internal-bug message with the unresolved concepts and
   candidate graph if planning still fails.

## Regression coverage

Add a small two-year fixture with multiple stores, weeks, and nullable weekday
measures. Assert that:

- two aggregate rowsets can join on projected keys;
- an arithmetic offset is valid in a scoped join predicate;
- the generated SQL contains the store and offset-week predicates;
- the result matches an equivalent normalized-week/window formulation; and
- planning remains bounded as measures are added.

