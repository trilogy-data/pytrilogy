# P1: inline-filtered aggregate whose condition reads a same-grain pinned aggregate plans a group-node cycle (hard crash)

**Status: FIXED 2026-08-15** in `group_rules.py`
(`_aggregate_lineage_layers`). The guard was NOT softened; see Resolution.

**Was: OPEN. Loud failure on a valid query.** The raise itself is the
deliberate cycle guard from `830fc4329` (#633) doing its job; the bug is the
planner wiring a circular dependency for a construct that has an obvious valid
build order. Before #633 this shape would have fallen through silently, so do
not "fix" this by softening the guard.

Found 2026-08-15 while probing the r04 scope question (see
`bug_where_pinned_aggregate_ignored.md`, which resolved NOT-A-BUG; this crash
was its one genuine defect).

## Symptom

Selecting at grain `k` an aggregate whose inline `?` condition references an
aggregate pinned `by k` raises:

```
UnresolvableQueryException: Query planning produced a circular dependency
between group nodes, so there is no valid build order:
[('grp:aggregate:d0:local.order_id:input:local.line',
  'grp:filter:d*:local.order_id:sig:5c7957da6d99'),
 ('grp:filter:d*:local.order_id:sig:5c7957da6d99',
  'grp:aggregate:d0:local.order_id:input:local.line')]
```

raised at `trilogy/core/processing/v4_helper/strategy_builder.py:2039`
(`_topological_dependency_order`'s raising twin). Ten
`[v4] group-graph lineage cycle, skipping concept-set pass` warnings
(`group_graph.py:2024`) precede it: every retry rebuilds the same cycle.

## Reproducible-on

`2435237d4` (current HEAD at time of filing), fully local synthetic model, no
warehouse needed.

## Minimal repro

```trilogy
key line int;
property line.order_id int;
property line.wh int;
property line.region string;

datasource lines (line, order_id, wh, region) grain(line)
query '''select 1 as line, 10 as order_id, 1 as wh, 'A' as region
union all select 2, 10, 2, 'B'
union all select 3, 20, 1, 'A'
union all select 4, 20, 1, 'A'
union all select 5, 30, 3, 'B'
''';

auto owc <- count_distinct(wh) by order_id;

select order_id, count(line ? owc > 1) as n;
```

Expected (owc: order 10 = 2, orders 20/30 = 1; the condition is constant per
order): `(10, 2), (20, 0), (30, 0)`. Observed: the crash above.

## Trigger matrix

| # | Query | Result |
|---|---|---|
| 1 | `select order_id, count(line ? owc > 1) where region = 'A'` | was CRASH, now OK |
| 2 | `select order_id, count(line ? owc > 1)` (no WHERE at all) | was CRASH, now OK |
| 3 | `select order_id, count(line) where region = 'A'` | OK |
| 4 | `select order_id, count(line ? region = 'A')` (plain-column condition) | OK |
| 5 | `select count(line ? owc > 1) where region = 'A'` (global grain) | OK `(0,)` |
| 6 | inline pin: `count(line ? (count_distinct(wh) by order_id) > 1)` at order grain | was CRASH, now OK |
| 7 | `select order_id, owc where region = 'A'` (project the pinned agg) | OK |
| 8 | cell 1 with `where wh = 1` instead | was CRASH, now OK |
| 9 | `sum(line ? owc > 1)` instead of count | was CRASH, now OK |

Minimal failing combination (cells 2, 5, 4): **select grain = the pin grain**,
AND the `?` condition references the pinned aggregate, AND the reference sits
inside another aggregate. A WHERE clause is irrelevant (2 vs 1/8); the auto vs
inline spelling of the pin is irrelevant (6); which outer aggregate is
irrelevant (9). Projecting the pinned aggregate directly is fine (7); at
global select grain it plans (5).

## Observed cycle and hypothesis

The cycle is `aggregate(input=local.line, grain=order_id)` <->
`filter(grain=order_id)`. The true dependencies are acyclic:

1. `owc` = aggregate over raw lines by order_id (input `local.wh`);
2. filter stage applies `owc > 1` to lines;
3. `count(line)` aggregates the filtered lines by order_id.

The printed aggregate node is the `input:local.line` one (step 3), meaning the
graph has an edge aggregate -> filter AND filter -> aggregate on the SAME node.
The filter's condition dependency should point at the step-1 `owc` aggregate
(input `local.wh`), so the likely defect is the filter group's
condition-dependency resolving onto the wrong aggregate node when two
aggregates share the grain, plausibly a node-signature keyed by grain that
collapses or mis-matches them. Root cause needs a pass through the group-edge
construction in `trilogy/core/processing/v4_helper/group_graph.py` (edge
wiring) with this repro; not chased further here (read-only filing).

## Severity

P1: hard error with a clear message, valid and natural construct (the "count
things whose group passes an aggregate test, per group" shape). Not silent.
The construct is reachable from ordinary agent-written TPC-DS answers; the r04
agent hit an adjacent shape live. Every workaround exists (project the pinned
aggregate and filter in HAVING, or stage with `then where`), but the agent has
to discover them by trial.

## Verdict

FRAMEWORK bug, planner group BUCKETING (not edge wiring). Do NOT relax the
`strategy_builder.py:2039` cycle guard; it is what makes this loud.

## Resolution (2026-08-15)

The "wrong aggregate node" hypothesis above was close but off by one layer.
Instrumenting `_materialize_group_graph` showed every CONCEPT edge is correct
and acyclic: `local.owc -> _virt_filter_line -> local.n`, exactly the chain
the hypothesis section predicts. The filter's condition dependency resolves
onto the right node.

The defect is that `owc` and `n` are not two nodes at all. Aggregate bucketing
(`_partition_standard_aggregates`) keyed buckets on
`(label, depth, grain, input_grain, populations)` alone. Both aggregates sit at
grain `order_id` over `local.line`-grain rows, so they merged into ONE bucket,
and collapsing a producer with its consumer folds the acyclic concept chain
into a group-level 2-cycle. That is why the printed cycle names the
`input:local.line` aggregate on both sides: it is one node wearing both roles.

Fix: `_aggregate_lineage_layers` in
`trilogy/core/processing/v4_helper/group_rules.py`. Within a candidate bucket,
members are layered by longest chain of in-bucket lineage ancestors, so no
bucket holds both an aggregate and a lineage consumer of it. This is the
aggregate twin of `_feeds_extra_signature_group`, which already enforced the
same producer/consumer split for BASIC. Layer 0 keeps the unsplit group id, so
queries that do not hit the shape plan byte-identically.

Resulting plan is the acyclic chain: scan -> `owc` aggregate -> broadcast join
back to lines -> filter -> count.

Regression tests: `tests/engine/test_duckdb_filter.py`
(`test_filtered_aggregate_over_same_grain_pinned_aggregate`, 5 row-asserting
cells, plus `test_same_grain_aggregates_without_lineage_still_co_source` as the
over-split guard). All 5 fail with the layering patched off, reproducing the
exact cycle above.

Verification: all 9 matrix cells plan; full suite green (8004 passed);
tpc-ds corpus render byte-identical A/B with 0 layering firings, so the rule
only activates on this shape. Also probed: the window analog plans, two sibling
consumers of one pin correctly share a layer, and a 3-level aggregate chain
layers to exactly 3 GROUP BYs.

Cells 1 and 8 now return `(10,0),(20,0)` rather than the report's naive
expectation, because the WHERE narrows the pinned aggregate's population, which is the
pre-existing behavior cell 7 already showed and which
`bug_where_pinned_aggregate_ignored.md` resolved NOT-A-BUG. Cell 9's NULLs are
standard SUM over an empty set.
