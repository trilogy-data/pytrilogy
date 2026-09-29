# The existence lineage is built twice (class A of the dead-group census)

## Status: FIXED 2026-09-28 with shape B (one mechanism), and the two logs its A/B left grown closed the same day. The analysis below is kept as written; "What landed" onward says what changed.

Follows `docs/handoff_duplicate_source_requests.md` ("Dead groups", class A)
and `docs/handoff_dim_peel_beside_region_domains.md` (class C, fixed at
`a36122952`). This is the largest class: 67 of the 168 dead groups, and it
contains the 18 `build_strategy_node` repeats the first handoff left over.

## The case

`tests/modeling/tpc_ds_duckdb/query37.preql`, last statement:

```
select items.id, items.desc, items.current_price
where items.sk in inv_item_ids;   # inv_item_ids: a rowset / `?` set over the inventory fact
```

```bash
# the two dead groups and their reasons (`root subtree`, `filter existence`)
.venv/Scripts/python.exe local_scripts/plan_debugger/dead_groups.py --detail tests/modeling/tpc_ds_duckdb/query37.preql
# the repeated plan_source request, with the call path of the repeat
.venv/Scripts/python.exe local_scripts/plan_debugger/source_repeats.py --detail tests/modeling/tpc_ds_duckdb/query37.preql
# step through it: two plans, p0 (the statement) and p1 (the feeder)
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/tpc_ds_duckdb/query37.preql --open
```

What the trace shows (statement plan p0):

| pass | what exists |
|---|---|
| group graph | `grp:root:root_d1:∅` -(lineage)-> `grp:[@condition]filter:d1:∅:existence:local.inv_item_ids` -(**existence**)-> `grp:root:root:∅` -> FINAL |
| build loop | builds `root_d1` (a `plan_source` over `inv.warehouse_inventory` ⋈ `inv.date.date`) and the existence FilterNode over it |
| build of `root` | its generator plans the `IN` body as **its own plan p1** (`plan: local.inv_item_ids`), whose `root` group asks `root_d1`'s exact question again |
| FINAL tree | `items` scan with p1's tree as its existence parent; p0's `root_d1` and existence filter appear nowhere |

The SQL reads only p1's CTE. The same shape, per file: tpc_h q02/q02-region/q20;
tpc_ds q02(+one/two)/q08/q10/q11/q14/q16/q23/q33/q35/q37/q45/q54/q56/q58/q60/
q69/q83/q84/q94/q95. Labels in the census: `filter/rowset/basic existence` 29
(the group on the existence edge) + `root/basic/filter/aggregate/unnest
subtree` 38 (everything upstream of it, `root_d1` included). q08 alone has 10
(a long d1 chain: unnest, basic, filter, aggregate, all dead).

## The mechanism, by seam

Two mechanisms plan the semijoin set; only the second is read.

1. **The group graph materializes the set's lineage as groups, on purpose.**
   `_add_d1_root_buckets` gives the condition phase its private `root_d1`,
   and `_prune_existence_exclusive_roots` (`v4_helper/group_graph.py:426`)
   then drops the existence-exclusive columns from the shared root "once they
   have been duplicated into the private root_d1 bucket", its docstring
   saying the set "is sourced as a separate discovery". So the design already
   knows the set is planned elsewhere, and still keeps the `root_d1` chain as
   groups with an `existence`-kind edge into the host.
2. **The build loop builds every one of them** (`build_strategy_node`,
   `strategy_builder.py` ~4960 onward): a group is a group. `root_d1` is a
   real `plan_source`; the FilterNode/BASIC/aggregate over it are wrappers.
3. **The host's own generator plans the set itself, first.** `_parent_nodes_for`
   skips existence-edge predecessors (`strategy_builder.py` ~697: "existence-
   kind edges feed a subselect, not the row stream; `_attach_existence_sources`
   wires them as side-channel parents post-build"). Then the ROOT generator's
   `_resolve_root_condition_sources` (`v4_node_generators/root.py:187`) calls
   the shared `resolve_existence_sources` ("Existence args are NOT forked;
   they go through the shared `resolve_existence_sources`, since a side-channel
   subselect is built the same way regardless of how the consumer sourced its
   own rows"), which runs `search_parent > search_concepts` on the set's
   concept: a nested `_build_from_graph`, plan p1, with its own group graph,
   its own `root` group and its own `plan_source`. That request is the repeat
   `source_repeats.py` counts under `build_strategy_node`.
4. **At FINAL the built chain is offered and declined.**
   `_attach_existence_sources` (`strategy_builder.py:521`) walks every
   condition host and every built node; `_attach_existence_to_node` (489)
   adds a built existence-group node as a parent only when it brings an
   output the host's parents do not already have (~513). The host's parents
   already include p1's feeder, which outputs the same concept, so the built
   chain is skipped. `_CleanFeederCache` (285) is the third party here: for a
   self-referential membership it re-sources the set unfiltered, once, and
   hands out copies, because wiring the built node would form a cycle.

So the built chain is not a fallback anyone reaches in the corpora: of the 20
built groups with `:existence:` in their id across thelook, TPC-H and TPC-DS,
0 are in their plan's FINAL tree (measured with `dead_groups.py`'s helpers,
2026-09-28). Its cost
is `root_d1`'s `plan_source` (a fact-table join, in q37 two tables; in q08 a
customer/address/demographics chain) plus the wrappers, per existence arg.

## Two fix shapes

**A. Do not build lineage whose only route to FINAL is an existence edge
into a host that resolves the set itself.** Decidable from the group graph
before sourcing: a group is existence-only when every path from it to FINAL
passes through an `existence`-kind edge (the `root_d1 -> ... -> filter ->
host` chain; `subtree` + `existence` labels in the census are exactly this
set). Skipping the build leaves `built` without those gids, so
`_attach_existence_sources` finds nothing to offer and the generator's plan
stands, which is what happens today anyway. Cheapest; touches the loop only
(a guard before `build_node`, or better, don't materialize them as groups in
`_materialize_group_graph`). Risk: a host that does NOT resolve its own
existence args and relied on the built chain. Check `resolve_condition_sources`
(the generic path in `condition_sources.py`) and every generator that
receives `conditions`; if all of them call `resolve_existence_sources`, no
host relies on the chain. The self-referential case goes through
`_CleanFeederCache` regardless.

**B. One mechanism: the host reads the built chain.** Stop
`_resolve_root_condition_sources` (and the generic path) from calling
`resolve_existence_sources` for an arg the group graph hosts, and let
`_attach_existence_sources` wire the built node. Retires the nested plan p1
and the 18 repeats, and the group graph becomes the only place the set's
lineage is decided. Larger: the built chain is scoped and conditioned by the
statement's group graph (`root_d1` carries the condition phase's atoms and
span scope; `carry_spans_to_condition_scans`), while p1 plans the set as a
bare statement; the two can differ under `~` regions and `then where`
stages, and the FINAL tree's shape changes for ~20 TPC-DS files. Needs the
zquery-log SQL A/B and rows on every affected file, plus the self-referential
cycle case `_CleanFeederCache` exists for.

Recommendation: A first. It removes the waste with no change to what the SQL
reads, and its A/B should be byte-identical (the built chain never reaches a
CTE). B is a design decision for the owner: whether the group graph or the
generator owns the semijoin set.

## Verification for A

- `source_repeats.py` over the three globs: 363 calls today, 18 repeats by
  `build_strategy_node`, 1 by `parent_for_consumer`. After A: the 18 gone,
  calls down by at least the `root_d1` count (one per existence arg).
- `dead_groups.py` summary: the `existence` and `subtree` reasons should
  drop to (near) zero; nothing new should appear under `unbuilt`.
- Full suite (`-m "not adventureworks_execution and not clickhouse_server"`
  locally), then the three modeling suites: every committed `zquery<N>.log`
  byte-identical. If one changes, A reached a host that read the chain: that
  file is the case to study, not a cost to accept.
- Rows on the thelook files that use `?` sets (`db_build:seed`); the TPC-DS
  suite compares rows against the reference SQL already.

## Verification for B

Everything above, plus a before/after diff of the FINAL tree per affected
file (`trace_query.py`, `final` step) and rows on every file whose
`zquery<N>.log` changes, and `tests/engine/test_self_ref_membership*` /
whatever pins the `_CleanFeederCache` cycle (grep "self-referential").

## Pointers

- `trilogy/core/processing/v4_helper/group_graph.py`: `_add_d1_root_buckets`,
  `_prune_existence_exclusive_roots` (426), `_materialize_group_graph`,
  `EdgeKind.EXISTENCE` edges.
- `trilogy/core/processing/v4_helper/strategy_builder.py`: `_CleanFeederCache`
  (285), `_attach_existence_to_node` (489), `_attach_existence_sources` (521),
  `_parent_nodes_for`'s existence skip (~697), the build loop (~4960),
  `_attach_existence_sources` call before FINAL (~5085).
- `trilogy/core/processing/v4_node_generators/root.py`:
  `_resolve_root_condition_sources` (187).
- `trilogy/core/processing/v4_helper/condition_sources.py`:
  `resolve_existence_sources`, `resolve_condition_sources`.
- Measurement: `local_scripts/plan_debugger/dead_groups.py` (labels
  `existence`, `subtree`), `source_repeats.py`, the viewer's struck-through
  steps (tags are reliable since `09b7e4970`).

## What landed (shape B)

The rule: **for a group the loop builds, the group graph is the only planner
of an `IN <set>` arg.** A generator hosts the atom in its node's `conditions`
and never plans the set; the loop wires the set's built provider onto the
host as a side-channel parent as soon as the node exists, before any consumer
copies it. The standalone nested planner survives in exactly one place,
`_CleanFeederCache`, and is reached only when no built group covers the arg
(the self-referential cycle, a FINAL-time re-source, a composite tuple no
single group holds).

- `strategy_builder.py`: `_wire_existence(node, built, feeder_cache,
  preferred)` walks a node's subtree and attaches per node; it replaces the
  pre-FINAL `_attach_existence_sources` sweep and `condition_hosts`. Called
  in the loop right after each build (preferring the host's EXISTENCE-edge
  predecessors as provider), on a `parent_for_consumer` slice, and over the
  FINAL tree. `_existence_parents_for` falls back to the feeder cache for
  any arg size and slices the provider copy to the arg's columns (mirrors
  the old generator path). `_group_existence_arg_groups` is gone: the node
  walk sees the same atoms through `node.conditions`.
- `root.py`: `gen_root` no longer calls `resolve_existence_sources`; the
  gated-vs-wrapper host decision reads the arg addresses instead of a
  feeder's outputs.
- `condition_sources.py`: `resolve_row_sources` split out; the rowset
  boundary (`gen_rowset`) uses it. `resolve_condition_sources` (row +
  existence) remains the entry for clauses applied outside any group graph
  (multiselect WHERE, nested-select HAVING): those have no built provider.

Measured over thelook + TPC-H + TPC-DS: `source_repeats.py` 363 calls / 19
repeats -> 351 / 2 (the two left are thelook q16's unfiltered re-ask and q35's
`parent_for_consumer` slice, both known); `dead_groups.py` 162 dead -> 114,
`existence` 29 -> 0, `subtree` 38 -> 10.

The SQL A/B was NOT byte-identical, and the growth was two more bugs, not a
cost: the built chain is `FilterNode(x ? cond)` over the `root_d1` scan, and
once pushdown moved `cond` onto the scan the FilterNode's CTE was a rename-
only passthrough `CollapseSingleParent` could not fold (`rename_reference`
did not know a filter item), and a predicate over a scalar the parent
computes itself could not be pushed onto it, which kept q16's returns join
LEFT. Both fixed; see `docs/handoff_grain_matched_projection_collapse.md`
"Filter renames".

A third correction came from review: with the nested plan gone, nothing
claimed the SET's grain. `target_week_seqs <- week_seq ? date in (...)`
rendered as a bare projection of a date-grain scan, one row per date: a node
advertising a concept at week_seq grain while emitting finer rows (the
silent-fan-out class), harmless only because existence parents are
side-channel. `gen_filter` with `existence_source` now groups to its outputs
when the parents' rows are not already unique at the set column
(`_rows_unique_at_set`, judged on the filter's CONTENT column so a key-grain
set gets no redundant GROUP BY). q37/q45/q56/q60 are then byte-identical to
the committed baseline; q05 -629, q24 -671, q08 -105, q23 -79 lose a
passthrough or a second scan, rows verified by the suites.

## q54 and q64 FIXED 2026-09-28: the feeder is a SET, wired once

Two rules, both at the wiring seam, guarded by
`tests/core/processing/test_v4_existence_feeder_set_grain.py` (which fails at
`07a348b1d`):

- **`_feeder_at_set_grain`**: the slice to the subselect's columns now comes
  with a `GroupNode` over them. A provider built for the row stream sits at
  the grain its own consumers need, and slicing it there leaves duplicates.
  q54's provider is the `my_customers` boundary, whose body reads the
  multi-channel union, so the node it projects the handle off holds the body's
  own grain keys hidden and groups by them: `GROUP BY 1, sales_channel,
  sales_item_sk` over union arms projecting both, where the old nested plan
  gave `GROUP BY 1`. The boundary's `grain` claim (the handle alone) was the
  lie; the fan-out is real and unread, which is still a fan-out. `GroupNode`
  judges whether the group is needed (`check_if_group_required`), so a
  provider already at the set's grain resolves to a plain SELECT and elides —
  no size cost where the rule was already met.
- **`_attach_existence_to_node` is now idempotent**: an arg group a PARENT
  already supplies is left alone (judging it on `existence_concepts` instead
  would be wrong — the attach sets those even when it found no provider, and a
  later pass with more of `built` must still get its chance).
  `_wire_existence` runs per node right after each build, and only a parent
  bringing a NEW output is appended, so the first wiring wins. When the outer
  loop walks the subtree of a node that
  wraps a NESTED plan, `built` is the outer plan's groups and holds no
  provider for the inner plan's set, so the fallback was planned for a set the
  inner plan had already wired correctly. Standalone `_CleanFeederCache._build`
  calls over the three corpora: 7 -> 2 (q14 1, q54 1, q64 3 gone; q82/q82.1's
  self-referential `store_sales.item.sk` remains, which is what the cache is
  for). `_CleanFeederCache`'s single-column no-slice is NOT the cause and is
  left alone.

Against the pre-round baseline (`5f16370f1`), after both: q02, q10, q2.1, q2.2,
q33, q54, q58, q69, q83 byte-identical (q10 differs only by two symmetric
feeders swapping CTE names); q14 -46, q23 -14; q16/q94/q95 +15 and q64 +39
(the pushdown fix's extra `is not null` predicates plus name churn); q08 +147
and q35 +189, both below.

**q08 (2853 -> 3105, rebaselined)** is the one growth, and it is the rule
working: the inner `exists` RHS was the address-grain `abundant` and is now
deduped to the 5-char zip prefix the set actually is (~50k rows -> a few
thousand at sf=1), and the outer feeder's dedup that pre-round had is restored.
The dedup costs a CTE because `CollapseSingleParent` will not fold it into
`abundant`: `lineage_contains_aggregate` counts `zip_p_count` as an aggregate
`abundant` renders INLINE, when `abundant` reads it out of `questionable`'s
source_map. Filed in `docs/handoff_grain_matched_projection_collapse.md`
("A parent that SOURCES an aggregate"); fixing it should absorb most of the
growth.

## Open: q35 (+189 over pre-round, rows correct)

Per `feedback_plan_shift_cost_is_often_a_second_bug` this is the next item, not
an accepted cost. Reproduce with `TRILOGY_BENCHMARK_REBASELINE=1` off (the size
check fails and names the query) and the plan-tree dump described in
`project_existence_one_owner_group_graph_wires_feeders` (memory).

q35 never reaches the fallback. The customer scan (three joins, three `exists`
atoms) renders TWICE: the count aggregate collapsed its scan into its own CTE
(`premium`) and the avg/min/max branch kept a second scan (`cheerful`). Before,
one scan `protective` fed both (the avg branch read it through a passthrough
`puzzled`). `source_repeats.py --detail` is identical before/after (6 calls:
root #3, a narrower `parent_for_consumer` slice #4, a repeat #5), so the
difference is downstream of sourcing: the slice's existence parents are now
`built[...]` copies wired by `_wire_existence(sliced, ...)` in
`parent_for_consumer`, where the old generator path wired history-cached
nested-plan nodes. Compare the two scans' resolved QueryDatasource identities
(the CTE dedup key) in the `final` step of `trace_query.py`; the
wrong-identity side is the bug.
