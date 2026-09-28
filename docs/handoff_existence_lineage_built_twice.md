# The existence lineage is built twice (class A of the dead-group census)

## Status: FIXED 2026-09-28 with shape B (one mechanism). The analysis below is kept as written; "What landed" at the end says what changed and what the A/B showed.

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

## Open: three logs still grew (rows correct)

Per `feedback_plan_shift_cost_is_often_a_second_bug`, these are the next
items, not accepted costs. Reproduce any of them with
`TRILOGY_BENCHMARK_REBASELINE=1` off (the size check fails and names the
query) and the plan-tree dump described in
`project_existence_one_owner_group_graph_wires_feeders` (memory).

- **q54 (+230) and q64 (+183)** are the only grown files that reach the
  `_CleanFeederCache` fallback (monkeypatch `_CleanFeederCache._build` to
  count hits: q54 `my_customers.my_cust_id`, q64 `cs_ui.cs_ui_item_id` x3;
  every other file has 0 hits). The set is a ROWSET handle. The cache's
  key-widening is NOT the cause (dropping it changes neither size). In q54 the
  feeder now renders `GROUP BY 1, sales_channel, sales_item_sk` over union
  arms that project those two hidden columns, where the old nested plan gave
  `GROUP BY 1`: the fallback's plan keeps the rowset body's hidden columns
  through its FINAL GroupNode, and `_CleanFeederCache._build` slices outputs
  only for multi-column groups (`len(group) > 1`). Two questions, in order:
  why no built group covered a rowset handle the same plan builds (the
  wiring runs before the FINAL sweep; a host in a NESTED plan may be asking
  for a rowset the OUTER plan owns), and whether the single-column slice
  should mirror `resolve_existence_sources` (always slice).
- **q35 (+196)** never reaches the fallback. The customer scan (three joins,
  three `exists` atoms) renders TWICE: the count aggregate collapsed its scan
  into its own CTE (`premium`) and the avg/min/max branch kept a second scan
  (`cheerful`). Before, one scan `protective` fed both (the avg branch read
  it through a passthrough `puzzled`). `source_repeats.py --detail` is
  identical before/after (6 calls: root #3, a narrower `parent_for_consumer`
  slice #4, a repeat #5), so the difference is downstream of sourcing: the
  slice's existence parents are now `built[...]` copies wired by
  `_wire_existence(sliced, ...)` in `parent_for_consumer`, where the old
  generator path wired history-cached nested-plan nodes. Compare the two
  scans' resolved QueryDatasource identities (the CTE dedup key) in the
  `final` step of `trace_query.py`; the wrong-identity side is the bug.
- q16 +40, q94/q95 +26, q58 +34, q83 +38, q44 +7, q69 +9, q2.1/q2.2 +76:
  CTE-name churn (longer random names) plus the feeder's `GROUP BY` now
  rendered on the joined scan; no plan-shape change.
