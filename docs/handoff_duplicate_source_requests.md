# Duplicate source requests in v4 discovery

## Status: FIXED 2026-09-28 at the callers (no request cache).

Two commits on `extension-row-null-semantics`:

- `45ecbb025` *A ROOT request asked once is answered once*: the build loop
  records each ROOT group's `RootRequest` (outputs, WHERE, scope, parents).
  FINAL plans a fresh scan only when the built node does not answer its
  projected request (`RootRequest.answered_by`); a consumer slice equal to
  the parent's request reads the parent; a ROOT group whose request equals a
  built one reads a copy of it.
- `e8f8840a0` *A region domain is built under the FINAL's scope*: section 4's
  open question, answered "yes": the loop builds a domain under FINAL's scope,
  so FINAL's re-source is answered by the built node and the domain's other
  readers see the node FINAL emits.

Measured over the same corpora (`source_repeats.py`):

    before  427 plan_source calls, 65 verbatim repeats
    after   369 plan_source calls, 19 verbatim repeats

Per caller: `_fresh_final_root_projection` 9 -> 0, `parent_for_consumer`
37 -> 1, `build_strategy_node` 19 -> 18. The 18 are not two groups asking
the same question (see "What remains"). The SQL A/B: after
both commits the TPC-DS, TPC-H and thelook modeling suites (212 + thelook
battery) left every committed `zquery<N>.log` unchanged, so the generated SQL
is byte-identical; `tests/core/processing` (685) and `tests/engine` (1474,
minus the local ClickHouse server) pass.

### Why FINAL was the suspicious one

An audit of every FINAL ROOT re-source across the corpora (26 of them):

| outcome | n | how the request differed from the build's |
|---|---|---|
| verbatim | 9 | not at all |
| same plan | 6 | scope only: region domains (section 4) |
| same plan | 5 | asked for columns the built scan already outputs (WHERE args) |
| different plan | 6 | preserve-key widening (`customer.sk`, `store.sk`, `item.sk`); q46/q68 |

20 of 26 reproduced the built node, and the reason to re-source was decidable
before planning: an output the node does not fully carry, a different WHERE,
or a different scope. That is what `answered_by` checks. A partially bound
column is deliberately NOT an answer: asked for outright, `plan_source`
completes it from another source, which is a different plan.

### What remains (19 repeats, all cross-plan or internal)

- **Existence feeders** (TPC-DS q02/q08/q10/q16/q33/q35/q37/q45/q56/q58/q60/
  q82/q94/q95): `gen_root > _resolve_root_condition_sources >
  resolve_existence_sources > search_parent > search_concepts` plans the
  `IN <subselect>` body as its own statement, and that plan's ROOT group asks
  the outer root group's exact question. No caller sees both; a per-statement
  memo on the history is the only dedupe, and in q37 the outer root's node is
  dead anyway (the SQL reads only the feeder CTE `wakeful`): that is a
  built-but-unused group, measured and classified in "Dead groups" below.
- **`_plan_source`'s unfiltered fallback** (thelook q16): a conditioned
  request that declines re-asks without the WHERE, and the re-ask equals the
  plain ROOT group's request. Internal to source planning.
- One `parent_for_consumer` slice (q95) whose request is not the parent's.

`_strict_leaf_subset_binds` is why q11's no-op slice got through: it counts
binders per column, not join paths (`supplier.id` is bound by both `partsupp`
and `supplier`, so `supplier` looks prunable, but it is the path to `nation`).
The request-equality guard sits in front of it; the check itself is untouched.

## Dead groups (followup, 2026-09-28)

A dead group is one the build loop built whose node never reaches its plan's
FINAL tree: the viewer strikes it through (`notInFinal` in `viewer.html`).
`dead_groups.py` applies the same rule to whole corpora and classifies each
one by derivation and by why it is dead (see its docstring for the labels):

```bash
.venv/Scripts/python.exe local_scripts/plan_debugger/dead_groups.py \
  "tests/modeling/thelook_duckdb/*.preql" "tests/modeling/tpc_h/*.preql" \
  "tests/modeling/tpc_ds_duckdb/query*.preql"
.venv/Scripts/python.exe local_scripts/plan_debugger/dead_groups.py --detail tests/modeling/tpc_ds_duckdb/query37.preql
```

    168 dead of 923 built groups in 163 statements
    by derivation: filter 50, basic 47, root 36, rowset 14, constant 12, aggregate 8, unnest 1
    worst files: tpc_ds q08 (10), q64 (10), q76 (6), q16/q94/q95 (5)

### First, 17 were not dead: the viewer lost the tag

`origin_group` (the tag the strike-through keys on) was dropped on three
paths, so the viewer struck through groups FINAL reads. Fixed in this
followup, with `test_final_tree_keeps_the_tag_of_a_root_published_through_its_scan`:

- `_elide_single_parent_passthrough` copies the PARENT (`parent.copy()`) and
  carried `region_spans` from the projection but not its tag. A ROOT whose
  generator hands back a passthrough over its own conditioned scan (tpc_h
  q01/q06/q15, thelook q19...) was published untagged, and its trace step
  showed the pre-elision node (`conditions=None`) while `built` held the scan.
- `WindowNode`, `UnnestNode`, `UnionNode`, `SubselectNode`, `RecursiveNode`
  and `ConstantNode` `copy()` dropped it (10 window + 2 unnest groups).
- A twin-request ROOT (`built[twin].copy()`, commit `45ecbb025`) kept the
  twin's tag, so `root_d1` in tpc_h q11 never appeared on any node.

### The 168 that are dead, in four classes

**A. The existence lineage is built twice (67).** q37's shape, and the
answer to "why does the graph have a root group only an existence feeder's
separate plan ends up using": the group graph materializes an `IN <rowset>`
argument's d1 lineage as groups (`root_d1 -> ... ->
[@condition]filter:d1:...:existence:<arg>` -> host root, by an `existence`
edge) and the loop builds every one of them, `root_d1` with a `plan_source`.
But `_parent_nodes_for` skips existence-edge predecessors (deferring them to
`_attach_existence_sources` at FINAL), and the host ROOT's generator resolves
the argument itself first: `_resolve_root_condition_sources` ->
`resolve_existence_sources` -> `search_concepts` plans the subselect body as
its own nested plan (`root.py`: "Existence args are NOT forked; they go
through the shared `resolve_existence_sources`"). At FINAL,
`_attach_existence_to_node` adds a built existence node only when it brings
an output the host's parents lack (`strategy_builder.py` ~line 513), and the
generator's feeder already carries it, so the built chain is skipped whole.
Labels: `filter/rowset/basic existence` 29 (the group on the existence edge)
plus `root/basic/filter/aggregate/unnest subtree` 38 (everything upstream of
it). The 18 remaining `build_strategy_node` repeats above are these
`root_d1` groups asking the nested plan's question. Files: tpc_h q02/q20,
tpc_ds q02/q08/q10/q11/q14/q16/q23/q33/q35/q37/q45/q54/q56/q58/q60/q69/q83/
q84/q94/q95.

Two ways out, for the owner: stop building lineage whose only route to FINAL
is an existence edge into a host that resolves existence itself (today every
ROOT host does), or make the built chain the ONLY mechanism (the host reads
it through `_attach_existence_sources`, and `_resolve_root_condition_sources`
stops calling `resolve_existence_sources` for arguments the graph hosts).
The second also retires the nested plan and the 18 repeats; it is the larger
change and the one that needs the SQL A/B.

**B. Inlined into a consumer (81, cheap).** No `plan_source`: these
generators wrap their parents. `filter consumed` 27: a filtered aggregate's
argument (`sum(x ? cond)`) is rendered as a CASE inside the aggregate over the
root (tpc_ds q43 has seven `_virt_filter_sales_price_*` in one group).
`basic consumed` 20: a scalar the aggregate computes on its parent instead
(thelook q03's `item_margin` appears as an output of the root MergeNode).
`basic final-only` 18: sibling scalars folded into one projection (tpc_ds
q99's `warehouse_short_name` rides the `cc_name_lower` group's SelectNode).
`constant final-only` 12 and `aggregate consumed` 4 likewise. Cost is group
graph and build steps only; the trace reads as if the group did nothing.

**C. Roots nobody reads (16, real sourcing waste).** Each is a `plan_source`
whose result is discarded:
- `root final-only` 10: a dim peel `root:∅:dim:<key>` built beside the
  extent peel `root:∅:extent:<key>` for the same key, FINAL reads the extent
  peel (thelook q06/q07/adhoc03/adhoc04/q19, tpc_h adhoc04) — FIXED, see
  `docs/handoff_dim_peel_beside_region_domains.md`; the main
  `root:∅` when every member is consumed from `root_d1` only (tpc_ds q04/q74:
  a UNION of three fact tables joined to date, sourced for nothing); the
  `root:∅:existence:<key>` peel (q82, q82.1).
- `root resourced` 4: dim peels FINAL re-sources under its own request
  (tpc_ds q01/q65/q79; section 3's preserve-key widening).
- `root consumed` 2: thelook adhoc03/q19's `root:∅` scan of `order_items`,
  subsumed by the `root:root:dim:local.id` merge's own rescan of it.

Whether a ROOT will be read is not decidable when the loop reaches it (FINAL
picks its contributors later), so the fix shape is lazy ROOT sourcing: source
a ROOT group on first read (`_parent_nodes_for` and FINAL both go through
`built`), and an unread one costs nothing.

**D. `rowset unbuilt` 4** (tpc_ds q64, nested plans p2/p5): the build
produced no node. Not investigated.

## Original investigation

## Summary

Within one statement, `plan_source` is often asked the exact same question
more than once and answers it from scratch each time. Across the thelook,
TPC-H and TPC-DS corpora:

    427 plan_source calls
     65 repeat an earlier request VERBATIM (same outputs, WHERE, deferred WHERE,
        flags, span scope, environment and history)
      0 of those repeats returned a structurally different plan
    0.16s of 2.01s total sourcing time is spent on the repeats

The repeats come from three callers, which are three different mechanisms:

| caller | repeats | mechanism |
|---|---|---|
| `parent_for_consumer` (in `_parent_nodes_for`) | 37 | a speculative narrower rebuild of a ROOT parent that is identical to the parent |
| `build_strategy_node` (the build loop) | 19 | two ROOT groups (the same root in two phases) with identical requests |
| `_fresh_final_root_projection` | 9 | FINAL re-sourcing a ROOT contributor the loop already built with the same request |

A fourth, related waste is NOT a verbatim repeat and does not show in that
count: a region DOMAIN group is built under its group scope, then FINAL
re-sources it under FINAL's scope and discards the built node (section 4).
That is the case visible in `adhoc04`.

## Reproduce

```bash
# counts, per file and by caller
.venv/Scripts/python.exe local_scripts/plan_debugger/source_repeats.py \
  "tests/modeling/thelook_duckdb/*.preql" "tests/modeling/tpc_h/*.preql" \
  "tests/modeling/tpc_ds_duckdb/query*.preql"

# one file's calls, with each repeat's call path
.venv/Scripts/python.exe local_scripts/plan_debugger/source_repeats.py --detail tests/modeling/tpc_h/query11.preql

# step through the same statement in the viewer (traces are gitignored; regenerate)
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/tpc_h/query11.preql --open
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/thelook_duckdb/adhoc04.preql \
  --rows --setup tests.modeling.thelook_duckdb.db_build:seed --open
```

`source_repeats.py` wraps `plan_source` in the three modules that import it
(`source_planning`, `strategy_builder`, `v4_node_generators/root`) and
fingerprints each request; see its docstring for exactly what the fingerprint
covers. In the viewer, each step's header shows the group being built and the
planner call path, and built groups whose node never reaches the FINAL tree are
struck through in the step list.

Worst files: `tpc_h/query11` (4 repeats of 5 calls), `tpc_ds/query14` (4 of
8), `tpc_ds/query10` and `query44` (3 each).

## 1. `parent_for_consumer`: a slice identical to its parent (37)

`strategy_builder.py` `_parent_nodes_for` -> `parent_for_consumer` (~line 766).
For a ROOT parent feeding a GROUPING consumer, it builds a narrower "slice" of
the parent carrying only the columns the consumer needs, and adopts the slice
only if it strictly prunes the leaf datasources
(`_leaf_datasource_ids(sliced) < _leaf_datasource_ids(node)`) or the parent
carries the wrong side of a scoped relation. Otherwise it discards the slice
and returns `node.copy()`.

The slice is attempted whenever `slice_addresses` is a strict subset of the
parent's outputs. A conditioned root scan exposes its WHERE's arguments beyond
what it was asked for. In `tpc_h/query11` the root was requested as
`{available_quantity, id, supply_cost, supplier.id}` with
`where supplier.nation.name = GERMANY`, and the built node also outputs
`supplier.nation.name` and `supplier.nation.id`. The consumer needs only the
four requested columns, so the slice request is EXACTLY the parent's original
request. The rebuild is identical, prunes nothing, and is discarded, once per
grouping consumer (3 times in q11: trace steps 19, 22, 25).

Fix options, cheapest first:
- Skip the slice when its outputs and conditions equal the request the parent
  was sourced with. Requires remembering each built ROOT's request (it is not
  on the node today).
- The request cache in section 5, which covers this and sections 2–3.

## 2. Twin ROOT groups with identical requests (19)

The same root in two phases, e.g. `grp:root:root:∅` and `grp:root:root_d1:∅`
in `query11` (trace steps 13 and 16): the d1 root feeds the condition-phase
aggregates. When the WHERE atoms placed on each twin are the same, the two
groups send identical requests. The groups are legitimately distinct in the
group graph; only their sourcing is duplicated. The cache in section 5 makes
the second one free. Deduplicating the groups themselves is a larger change
and not obviously right: the phases can diverge under other WHEREs.

## 3. FINAL re-sourcing an unchanged ROOT contributor (9 verbatim)

`_assemble_final_node` (~line 4130 onward) re-sources EVERY ROOT contributor
through `_fresh_final_root_projection` (~line 3097) and replaces the built
node, unless the group's WHERE atoms can't all be stated on the scan. The
comments give the reasons the request can differ from the build's:
- preserved merge keys are added (`preserve_keys`, `own_join_keys` narrowing);
- filter-only WHERE args that were peeled into the bucket are added;
- a region domain keeps FINAL's scope (section 4).

When none of those change the request, the re-source is verbatim (9 cases),
and the cache in section 5 makes it free.

## 4. Region domains built at group scope, then re-sourced at FINAL scope

Not a verbatim repeat, so not in the 65; `adhoc04` is the example (trace steps
18–23 build the domains, 31–34 re-source them).

- **The build loop** (`build_strategy_node`, ~line 4542) scopes every group,
  domains included, with
  `extent_free = ownership.suppressed_for(gid) | span_scope.owned`. For the
  `product.id` domain that suppresses `user.id`
  (`extent_free={user.id}`), and vice versa.
- **FINAL** (~line 4250) deliberately re-sources a domain under FINAL's own
  scope: "A domain is the FINAL's own rows and keeps the FINAL's scope: the
  customer domain completes its transitive `~` address there, and the orphan
  address's state is read off it (`test_licensed_transitive_attr_span`)."
  In adhoc04 that is `extent_free=∅`.

So the two requests differ by scope and both are planned. In adhoc04 the plans
come out the same (a plain `products` / `users` scan), the FINAL one is used,
and the built one is dead: the viewer strikes steps 20 and 23 through, since
their nodes never reach the FINAL tree.

**The built domain node is NOT always dead.** A domain can have readers of its
own: an aggregate evaluated over the region's rows
(`_feed_region_domains`' docstring, ~line 2760), and
`feed_region_domains_to_present_scalars`. Those readers consume the build-time
node under the group scope, and `parent_for_consumer` deliberately reads a
region domain "as built" (a re-sourced slice would hold no region). The built
node is also FINAL's fallback when the re-source is skipped (an atom the scan
cannot state).

Open question for whoever picks this up: should a domain be built under
FINAL's scope in the first place? The FINAL comment argues a domain's rows are
FINAL's rows. If that holds for its other readers too, building domains at
FINAL scope makes the FINAL re-source a verbatim repeat (free under section
5), and removes the scope mismatch between what the readers saw and what FINAL
emits. If the readers NEED the group scope, the two builds are genuinely
different questions and only the no-reader case (adhoc04) can drop one: skip
building a domain with no readers, and let FINAL source it once.

## 5. Proposed general fix: reuse identical requests within a statement

Remember each `plan_source` result keyed by the full request fingerprint, and
return a copy on a verbatim repeat. This mirrors the existing history cache
(`concept_strategies_v4.py` ~line 823: "a node may be mutated after being
cached; always store a copy"; `V4History.build_to_history` in
`v4_helper/history.py`). Callers mutate what they get back (`region_spans`,
`origin_group`, wrapping), so both the stored and returned nodes must be copies.

The fingerprint must be at least what `source_repeats.py` uses:
- output addresses;
- `conditions` and `deferred_conditions`;
- `require_full` and `complete_partials`;
- the full `span_scope`: `owned`, `unextended`, `extent_free`, `extent_free_carried`;
- environment and history identity. Adding environment identity dropped the
  count from 70 to 65: rowset bodies plan in their own environment and must
  not share entries.

Before trusting it, check anything else `plan_source` reads from the
environment that can change mid-statement (e.g. `statement_output_addresses`
and `statement_hidden_addresses` when a nested plan is running).

Evidence it is safe: 0 of 65 repeats produced a different plan (compared
structurally: node type, outputs, datasource, conditions, recursively). The
same measurement is the regression check. Then run the full suite, and do a
before/after comparison of the generated SQL: the `zquery_*.log` blob-hash
A/B over the modeling corpora should show byte-identical SQL, since a cache
hit returns the plan the repeat would have built.

Expected win: modest in time (about 8% of sourcing, which is itself a slice of
planning), larger in trace readability (37 discarded slices and the twin
sources disappear), and it makes section 4's fix a pure scope decision.

## Also noticed (not duplicates)

- `copy()` carries `region_spans` (and now `origin_group`) on only 5 of the 11
  node classes: base, filter, group, merge, select. `RecursiveNode`,
  `ConstantNode`, `SubselectNode`, `UnionNode`, `UnnestNode` and `WindowNode`
  construct and return without it, so a copy of one of those loses its
  region contract. Worth checking whether any of them can be a region domain
  or carry `region_spans`.
- adhoc04's final CTE groups by all 7 output columns (FINAL's
  `deduplicate_to_grain`) although its rows are already unique at those
  columns. Another session was working on this (`rows_unique_at_outputs` in
  `grain_utility.py`, `tests/engine/test_projected_row_identity.py`).
