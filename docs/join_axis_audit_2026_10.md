# Join-axis audit (2026-10): why merge join keys are discovered ad hoc, and a design to declare them

Prompted by PR #770 (issue #769): three keyless-join bugs fixed by teaching
FINAL assembly a new notion of a group's grain (`emitted_grain`), a new bridge
ranking, and a scalar exemption. Each fix was correct and each was local. The
question this audit answers is why the join between two planned groups keeps
needing local fixes at all, and what one change would stop that.

Method: read the three stages that touch join keys, instrument them, compile
the 210-statement corpus (`tests/modeling`, `tests/engine`, `tests/complex`,
`tests/discovery`), record which paths fired and which keys the resolve-time
join discovery used, and test whether a short declarative rule over stage-2
facts alone reproduces those keys. The instrument is
`local_scripts/sql_ab/join_census.py`; every number below comes from it on
`fix-769-keyless-join` at `5efddda57`.

## 1. Verdict

The pipeline does what the architecture says for concepts and for groups: a
logical concept graph, a logical group graph, physical sources mapped onto each
group, then materialize/optimize/render. It does **not** do that for the joins
*between* groups. The group graph has no edge that says "A pairs with B on K".
A merge's join keys are an emergent property of which columns its parents
happen to project, reconstructed at resolve time by `get_node_joins` from
column overlap. Everything between stage 2 and resolve is therefore arranging
projections so that the reconstruction lands on the right answer, and three
stages re-derive "what identifies this group's rows" in three vocabularies that
can disagree. That is the ad hoc quality.

It is not a defect of graph generation. The concept graph carries what a join
plan needs (grain, keys, FD closure, authored relations, lineage). The gap is a
missing artifact: a join plan computed once at stage 2, consumed by stage 3 and
by the MergeNode, instead of a flat "merge grain" that each stage reinterprets.

## 2. Where the join axis is decided today

Three stages, three vocabularies, one answer expected of all of them.

| stage | where | vocabulary for "what identifies this group's rows" | size |
|---|---|---|---|
| 2, group graph | `_final_merge_grain`, `_group_final_grain_contribution`, `_lineage_pinned_grain`, `_refresh_final_contract`, `_consumer_required_input_grain`, `_shared_row_parent_join_keys`, `_rowset_relation_keys`, `_refresh_input_contracts` (`group_graph.py` 644-1190) | `grain_components`, `emitted_grain`, `extent_spans`, lineage walks to a barrier; emitted as flat address sets: `merge_grain`, per-contributor `preserve_keys` / `projection_grain`, per-input `required_grain` / `preserve_keys` | 12 functions, 547 lines |
| 2, topology repair | `_regraft_group_sources`, `_regraft_candidate`, `_terminal_basic_grouping_provider`, `_synthetic_dimension_regraft_parent`, `_absorb_group`, `_merge_basic_into_window_parent` (`group_graph.py` 2326-2788) | ancestor overlap, label equality, `_same_rows` on grain, input coverage; mutates the graph and overwrites `grain_components`, then `_compute_concept_sets` is re-run | 9 functions, 463 lines |
| 3, assembly | `_assemble_final_node`, `_cover_groups_for_mandatory`, `_wrap_for_grain`, `_relevant_root_preserve_keys`, `_resourced_root_contributor`, `_satisfy_parent_projection_contract`, `_widen_merge_join_keys`, `_carry_join_keys`, `_pairing_addresses`, `_pairing_islands`, `_bridge_unpaired_parents`, `_island_bridges`, `_key_bridged_members`, `_keyed_by`, `_pair_is_keyed`, `_fold_covered_contributors`, `_gate_key_row_stream` (`strategy_builder.py` 2385-5752) | built node outputs and pseudonyms, `build_fd_determines`, `node.resolve().grain`, sibling `emitted_grain`, statement relation members | 82 functions, ~3,370 lines |
| resolve | `MergeNode.generate_joins` → `get_node_joins` → `resolve_join_order_v2`; `_raise_if_keyless_row_bearing_join` (`join_resolution.py`) | a bipartite graph of datasource ↔ canonical concept over the parents' non-hidden outputs; `SideFacts.grain` from the resolved datasource | the only place keys become joins |

`MergeNode` accepts explicit `node_joins`. Two sites use it (the multiselect
align, and `_with_null_members`). Every other merge, FINAL included, is an
auto-join: the planner's intent reaches the join only through which columns
survived onto each side. The keyless-join guard at resolve is the sole check
that the intent survived, and when it fires the fix has to be found upstream
by reading the SQL backwards.

Comment density tells the same story: the string "ON 1=1" or "cross-join"
appears 34 times in `strategy_builder.py` and 13 times in `group_graph.py`,
each marking a place where a projection had to be shaped so the resolve-time
reconstruction would not fall to a cartesian.

## 3. What the corpus says

210 statements compile; 45 have a FINAL merge with two or more sides (37 with
two, 6 with three, 2 with four). Statements where each path fired:

| path | statements | stage |
|---|---:|---|
| `_lineage_pinned_grain` returned a non-empty pin | 74 | 2 |
| `_regraft_candidate` found a full-coverage spine | 68 | 2 |
| `_terminal_basic_grouping_provider` re-homed a scalar BASIC | 34 | 2 |
| `_regraft_candidate` found a partial spine | 13 | 2 |
| `_synthetic_dimension_regraft_parent` minted a ROOT bucket | 9 | 2 |
| ROOT `preserve_keys` requested / kept after `_relevant_root_preserve_keys` | 28 / 26 | 3 |
| `_fresh_final_root_projection` re-sourced a ROOT for the merge | 5 | 3 |
| `_pairing_islands` saw two or more islands at FINAL | 5 | 3 |
| `_carry_join_keys` widened a FINAL parent with a join key | 5 | 3 |
| `_wrap_for_grain` emitted a GroupNode at FINAL | 3 | 3 |
| `_bridge_unpaired_parents` added a parent (per-group merges) | 1 | 3 |
| `_satisfy_parent_projection_contract` reshaped a parent (per-group / FINAL) | 7 / 1 | 3 |
| keyless FINAL joins | 4 | resolve |

The four keyless FINAL joins (tpc-ds q14, q23, q24; tpc-h q15) are all a
global-aggregate scalar beside rows, which is the legitimate cross product;
the guard raised nowhere on the corpus.

Two facts about the stage-2 contract:

- **It describes the wrong set of contributors.** `_add_final_node` draws a
  MERGE edge from every group to FINAL, so `_refresh_final_contract` writes one
  `FinalContributorContract` per group and unions every group's grain into
  `merge_grain`. The contributors that actually merge are elected in stage 3 by
  `_cover_groups_for_mandatory`, from built nodes. On the issue-769 shapes the
  contract lists 7 or 8 contributors; assembly merges 2 or 3. In 7 of the 45
  corpus FINALs the side count at the merge differs from the elected cover
  (sides added by filter-arg parents and bridges, or removed by folds), so
  even the cover is not what the merge sees.
- **Its merge grain is not the join axis.** Comparing `merge_grain` to the keys
  `get_node_joins` actually paired on, under the environment's value classes:
  9 of 45 agree exactly; in 8 the merge used a key the contract never named
  (statement relation members, rowset-side spellings, keys a ROOT was
  re-sourced to carry); in 34 the contract named keys no join used, because
  `merge_grain` is also the output grain of every grouping output and the
  grain of every non-contributing group.

Churn since 2026-07-01: 36 commits touched `strategy_builder.py` and 27
touched `group_graph.py`; `_widen_merge_join_keys` alone was modified in 7 of
them, `_final_merge_grain` in 4.

### PR #770 as the worked case

`select site_key, event_date, pages_per_session, updated_through` over the
issue-769 model. Stage 2 says `merge_grain = {event_date, pageview_id,
site_key}`, because the FILTER groups (`pageview_id ? bot_reason = 0`) sit at
`pageview_id` grain and every group feeds FINAL. Stage 3 elects two
contributors: the BASIC ratio (rows at `(site_key, event_date)`) and the `<*>`
scalar. Before the PR, `grain_components` of the ratio, which says where the
value is computable, was taken as what its rows carry, so the scalar was asked
to preserve `pageview_id`, re-sourced at fact grain, and keyed onto a side that
never held `pageview_id`. The fix introduced `emitted_grain` (grain ∩ row
columns) and switched three readers to it; about 40 readers of
`grain_components` across `condition_placement.py`, `group_behaviors.py`,
`concept_graph.py` and `group_graph.py` were left on the old meaning, each a
candidate for the same latent confusion. The `<*>` case needed a second
exemption (`SINGLE_ROW` outputs preserve no key), and the fourth shape needed
a third change (`_island_bridges` trying every candidate instead of the
most-derived one). Three independent rules, in three functions, for one
missing fact: which keys each contributor's rows carry, and which pairs of
contributors pair on which of them.

## 4. Root causes

**R1. `grain_components` means two things.** For a GROUPING group it is the
GROUP BY, an emitted row identity. For a BASIC, FILTER or ROOT it is where the
value is well defined, an FD statement, and says nothing about the columns the
rows hold. `emitted_grain` names the distinction for three readers; the field
itself still carries both meanings.

**R2. The FINAL contract is computed before the contributors are known** (every
group has a MERGE edge to FINAL), so it can only be a union of grains, not a
pairing. Stage 3 then has to re-decide the axis per contributor from sibling
grains (`sibling_grain`), FD tests (`_relevant_root_preserve_keys`), and the
built nodes' outputs.

**R3. Join keys are emergent from projection.** Nothing in the plan states a
join; `get_node_joins` infers one from shared non-hidden outputs. So "give this
side a join key" is implemented as "make this side project that column"
(`preserve_keys`, `_carry_join_keys`, `widen_projection`, a fresh ROOT
re-source), and "two sides pair on nothing" is detected after the fact
(`_pairing_islands`) and repaired by pulling in a third node
(`_bridge_unpaired_parents`). Each repair reasons about built nodes, after
sourcing, with information stage 2 had.

**R4. Stage 2 repairs its own topology.** `_regraft_group_sources` runs after
`_compute_concept_sets`, re-points lineage edges, overwrites a group's
`grain_components` with its provider's, mints synthetic ROOT buckets, and the
concept sets are recomputed. The group graph is built, inspected, patched and
rebuilt rather than derived once from the concept graph.

**R5. The one principled join model is thrown away at emission.** Inside a
group, the network search models joins explicitly (`network_topology`:
functional edges, the minimum-blend spanning tree, `paired` and `colocated`
obligations). Its chosen structure is not handed to the MergeNode;
`_emit_bridge` passes sources and connectors and the keys are rediscovered at
resolve. The v4 design doc already lists "emission is still an adapter" as
open item 6. The FINAL merge never had such a model at all.

## 5. Could stage 2 have stated the joins? A measurement

`join_census.predicted_keys` is a 20-line rule over the stage-2 attrs of the
cover contributors, nothing from built nodes:

- `identity(g)`: a grouping group's `emitted_grain` (its GROUP BY, or a
  rowset's grain); a ROOT's `anchor_keys` (dim peel key) and `extent_spans`;
  a row-preserving group's carried grain.
- `carries(g)`: a grouping group's rows hold its outputs; a row-preserving
  group's rows also hold its inputs and members.
- Two contributors pair on `(identity(a) | identity(b)) & carries(a) &
  carries(b)`; the FINAL's keys are the union over pairs.

Against the keys `get_node_joins` actually used, canonicalized:

| outcome | FINAL merges |
|---|---:|
| exact | 38 |
| under-predicted (the merge used a key the rule did not state) | 5 |
| over-predicted (the rule stated a key the merge did not use) | 2 |

The seven misses each name a rule the model lacks, and each is a stage-2 fact:

- **Authored statement relations** (`subset join bought.store_sales.customer.sk
  = customer.sk`, `subset join` on ranks): q44, q46, hackernews adhoc03. A
  relation's members are a pairing by declaration; `_final_merge_grain`
  already special-cases this under `relation_edge_members`.
- **FD-carry**: q41 pairs the FILTER (rows at `item.sk`) with an aggregate by
  `item.manufact` on `manufact`, which the filter does not read but its row
  key determines. `carries(g)` must include what `identity(g)` FD-determines,
  which is exactly what `_relevant_root_preserve_keys` and `widen_projection`
  do at stage 3 with `build_fd_determines`.
- **Cover changes after election**: q29, q59, q64. Folds and filter-arg
  parents change the side set after the cover is elected; the rule was
  applied to the cover, the merge saw something else. Under the design these
  are cover decisions, so the mismatch disappears by construction.

So the information is there at stage 2, and a rule short enough to read
reproduces 84% of the physical layer's choices outright, with the remainder
accounted for by three named rules. That is the evidence that the repair
family is re-deriving something that can be stated once.

## 6. Design: a join plan owned by stage 2

One object, computed once, after the contributors are known:

```
RowIdentity(gid) -> frozenset[str]          # one rule per derivation class
Carries(gid)     -> frozenset[str]          # columns the rows hold, FD-closed over the identity
Pairing(left, right, keys, kind)            # kind: LOOKUP | BLEND | AUTHORED | SCALAR
JoinPlan(contributors, pairings, carry_demands: dict[gid, frozenset[str]])
```

- `RowIdentity`: GROUPING → GROUP BY keys; ROWSET → its grain; ROOT → its
  anchor keys / dim keys / region spans, else the KEY concepts among its
  members; BASIC/FILTER → the identity of the row parent it preserves. This
  replaces the second meaning of `grain_components`; the first meaning
  (where computable) stays as `grain_components`.
- `Pairing.kind`: LOOKUP when `keys ⊇ identity` of one side (the pair cannot
  multiply), AUTHORED when the keys are a statement relation, SCALAR when a
  side is `SINGLE_ROW` (the legitimate cross product), BLEND otherwise. A
  BLEND is the thing the keyless guard exists to catch, and the thing
  `_key_bridged_members` repairs by widening or deduping; under the plan it
  is a stage-2 verdict: widen a side's `carry_demands` when FD allows, or
  refuse the cover.
- `carry_demands[gid]`: keys a contributor's pairings need that it does not
  yet carry. This is the one meaning of `preserve_keys`, per edge instead of
  per contributor, and replaces `_final_merge_grain`,
  `_group_final_grain_contribution`, `_lineage_pinned_grain`,
  `_relevant_root_preserve_keys` and the `sibling_grain` logic in
  `_assemble_final_node`.
- **Connectivity is a plan check.** The pairing graph over contributors must
  be connected once SCALAR sides are removed. A disconnected plan is either
  a bridge decision (add the group that pairs the islands to the cover, as the
  network search's `colocated` obligation does within a group) or an error
  raised before any node is built. `_pairing_islands`, `_island_bridges`,
  `_bridge_unpaired_parents` and the resolve-time keyless guard collapse into
  this check.
- **Cover election moves to stage 2.** `_cover_groups_for_mandatory` reads
  only outputs, ancestors, `dim_keys`, `members` and extent ownership; all are
  stage-2 facts. Where a built node's outputs differ from the group's
  `output_concepts` today, that difference is a bug to surface, not a reason
  to elect late. The contract then describes the contributors that merge.
- **The MergeNode receives the plan.** `node_joins` from pairings, so
  `get_node_joins` stops choosing keys and keeps what it is good at: join
  type, direction, nullability provenance, padding guards, region hosting.
  Join *types* stay at resolve; only *keys* move. Until then, a plan-time
  assertion that the discovered keys equal the planned ones gives the same
  guard earlier and with the group ids in the message.
- **Same object for per-group merges.** `GroupInputContract.preserve_keys` is
  today the union of five sets (`required_grain`, `bridge_keys`,
  `grouping_parent_grain`, `domain_spans`, `relation_keys`); it is a
  `JoinPlan` over the consumer's row parents.
- **Regrafts become cover and provider choices.** `_regraft_candidate` and
  `_terminal_basic_grouping_provider` pick a row source for a group by
  ancestor overlap and grain equality; with `RowIdentity` and `Carries` on
  every group that is "the deepest group whose identity equals mine and
  carries my inputs", a query over the graph instead of a mutation of it.

What stays where it is: join typing and padding (`join_resolution.py`
`get_join_type`, `SideFacts`, `_padding_guard`, region hosting), nullability
provenance, `_wrap_for_grain`'s dedup of a wide ROOT to a pairing's grain
(now driven by the pairing rather than by `final_merge_grain`), and the
network search inside a group.

## 7. Status and what is left

**Landed.** `v4_helper/join_plan.py` computes a `JoinPlan` from stage-2 facts
for every v4 merge whose sides are groups:

- the FINAL merge, over the cover `final_cover.elect_cover` picks from what
  each group exposes at stage 2 (its outputs; a row stream also the grain
  keys and property-grain key its rows hold), less what it cannot render
  (`final_cover.unrendered`);
- every per-group merge (`_pre_merge_parents`), over the consumer's row
  parents, with the consumer's input contract `preserve_keys` as each side's
  carry demand. A side that is itself a merge assembled below the consumer
  stands for the groups it joins, and the merge pairs only across sides.

The MergeNode carries the plan's `permitted_keys`, and `get_node_joins` drops
every shared column off it before ordering and typing joins. Join typing,
padding and hosting stay at resolve.

The contract, as the census plugin checks it: a pairing's *keys* are decisive
(what widening carries onto that side); its *implied* columns are what the
keys determine, which resolve may also equate (a side's physical grain is
never pruned). Resolve may prune a planned key; it may never pair off the
plan, nor go keyless where the plan pairs. Over `tests/modeling`,
`tests/complex`, `tests/discovery` and `tests/engine` (3,417 planned
merges), resolve left unrestricted pairs off the plan in 6, each the same
rows: a grand total paired on `all_rows` that the plan broadcasts on 1=1, a
key the planned keys determine (`sale_date` beside `ticket`), and a gcat
date-spine merge that is keyless with or without the plan.

Rules the plan states, each from a suite shape:

- identity and carried columns compare by value class and authored relation,
  never spelling (`ka`/`kb`, `ws`/`week_seq`);
- a row stream carries its row parents' columns, the keys its input contracts
  stitch on, its source grain when no barrier is in its lineage, the other
  arguments of an atom over its own outputs, and any sibling identity key it
  can render or its identity determines;
- a rowset reader carries the boundary under its handles, never the base
  key a body selects; a handle is never renderable off another relation;
- authored axes are a statement fact computed over every group at stage 2
  (`FinalAssemblyContract.authored_axes`), not over the cover;
- a ROOT split into several wraps pairs with itself on what the wraps hold.

**One FINAL plan; bridges elected by it.** The FINAL plan is stated once,
over the cover and every side joined beside it to pair or filter it: the
bridges `join_plan.elect_bridges` picks from the plan's islands, each
filter-only argument's producer and each gate key's row stream
(`_final_extra_sides`). Those extras pair, but fix no merge grain, ask no ROOT
to carry a key, and are never credited a key they would have to be widened to
(a bridge is joined as built). The merge reads the plan over the sides it
joins (`JoinPlan.over`: a contributor folded into another side pairs with
nothing); a side the plan never elected is a planner bug. Per-group merges
take their bridge candidates from the same election over their row parents,
reading the consumer's preserve keys as asks, not columns a side holds (a
dimension peel is never widened to its sibling dimension's key, query64).
Only a candidate whose every pairing is keyed is elected (a filter pairing
an aggregate on its value multiplies both); with none, the plan stays
islanded and the keyless-join guard names the gap. Stage 3 joins the
elected bridges in the plan's order, each widened to the keys the plan pairs
it on (`_join_elected_bridges`), skipping one that no longer joins two
islands as built; it neither searches built nodes for a bridge nor repairs
a member's pairing. Over all of `tests/` the
search had added exactly the elected bridges at 442 of 454 islanded merges;
the other 9 were an elected condition-phase basic whose rows hold the key it
bridges on but whose built node did not project it (an OR of gates by
`status` and by `customer_id`). Widened to its planned key it bridges as
planned, and the SQL is unchanged.

What made the islands honest:

- a row stream beside a grouped parent is credited its row parents' columns
  only within what its grain determines: the parents are projected to the
  stream's grain before they pair (`_satisfy_parent_projection_contract`), so
  `sale_recency` at `(customer, sale_date)` holds no sale line's item
  (`test_partial_grain_with_by_key_aggregate`). Without a grouped parent the
  rows pass as they stand (`item_name as p_name` off the sale lines holds the
  store);
- a row stream reading only region domains holds only their spans, whatever
  its grain or input contract says (`s.sk` off the item domain never holds the
  sale's `order_number`).

The handoff's hypothesis, that ROOT slicing in `parent_for_consumer` made the
plan over-state a stream, did not hold: that slice fires for no over-claiming
stream in the suites. Over the full suite, every bridged merge (81) elects
the bridge the built-output search found, and every FINAL's sides fall inside
its one plan.

A per-group merge is planned once: the consumer's preserve keys are carry
demands only on a side that can hold them (a grouped side its outputs, a row
side what its rows determine; a dimension peel is never widened to its
sibling dimension's key), the plan elects its bridges as extras, and the
merge pairs on that plan over the sides it joins. The FINAL's filter-argument
producers and gate hosts are elected off group outputs and the region domains
the group graph shows each side reading.

What made the cover honest (over all of `tests/`, the stage-2 cover equals
the one filtered by built outputs):

- a row stream exposes the grain keys its rows hold (`emitted_grain`), not
  the grain it is computable at (`f_byitem` at `(item, rid)` holds no `rid`);
  an UNNEST's element grain is no column of it under the merged key's name;
- a filter whose every value is one row (`avg(x) by * ? yr = 1999`) exposes
  no grain;
- a recursive walk carries its own value and the edge it walks, nothing its
  grain key determines (`gen_recursive`);
- an aggregate reads a value through a same-grain grouping twin built before
  it (`GroupAttrs.twin_reused`, set once over the build order). It recomputes
  that value only off a row parent that survives as a feeder, the builder's
  redundant re-scan rule; with none it is built as its population alone, and
  every reader of the lost value loses what it computed off it (a lost value
  is opaque: a reader never re-derives it from its lineage). The named combo
  in `test_v4_window_basic_merge` is the shape.

`_add_region_domain_contributors` elects off group outputs, members and
concept pseudonyms. The relation-axis pass that sat beside it added no
contributor anywhere in `tests/` (the cover and the authored axes already
seat every hosting side) and is gone.

**What a node renders, at stage 2.** `output_concepts` is what readers read
off a group, not what its node renders. `renders.renders` states the latter,
and the FINAL add passes decide off it: partial completion (which side
renders a relation member, complete unless a declared subset side), the
descendant fold (`renders.parents_render`: what a reader's parents render)
and alias promotion (a host renders the rename's arguments, plus the
concepts the cover gave it; grouping rows read off rendered lineage; a
source fed by an EXISTENCE edge is never promoted). Each rule is a measured
shape (computed both ways over all of `tests/`, zero disagreements):

- a one-value group renders its value alone (`avg(x) by * ? yr = 1999`
  holds no `yr`);
- a reader's preserve key is a column only where the group can hold it
  (`join_plan._widenable`), and a rowset handle or relation member only
  under a name the group reads: the basic over side a renders the axis as
  `ka`, never `kc`, whatever FD says;
- a row stream renders its members and, reading source rows, the row keys
  it is grained at (a basic at `tree_id` over a demoted merge key);
- a grouping group renders its outputs and not the keys it carries: an
  aggregate by `kb` over side a renders the axis as `ka`, as the demand pass
  named it. Over the rowsets of a coalescing relation's sides it renders
  each side's handle (`pop:a|b` by `b.k` renders `a.k`);
- a scan or boundary computes an expression relation member off its
  arguments (`cur_period.wk + 1`); a scan rendering one member renders every
  datasource-bindable member (`ka`, `kb`, `kc`), never a rowset handle; a
  boundary renders no other rowset's handle (a deferred WHERE's);
- a reader's parents render to it as the builder slices them: a WINDOW
  slices its ROOT parents to its grain, a row stream beside a grouped parent
  projects its row parents to its grain, and a CONSTRAINT parent is joined
  whole to apply its condition (the select-phase window renders its
  condition twin's `ratio`).

**Bridge candidates from the build order.** `elect_bridges` takes its
candidates from the build order, not from `built`: the groups the order
decides before the consumer (every group, for the FINAL; a group feeding no
output may sort after the FINAL node), still in the plan, that built a node,
most derived first (`_bridge_candidates`). A group folded into its readers
leaves the plan at its turn, so it is never a candidate. Over all of `tests/`
(5,925 elections, 307 bridged) the election from the order differed from the
one from `built` only where the FINAL's slice of the order cut off a group
sorted after it (117, all one shape), and at `root_d1` beside a disconnected
comparison, which builds nothing and must raise the clean disconnected error.
With both rules, the files those came from re-checked equal (1,893
elections).

**The inline fold, decided before the loop.** Which groups every reader
computes inline is decided once, before any node is built
(`_inlined_groups`), in build order over a copy of the plan each earlier fold
is applied to. It reads stage-2 facts only (`_PlannedReads`): what a parent
renders, the derivations above it, and, for a ROOT parent, whether its scan
groups its rows.

**The fold, applied before the loop.** Every fold is applied before the
first group is built, and the build loop no longer reads a node to decide one.
Two facts made that possible:

- A group's build reads its readers off the plan *before* any fold
  (`_GroupBuild.readers`): `arm_local`, `collapse_to_grain`, `existence_source`
  and the filter's intrinsic pushdown. Folding a basic makes its aggregate the
  ROOT's reader, and a ROOT that read the folded graph skipped the dedup it is
  built with (`test_pair_grain_aggregate` grew from 3 CTEs to 6). The readers
  a ROOT is built for are the plan's, whatever reads it after a fold.
- An aggregate's inline of an input that other readers keep is decided with
  the folds, over the folded plan, off the same stage-2 reads
  (`_FoldPlan.aggregate_inputs`, `_aggregate_input_groups`). The built reads
  it used (`_BuiltReads`, `basic_regroups`) are deleted.

A condition-phase twin vetoes a fold only for a value it computes: a row
stream computes a member only off what its row parents render
(`_PlannedReads._computes`). A condition-phase filter over a `root_d1` the
demand pass left without `v` renders no `v ? v > 5`, so it can't stand in for
the folded value (`test_where_on_aggregate_with_ratio_of_aggregates`).

The scan verdict had two bugs, both found by running the stage-2 rule beside
the built one. The search names an equivalence class by its representative,
which may be an alias an earlier statement left in the environment
(after `select supplier_id as sid`, `_virt_func_alias_…` names `supplier_id`).
`scan_regroups` looked that address up in the environment, found nothing, read
the source for no columns, and called it grouped. It also read a class by the
source's binding where the build reads the request's own concept
(`local._cs_ui_cs_ui_item_id` for `cs.item.sk` beside the q64 rowset). It now
names each class as the request does, and otherwise by the column the source
binds, the way the emission does.

Whether a ROOT's scan groups is the network search's answer: the sources it
picks, each read for the columns it provides and the keys it joins on, and
grouped when the datasource is finer than those or declares no grain
(`source_planning.scan_regroups` over `datasource_regroups`, the one rule
`create_datasource_node` applies). The search is pure and memoized on the
request, so asking it at stage 2 builds nothing. A grain read off datasource
metadata alone missed the source a merge reads for its join keys (`items`
read for `(order_id, product_id)` beside `orders` and `products`).

A ROOT renders the columns it scans: its outputs, or its members when the
demand pass left it none. A member the demand pass dropped is no column of it
(`root_d1` holds `v` as a member under `where a > 0`, and a condition-phase
filter folded over it could not read `v`).

Measured: the stage-2 predicate beside the built one over all of `tests/`
(39,679 decisions) disagreed at 25 before the scan rule took the search's
answer; the remaining shapes were a ROOT grouped inside a merge, a
condition-carrying scan regrouped after its WHERE, and the render differences
above.
With it, against main: 1 of 205 corpus plans moved, and it is smaller
(`_q83_with_sales_measure`, 7 CTEs to 4, 7 joins to 5: the return-date
filter folds into its aggregate). A SQL capture over the 3,448 tests in the
files the disagreements came from moved nothing else but CTE names.

Measured (fold up front and aggregate inputs): the stage-2 aggregate-input
decision beside the built one over `tests/modeling`, `tests/engine`,
`tests/complex`, `tests/discovery` and `tests/core` (8,061 decisions) agrees
everywhere. Before the two scan fixes and the twin rule it disagreed at 61. A
SQL capture over the same 6,920 tests against main moved no plan. The 43
statements that differ are column order that follows the hash seed (the
`test_grain_check_exact` validation query; `test_gcat::test_environment`), a
temp-table hash and a test skipped on one side. 0 of 205 corpus plans moved.

**One arm_local rule.** A ROOT's scan verdict at stage 2 reads `arm_local`
as its build does (`_arm_local`): off the plan before any fold, or'd with
`arms_delivered`. The stage-2 rule had read the partly folded copy and
dropped `arms_delivered`; the graphs agreed everywhere, the flag did not.
Over 657 scan verdicts the flag moved and the verdict never did.

**The unreadable-inline guard, at stage 2.** `_inlined_groups` raises when an
aggregate's planned inputs don't render a member folded into it, or a
planned condition-phase twin before it computes one. The built check beside
it flagged nothing over 8,273 aggregate builds and is gone.

**Keyed bridges, at stage 2.** `_key_bridged_members` checked each member
the bridge paired with on stage-3 grains and repaired an unkeyed pair in
place (widen the member to the bridge key, dedupe it, or refuse). Over all
of `tests/` the repairs fired only in their own three unit tests, on
hand-built nodes, and `elect_bridges` fell back to an unkeyed (BLEND) bridge
at 0 of 307 elections. The election is now keyed only and the repairs are
deleted.

Measured: SQL capture over `tests/modeling`, `tests/engine`, `tests/complex`,
`tests/discovery` and `tests/core` against part 9 moved no plan (hash-seed
column order, a temp-table hash, a test whose statement count varies run to
run); 0 of 205 corpus plans moved.

**The FINAL election owes projections; it edits no built node.** Alias
promotion and the region domain pass record, per contributor, the columns
the FINAL hosts on it beyond its build (`projections`), and
`_project_final_contributors` wraps each contributor once. The region pass
fired in 12 statements over `tests/`, all one shape: a ROOT domain, not
otherwise a contributor, takes a rename of its member that promotion had
hosted on an aggregate. The built aggregate exposed the rename only through
promotion's own wrap (12 of 12), so the "hide on the other host" was undoing
a promotion. Moving the rename now drops it from the other host's record.
Asking the domain's scan for the rename instead changes source selection
(54 plans moved, and the domain read a wider scan), so it stays a projection
over the domain. Over the region suites, 9 plans shed the dead rename column
and nothing else moved.

**The fold widen decides off planned renders.** Which BASIC inputs an
aggregate computes on its parent was already read off its outputs. The built
parent was read only by the satisfiability guard, and over `tests/modeling`,
`core`, `engine`, `complex` and `discovery` (289 calls) the stage-2 guard,
over what the aggregate's planned inputs render (`_FoldPlan.rendered` over
`aggregate_inputs`), agreed at every call and never refused. Where the planned
widen set and the built one differ, the built parent already holds the input:
the region path's second call reads the merge its first call widened. The
built parent is now read only to apply the widen.

Measured: SQL capture over the same five suites against part 10 moved the 11
rename plans (smaller, same CTEs and joins) and otherwise only the known noise;
0 of 205 corpus plans moved.

**The FINAL cover is elected before the build loop.** `_elect_final_cover`
runs the cover, partial completion, the descendant fold, alias promotion and
the region domain pass over the build order (minus FINAL, folded groups and
groups none of whose selected columns resolve), off planned renders. Over
`tests/modeling`, `core`, `engine`, `complex` and `discovery`, the election
over the build order equaled the one over `list(built)` at all 10,933
FINALs. The only planned groups that built nothing (2) select nothing, which
stage 2 sees; the other unbuilt shape, a disconnected `root_d1`, raises
before the FINAL.

**The descendant fold owes a passthrough.** `_fold_descendant_contributors`
records the ancestor contributor's columns on the basic (`passthroughs`), and
`_pass_through_final_contributors` widens a copy of the built node before the
FINAL projections wrap it. Over the same suites the fold moved 212
contributors, widened 184 nodes, and never one another built group reads.

Measured: SQL capture over the same five suites (7,515 tests) against part 11
moved no plan (only `test_gcat::test_environment`'s hash-seed column order);
0 of 205 corpus plans moved.

**The FINAL extra sides and bridges read the planned groups.** The groups
`_elect_final_cover` elects from (`_FinalCover.groups`) are also the
candidates `elect_bridges` and `_final_extra_sides` read; neither reads the
build loop's `built` or `unbuilt` any more. Over `tests/modeling`, `core`,
`engine`, `complex` and `discovery`, the candidates, the elected bridges and
the extra sides over the planned groups equaled the built ones at all 3,416
multi-contributor FINALs. A non-FINAL consumer's bridge candidates still drop
the groups built so far that came back empty; over the same suites (31,355
consumers) the planned list differed at 10, all in two tests whose
disconnected ROOT raises (`test_q64_nested_membership_two_source_agg_clean_
error`, `test_where_on_both_islands_raises`).

Measured: SQL capture over the same five suites against part 12 moved no plan
(only the `grain_check` hash-seed column order and `test_validate_fix_types`'
statement count); 0 of 205 corpus plans moved.

**The FINAL merge joins the plan's filter-argument producers.** The plan's
extra sides record the group elected for each address (`_FinalExtras`), and
the merge joins those groups instead of searching `built` again; the gate
host comes from the same record. The merge still decides which arguments it
needs from what its parents hold, because a ROOT contributor may or may not
be re-sourced to its cover depending on the built node's grain
(`_wrap_for_grain`). So the plan counts a ROOT contributor as holding only
its cover, and the merge skips an argument a parent still holds. Over
`tests/modeling`, `core`, `engine`, `complex` and `discovery` (3,438
multi-contributor FINALs, 46 with filter arguments), every argument the merge
needed had a planned producer, and it was the group the `built` search picked.
The plan elected an extra producer at 5 FINALs where the ROOT kept the
argument; the merge skips those. No gate host fired in those suites.

**A ROOT side carries the filter arguments it outputs.** The plan no longer
guesses whether a ROOT contributor's wrap keeps an argument: a filter-only
argument that no side's cover holds but a ROOT contributor outputs is
assigned to that contributor (`_FinalExtras.carried`), which adds it to its
re-source request, so its wrap keeps it. The merge joins exactly the plan's
producers; it no longer reads which arguments its built parents hold. The
sole-contributor path and `_apply_final_conditions` elect producers over the
planned groups, the FINAL's grouping-sets sibling is read off the
contributors' outputs, and a planned FINAL side that built nothing raises
`UnbuiltGroupException` instead of a bare `KeyError`.

Measured over `tests/modeling`, `core`, `engine`, `complex` and `discovery`
against part 14: 46 multi-contributor FINALs with filter arguments, 10
arguments carried by a ROOT side, 39 joined from a producer, and every
producer the plan elected was one the merge needed. The two FINALs where the
merge asks for a region grain key no group produces
(`test_padded_region_derivation_is_null_on_padding`, `count(customer_id) by
*`) dropped it before as well. The sole/apply elections over the planned
groups equaled the built ones at all 393 calls, and the grouping-sets sibling
at all 3,430 FINALs (24 true). SQL capture moved the 5 plans where the merge
re-joined a whole ROOT for an argument that ROOT contributed, all smaller
(`test_union_join_anchor_where_*` 10 to 6 CTEs and 5 to 3 joins, the pivot
subset join 18 to 16 CTEs, `test_dimension_attribute_read_on_the_fact_stream`
3 to 2 CTEs and 2 to 1 joins). Everything else that moved was known noise
(`grain_check` column order, `test_gcat::test_environment`, BigQuery temp
table names, and `test_declared_join_beside_a_property_of_the_superset_key`,
whose join type varies with the hash seed on main too). 0 of 205 corpus plans
moved.

**Every consumer's bridge candidates, the uncovered FINAL and the sole
path's pairing read the plan.** A non-FINAL consumer's bridge candidates are
the planned groups (`_FinalCover.groups`, the build context's `planned`)
decided before it, not the build order minus the groups `ctx.unbuilt` has
recorded so far; an elected bridge that built nothing (a disconnected scan,
`test_disconnected_comparison_raises_clean_disconnected_error`) raises
`UnbuiltGroupException` through `_planned_side`, as a FINAL side does. A FINAL no group covers is the first planned group
(`_uncovered_final_side`), raising `UnbuiltGroupException` if it built
nothing. The sole-contributor path decides whether to apply the FINAL WHERE
over the raw contributor off stage-2 facts: the contributor holds what it
renders plus what the election owes on it (`_FinalCover.holds`), a feeder
what its rows hold or can be widened to (`join_plan.carries`, the plan's own
`_held` term), and its region reads are `_group_region_reads`.

Measured over `tests/modeling`, `core`, `engine`, `complex` and `discovery`:
the planned bridge candidates equaled the built ones at 41,715 of 41,725
non-FINAL consumers, the 10 others in the two disconnected-ROOT raise tests
above. No FINAL went uncovered. At all 251 sole-contributor FINALs with a
deferred WHERE the planned feeder elections equaled the built ones, and the
pairing decision (22 relation-paired, 34 region-paired) agreed. A feeder's
`output_concepts` alone does not: it misses the relation key a rowset
boundary or a pinned row stream carries (`cat_avg.category`,
`ecoregion_id`), 21 decisions; `exposes` still misses the rowset's (1).
The planned region reads miss a domain the built node reads at 4 FINALs
(`test_derived_key_domain`'s `count(customer_id) by * > 2` gate), where no
feeder pairs on a span, so no decision moved.

SQL capture over the same five suites (10,572 tests) against part 15 moved
no plan: only the `grain_check` hash-seed column order, pytest temp-dir and
script paths, and repeat counts. 0 of 205 corpus plans moved.

**The sole path reads where condition placement put the FINAL WHERE.**
Whether a sole contributor already applies the deferred WHERE (an atom also
placed at the aggregate group's pre-filter wrapper, which a ROLLUP
contributor must not have re-applied over its subtotal rows) is now read off
the group graph: the atoms placed at the contributor plus those placed above
it that still hold on its rows (`accumulated_atoms_above`, the same set the
built nodes carry as `preexisting_conditions`). The walk over the built
subtree (`_subtree_applies_conditions`) is deleted.

Measured over `tests/modeling`, `core`, `engine`, `complex`, `discovery` and
the root `tests/test_*.py` files: at all 330 sole-contributor FINALs with a
deferred WHERE the placed atoms agreed with the built walk (32 already
applied). SQL capture over the same suites (12,561 tests) against part 16
moved no plan: only CTE alias names that follow run order (equal when both
sides run the same chunks), BigQuery temp table names, the
`test_declared_join_beside_a_property_of_the_superset_key` join type, and two
tests that compile nothing in an archive export. 0 of 205 corpus plans moved.

The planned region reads still miss the domain the built sole node reads at
4 FINALs (`test_derived_key_domain`, `count(customer_id) by * > 2` beside
`activity = 'dormant'` or `coalesce(flag, 1) = 1`). The walk is not missing
a fact of this graph: no group in it has extent spans. The ROOT hosting the
second atom re-sources its filter argument (`activity`, `coalesce(flag, 1)`)
through a nested plan at stage 3, and that plan has a region domain of its
own (`grp:root:root:∅:extent:local.customer_id`) that this graph never holds.

**The sole path reads an aggregate's planned input rows for a WHERE it
applies before the aggregate.** A sole contributor that aggregates takes the
FINAL row condition on its input rows when those rows hold every row
argument (`_row_condition_before_aggregate`). Before, this was decided by
walking the built node down to its GroupNode and reading the parents'
outputs. The rows are now what the aggregate's input groups carry, less what
its scalar input projection hides: an inlined CASE reads its own arguments
below that projection (`count(status)` beside the statement-wide gate
`count(customer_id) by * > 2` drops the gate column). The projection's rule
is shared with the build (`_aggregate_scalar_inputs`,
`_aggregate_input_keep`). A plan that pushes onto a node that cannot take
the condition raises.

Measured over the same suites: at 281 of 283 sole-contributor FINALs with a
deferred WHERE, the planned rows agreed with the built walk (5 pushes). The
2 others are the `test_v4_window_basic_merge` named-combo FINALs, where the
walk found a grain dedup over the window's own rows and filtered between
them. That was never a push before an aggregate, and the two filters render
the same SQL. SQL capture over the same suites (12,594 tests) against part
17 moved no plan: only BigQuery temp table names, `test_gcat::test_environment`,
a statement `test_validate_fix` compiles in one chunked run and not the
other (both compile 17 run alone), and two tests that compile nothing in an
archive export. 0 of 205 corpus plans moved.

**The FINAL WHERE applier reads what its contributor holds, the regions it
reads and what each feeder carries.** `_apply_final_conditions` picked the
filter-only arguments' producers off the built node's outputs and
`region_reads(node)`, and widened on relation keys off each feeder's built
outputs. It now takes the contributor's planned holdings, its
`_group_region_reads` and each producer's `carries`. The holdings are
`cover.holds(gid)` less what the FINAL dedup projects away: the dedup keeps
the contract's outputs, so a filter-only argument the contributor held is
gone after it and its producer (often the contributor itself, joined back on
its span) is joined in. The dedup's decision and its projection are split
(`_final_dedup_targets`, `_dedup_to_targets`), and `_final_dedup` returns the
projected node with what it keeps. The uncovered FINAL computes the cover's
renders too, so it holds the same way.

Measured over `tests/core`, `engine`, `complex`, `discovery`, each
`tests/modeling` suite and the root `tests/test_*.py` files: 306 FINAL WHERE
applications. At 224 the planned and built answers agreed outright. At 74 the
planned holdings disagreed until they were narrowed by the dedup (67 joined
the contributor back as its own producer, 7 an unnest's ROOT split). At 6 the
feeders' planned relation members differed from their built outputs, but
each touched the same relations, so the widen keys agreed. At the 4
`test_derived_key_domain` FINALs (Open item 2) the planned region reads are
empty where the built node read `customer_id`. The producer there is a
global aggregate and the node already carries the span, so the widen was a
no-op. SQL capture over the same suites (13,156 statements) against part 18
moved no plan: only the `grain_check` hash-seed column order (41), BigQuery
temp table names, `test_gcat::test_environment`, the
`test_declared_join_beside_a_property_of_the_superset_key` join type, the
`test_validate_fix` chunked statement, and two tests that compile nothing in
an archive export. 0 of 205 corpus plans moved.

**The FINAL merge and the aggregate build read planned region spans.** Three
decisions read `region_reads` off built nodes: which spans the
multi-contributor FINAL merge emits as hidden region keys, which of an
aggregate's parents are region domains (and so `domain_spans`), and the
`region_spans` the aggregate build hands `_project_basic_aggregate_inputs`.
Each now reads `_group_region_reads` over the planned groups:
`_planned_region_reads` maps each parent back to its group through `origin`
(raising on a parent that maps to none), and the aggregate's projection reads
its planned predecessors.

Measured over the same suites: 3,784 FINAL merges, 14,797 aggregate domain
choices and 14,770 aggregate projections. All but 2, 4 and 4 agreed. Every
disagreement is Open item 2: in `test_derived_key_domain` and one
`test_rowset_declared_join_pairing` query, the outer ROOT re-sources a gate
argument (`avg(customer_id) by *`, `count(customer_id) by *`) through a nested
plan whose `customer_id` extent group the outer graph does not hold. The 2
FINAL merges emit the same region keys either way. The 4 aggregate
disagreements are one query (`count(customer_id) by * > 2 and activity =
'dormant'`, twice) whose rendered SQL is byte-identical under the swap. SQL
capture over the same suites (13,159 statements) against part 19 moved no
plan: only the `grain_check` hash-seed column order (40), BigQuery temp table
names, `test_gcat::test_environment`, the
`test_declared_join_beside_a_property_of_the_superset_key` join type, and two
tests that compile nothing in an archive export. 0 of 205 corpus plans moved.

**`_parent_nodes_for` reads planned region reads.** Two decisions read
`region_reads` off `built[pgid]`. The first is the descendant-cover check: a
parent is dropped for a descendant or grouping sibling only if that one holds
the parent's regions. The second is the slice guard: a ROOT that reads a
region is never re-sourced as a narrower slice. Both now read
`_group_region_reads` over the planned groups.

Measured over the same suites: 6,591 cover checks and 15,043 slice guards.
Every cover check agreed. 4 slice guards disagreed, all of them Open item 2:
in `test_derived_key_domain` the condition-phase aggregates over
`grp:root:root_d1:∅` see a built `customer_id` read and an empty planned one.
In each of the 4, the slice path the swap takes back declines to slice (it
prunes no source), so the parent is the same copy either way. SQL capture
over the same suites (13,545 statements) against part 20 moved no plan: only
BigQuery temp table names, `test_gcat::test_environment`, the
`test_declared_join_beside_a_property_of_the_superset_key` join type, the
`test_validate_fix` chunked statement, and the `tree_reporting` build tests'
temp file paths. 0 of 205 corpus plans moved.

**The fold widen and the FINAL's expression mates.** The aggregate build's
normalize loop widens an aggregate argument its input never materialized (a
filter virtual) onto the input that renders it, and otherwise keeps the
virtual's row columns in the dedup. Measured over the same suites, 56
arguments reached it. Each time the aggregate had exactly one input: the
stream `_pre_merge_parents` had already assembled. So there is no parent to
pick, only whether that one stream's projection can compute the virtual
below the dedup. A planned fact gave the built answer in 54 of the 56. The 2
disagreements (`test_partition_family_strict_grain_dedup`, both tests) show
why the stage-2 fact is the wrong question. There the planned basic group's
rows hold `tree_id` and `src_label`, so the plan says yes. But its node is a
projection over the ROOT scan, and the scan has no `source_label`. Widening
it would compute the virtual above the projection it has to sit below, so
the fallback is right. The widen is an applied decision: it projects the
merge the plan assembled.

The other `renderable_addresses` reads apply decisions too:

- `_project_basic_aggregate_inputs` gates on the planned renders and reads
  the built stream only to say which BASIC mates the widen computes.
- `_widen_scan_chain` and `_carry_join_keys` widen planned or contracted
  keys down a built projection chain.

The one built read that decided something was the FINAL pass of
`_unprojected_expression_mates` after `_carry_planned_keys`. It picks the
expression relation members two parents reach and none projects. Measured
over the same suites, it fired 11 times, all in
`test_scoped_derived_rowset_join_matrix`. 10 were per-group or gate merges
(`_widen_merge_join_keys`), whose keys the merge discovers. The 1 FINAL hit's
mate was already a key `keys_for` gave both sides, so the pass re-ran the
same carry. The FINAL now carries only the planned keys.

**Nested-plan region domains (closed).** A ROOT that re-sources a filter
argument through a nested plan reads, as built, that plan's region domain.
The planned read is empty. The planned answer is the right one at the outer
plan's level. The nested plan places, pads and joins the region under its
own stage 2: in the `CUSTOMERS_DERIVED` repro (`select customer_id, status
where customer_id >= avg(customer_id) by * and status is null`), every
nested group's built read equals its planned read. What the outer ROOT
returns is the already-united row stream, and nothing in the outer plan pads
or pairs on that region. The built read is the nested node tree's
`region_spans` showing through the plan boundary. Parts 20 and 21 measured
the effect of every outer decision that reads it:

- the FINAL WHERE applier;
- the FINAL merge's region keys;
- the aggregate's domain parents;
- the `_parent_nodes_for` checks.

At every probed case the decision or its SQL came out the same. The rows are
pinned by `test_derived_key_domain`.

SQL capture over the same suites (13,760 statements) against part 21 moved no plan: only the `grain_check` hash-seed column order (41), BigQuery temp table names, `test_gcat::test_environment`, the `test_declared_join_beside_a_property_of_the_superset_key` join type, and the `tree_reporting` build tests' temp file paths. 0 of 209 corpus plans moved.

The rest of `_assemble_final_node`'s reads of `built` apply decisions: the
passthroughs and projections, each contributor's node, bridge nodes by the
planned id, and the ROOT re-source.

The merge, fold and pairing helpers (`_drop_ancestor_parents`,
`_fold_passthrough_parents`, `_fold_covered_contributors`, `_carries_through`,
`_pads_a_value_null_group`, `_pair_inside_padding_streams`) apply decisions
too. They take the node lists a generator or the assembly is about to merge.
In those lists, a node that no planned group built is routine: a scan source
planning made inside a generator, or a merge assembled within one build.
Measured over the same suites, 27,395 `region_reads` calls from these
helpers: 1,538 (5.6%) were on a node whose `origin_group` is unset through
every single-parent wrap. 1,159 of those were scans, 178
merges and 201 GroupNodes. A mapped node can also belong to a
nested plan's graph. What these helpers compare is the region identity of the
rows actually being merged, so they read the built stream.

**Per-group merges carry only planned keys (part 23).** A per-group merge
with a plan (`_pre_merge_parents`) used to run `_widen_merge_join_keys`, which
widened every side to the consumer's preserve keys and to the expression
mates the sides reach, then projected the planned keys. It now does what the
FINAL does: each side carries `JoinPlan.owed_by`, its pairing keys plus the
plan's `carry_demands` (the contract keys it was asked to carry). Two plan
facts had to grow for that:

- a contract key a side renders off what it holds is a carry demand
  (`y = coalesce(s_year, s_year)` onto the fact side under a `merge`), not
  only one its FD closure reaches; without it the `merge` cells of
  `test_shared_attribute_aggregate_matrix` re-derived `y` from the date
  dimension and joined it back INNER, dropping the NULL-date rows;
- a planned key that is an `alias` of a column the side holds is not carried
  again (`team_id` beside `game_tall_team_id`, ncaa adhoc05). Only a rename:
  a value-class mate under `merge` or `subset join` differs in rows or NULLs
  (`test_subset_join_rowset_onto_root`, `test_rowset_declared_join_pairing`).

The contract demands matter on their own too: carrying only the pairing keys
lost the contract's `hour` on hackernews adhoc05. `_widen_merge_join_keys` now
runs only where there is no plan: a merge assembled below the consumer, and
the gate merges. Corpus: 0 of 209 plans moved. Fuzzer 522/522.

**Gate and condition merges read a plan (part 24).** The merges that host a
FINAL-deferred WHERE used to widen with no plan: `_apply_final_conditions`
widened the contributor and its filter-argument producers to the gate's
grain keys, the region spans and every scoped relation a producer touches,
and the FINAL merge widened every side to a gate key no side held. Both now
state those keys as asks (`_condition_join_asks`) and carry each side only
the asks the side's join plan finds it can hold (`_carry_asked_keys`: the
plan's `carry_demands`, the contracted keys a side's outputs or rows hold or
render). Pairing keys are not carried there. A FINAL side is deduped to the
output grain, and its plan carries name the row columns beneath that dedup.
Carrying them pairs `select customer_id, label where sum(amount) by status
> 25 or status is null` with its row feeder on `order_id` and widens the
deduped side to a column it cannot render (`test_null_accepting_atom_keeps_the_padded_row`).
Over the part of the suite the probe reached, these widenings ran 9 times.
Only the relation-key ask ever added a column (7 times: `cat_avg.category`,
`tree_eco_region` under `test_merge_bridge_filter_only_axis`), and the plan
carries the same columns. Corpus: 0 of 209 plans moved, and 0 of 133 tpc
plans. `_widen_merge_join_keys` now runs only where a per-group merge has no
plan.

**A region projection keeps its group (part 25).** An aggregate over a
region merges its solid row parents first, then projects its scalar inputs
over that merge (`_project_basic_aggregate_inputs`) and merges the result
with the domain. The projection widens a *copy* of the solid side, and the
copy was in no `origin`, so the merge with the domain had a side no group
stood behind and fell back to unplanned widening. When the solid rows are one
planned group's, the copy now maps to that group, and the merge is planned like
any other per-group merge. Over the suite, merges with no plan fell from 113 to
20. Before the change, the fallback widening ran 115 times and added a
column once, in a unit test calling `_pre_merge_parents` with no planner
context. Every one of the 20 left has a side that is itself a merge of several
planned groups (an aggregate beside a window, a rowset beside the scan it
pairs with), which a per-group plan cannot name. Corpus: 0 of 205 plans moved, and
0 of 133 TPC-DS/TPC-H plans moved. The full-suite SQL capture moved no plan.
Fuzzer 522/522.

**A side merged below stands for its groups (part 26).** The 20 merges
part 25 left without a plan are the aggregate-over-a-region path: the solid
row parents merge first (an aggregate beside a window, a rowset beside the
scan it pairs with), and that merge is one side of the merge with the domain.
A side is now the set of planned groups whose rows it holds
(`side_members`: its own group, or every group a merge below it joins). The
plan is computed over those groups, and the merge reads it with
`JoinPlan.across`, which keeps only pairings between two sides: the pairings
inside a side were joined below it. Such a side elects no bridge, for the same
reason. A merged side owes the keys its groups pair on across sides. Over the
suite, every merge built with planner context now has a plan. In all 20 the
merged side pairs with the domain on the span (`customer_id`, `ticket`,
`product_id`/`user_id`), as a LOOKUP. With no planner context left to serve,
the unplanned widening (`_widen_merge_join_keys`,
`_unprojected_expression_mates`) is removed. The only call left without a plan
is a unit test of `_pre_merge_parents` given no planner context, and that merge
now widens no parent. Corpus: 0 of 205 plans moved. The full-suite SQL capture
moved no plan: once tmp paths and CTE names are normalized, every remaining
difference also moves between two runs of the same tree (the families above,
plus temp-table hashes and script order). Fuzzer 522/522.

**A condition merge pairs on its plan (part 27).** The merge
`_apply_final_conditions` builds to host a FINAL-deferred WHERE beside its
filter arguments' producers computed a join plan, but only to carry the
asks; the MergeNode itself discovered its keys at resolve. It now receives the
plan's `permitted_keys` across its sides (`_planned_keys_across`), as the FINAL
and per-group merges do. Enforcing the plan exposed one identity the plan
lacked: a pinned value (`grain_pin`) is evaluated on the select's rows, keyed
by its anchors, but its group's identity fell back to the keys its expression
is a function of. `select latitude where city = 'A' and bucket = 'Other'`
paired the row stream with the pinned `bucket` on `ecoregion_id`, a column
the row side never holds, and resolve went keyless. Stage 2 already knew the
anchors (`group_graph`'s `pin_anchors`, the columns a pinned group exposes);
they are now on `GroupAttrs.grain_pin_anchors`, and `row_identity` adds them.
Corpus: 0 of 205 plans moved.

**Two sides pair on their own pairing (part 28).** `permitted_keys` is one
set for the whole merge, so resolve could pair two sides on a key the plan
pairs only between two *other* sides: with A–B planned on `k` and B–C on `k`,
A–C on `k` passed the filter. Every planned MergeNode (FINAL, per-group,
condition host) now also receives `PlannedPairs`, the groups each parent
holds and `JoinPlan.pair_keys()` (each pairing's keys and implied columns, by
group pair). `MergeNode._resolve` maps that onto the sources it joins
(`_planned_source_pairs`), and `resolve_join_order_v2` narrows each pair's
connecting keys to that pair's own (`JoinFacts.pair_keys`). A side that shares
the pivot only with sides the plan does not pair it with waits for a planned
partner under the same pivot, so it is not joined on nothing. A pair with a
parent no planned group stands behind (an unattributed bridge or filter feeder)
keeps the merge-wide `permitted_keys`. So does the whole merge when resolve
folds a parent away as redundant, because no survivor is credited with that
parent's groups (5 of 215 planned merges over the corpus).
Over the suite, 18,010 planned merges reached join ordering, and the per-pair
restriction cut a key in 10 of them outside the new unit tests. 8 are the
tree_reporting city builds (USSTO, USSFO), where a `*_source` key the plan
pairs elsewhere is cut and the emitted SQL is unchanged. One is
`test_misgrained_broadcast_is_loud_not_silent`, which still raises as asserted.
One is `test_list_aggregate_beside_second_species_source`, a strict xfail
whose final merge now pairs the aggregate with the unnest on `species`, as the
plan says, instead of FULL-joining the dimension on `species` too. Its rows
are wrong the same way before and after, because the `1=1` the xfail names is
in an earlier CTE. Corpus: 0 of 209 plans moved. Benchmark growth check
clean.

**A folded parent's groups ride its survivor (part 29).** Part 28 read the
plan per pair only when no parent had been folded away: `deduplicate_nodes`
drops a parent another binds completely, and nothing said which survivor
stood for the dropped parent's groups, so the whole merge fell back to
`permitted_keys`. `deduplicate_nodes` now names the survivor of each fold
(followed through chained folds by `deduplicate_nodes_and_joins`), and
`_credit_folded` adds the dropped parent's groups to it; an unattributed
parent on either side still makes the survivor unattributed. The survivor
binds every column the dropped parent exposed, so the pairings planned on
the dropped parent's groups are keys it holds. Over the suite, 884 merges
folded a parent and 209 of those were planned merges that reached join
ordering; per pair, none of the 209 cut a key the merge-wide set allowed.
Corpus: 0 of 205 plans moved.

**A merge carries one plan (part 30).** A planned MergeNode was handed the
plan twice: `planned_keys` (the plan's `permitted_keys`) and `PlannedPairs`
(its `pair_keys()`), from the same `JoinPlan` at each of the three call sites.
The first is the union of the second's values, so the node now takes only
`PlannedPairs`, and `PlannedPairs.permitted` is what `get_node_joins` drops
every other shared column on. The per-pair map `_planned_source_pairs` hands
resolve is now total: a pair with an unattributed parent is stated with the
merge-wide keys instead of being left out. That is the same restriction as
before, since `_drop_unplanned_keys` already took every unplanned column out of
the graph, but resolve no longer reads two shapes of the plan. A probe over
`tests/modeling`, `tests/complex`, `tests/discovery` and `tests/engine`, cut
short after 4,519 planned merges, found one unattributed parent:
`test_membership_in_having_auto_concept_renders_valid_subselect`, a
GroupNode over a FilterNode over a merge, which `side_members` reads through
only for a bare merge. Corpus: 0 of 205 plans moved.

**A membership set is no side (part 31).** The one unattributed parent the
part 30 probe found was not a wrap the plan failed to read through. It was
the set a `having ws in ws_2001` reads, handed to the condition host as an
existence parent. The host renders it as an `exists (select 1 from ...)`
subselect and leaves it out of `join_candidates`, but
`_planned_source_pairs` still stated a pair for it, with the merge-wide keys.
The per-pair map is now built over the sources the merge joins, so an
existence-only source gets no pair. A probe over all of `tests/modeling`,
`tests/complex`, `tests/discovery` and `tests/engine`, run to the end this
time (9,943 planned merges), finds 18 that still join a parent no planned
group stands behind, in 7 tests. Each is an aggregate feeding a WHERE (a
`[@condition]` group: `sum(target) by bucket`, `sum(amount) by pstatus`, the
set in `where dx in x`) built as a GroupNode that `origin` does not name.
These are real joined sides, so they keep the merge-wide keys. Corpus: 0 of
205 plans moved.

**Every joined side is planned (part 32).** The part 31 probe counted
merges, not shapes. Re-run per joined pair, over the same four directories
(11,758 planned merges), it found 218 pairs with an unattributed side, in 4
tests. None was an aggregate `origin` fails to name. Each was a plan that
lost track of a parent after the merge was built:

- *A rewrite swapped the parents.* `_elide_passthrough_tree` (and
  `_deep_copy_node`) replace a merge's parents after `PlannedPairs` keyed its
  sides by parent identity, so a collapsed ROOT scan beside an unnest was no
  side. 200 of the 218 (`test_relation_reads_the_summary`). The authored
  `node_joins` already followed the swap. `MergeNode.replace_parents` now
  moves both, and the plan is rebound (`PlannedPairs.rebound`).
- *A membership set folded into a joined side.* The set `where dx in x`
  reads is an existence parent. Resolve folded it into the `g, x` group it
  duplicates, and `_credit_folded` read the set's missing side as an
  unattributed survivor. A source that is existence-only for the merge now
  adds no group to what its merged source holds (`_held_groups`).
- *A contributor moved below FINAL.* `_pair_inside_padding_streams` moves a
  FINAL contributor (`sum(target) by bucket as t`, `sum(amount) by pstatus`)
  into the merge where a stream padded a `?` key, after that merge was
  planned. It now enters the plan as a side with its own group, paired with
  the merge's groups on what the FINAL plan pairs it with the stream on
  (`PlannedPairs.joined`).

Over `tests/` with the probe attached, the one pair left with an
unattributed side is `test_unattributed_pair_reads_every_key_the_merge_may_pair_on`,
the unit test of that fallback. Corpus: 0 of 205 plans moved.

**Each preserve-key source shapes a plan (part 33).** §6 makes
`GroupInputContract.preserve_keys` the plan's carry demand, per edge. Before
replacing the union of five sets, each set was dropped in turn, and the SQL
of every statement the suite compiles (31,785 captured, `sqlcap` under
xdist) was diffed against `main`. The 209-statement corpus moved in none of
these runs, so it cannot gate this change. All five sets shape plans:

| set dropped | suite tests failing | plans moved | how |
|---|---|---|---|
| `required_grain` | (the contract's own grain; not dropped) | | |
| `bridge_keys` (KEY branch) | its unit test | USSFO city build, +15% SQL | carries 164 more `_virt_filter_` columns past the dedup CTE |
| `bridge_keys` (axes branch) | none | none on a targeted rerun | one full run moved the class-rejoin plans; it did not reproduce |
| `grouping_parent_grain` | none | 41 class-rejoin and fit-chain plans | the class CASE is re-derived in the aggregate (fit chain: 1 → 10 times) |
| `domain_spans` | none | partial-key assembly; two gcat plans shrink | the fact rows are FULL-joined back on `order_id, line_no` |
| `relation_keys` | 4 `union join` rowset tests | | wrong rows |

None of these moves broke a row assertion, which is why no test held them
until now. Three guards now do: `test_row_value_beside_class_aggregate_derives_the_class_once`,
`test_fit_column_times_a_row_value_derives_the_class_once`
(`grouping_parent_grain`) and `test_by_span_aggregate_never_rejoins_the_line_rows`
(`domain_spans`). Each fails with its set dropped and passes with it. The KEY
branch of `bridge_keys` keeps its unit test, and `relation_keys` keeps the
rowset tests. The union is now computed in one place,
`_consumer_preserve_keys`, which is the seam the per-edge carry demand
replaces. `sqlcap` writes one file per xdist worker and `sqldiff` reads
them back as one capture, which takes the suite oracle from 20 minutes to 7
on four workers. Corpus: 0 of 209 plans moved. Suite capture: no plan moved; the only
diffs are column and GROUP BY order.

**Two sets are a projection grain, not a carry demand (part 34).** Part 33
showed each of the five sets shapes plans, not which reader of the union
it shapes them through. The union has five: the merge plan's per-side
`contracted`, the `needed` it adds to a consumer's sourcing, `renders`'s
asks, a row stream's `_joined_on`, and `_satisfy_parent_projection_contract`,
which projects a dimension parent to a grain before it merges beside an
already-shaped sibling. With a set dropped and added back to one reader at a
time, `grouping_parent_grain` and `domain_spans` are restored by the
projection reader alone and by none of the four key readers;
`relation_keys` is restored by the join-key readers and not by the
projection. No pairing reads the first two: they are the grains the
consumer's siblings already stand at, so they fix the projection and ask no
side to carry anything.

The contract now says so. `GroupInputContract.preserve_keys` is the carry
demand, `required_grain | bridge_keys | relation_keys`
(`_consumer_preserve_keys`). `projection_grain` is that plus the sibling
grains (`_consumer_sibling_grains`), and is all the projection reads. The
three part 33 guards pass unchanged, and
`test_sibling_grains_fix_the_projection_not_the_carry_demand` pins the
split. Suite capture against `main`: 76 statements moved. 58 are known noise
(column and GROUP BY order, worker statement counts, temp-table names). The
other 18 are two shapes:

- 16 class-rejoin and fit-chain plans. A scalar aggregate (`g_b`, `b_pick`)
  is no longer asked to render `all_rows`, so it is not carried through the
  merge, and the consumer selects `1` instead. CTE and join counts are the
  same, the SQL is shorter, and the rows asserted are the same.
- 2 gcat plans, `test_full_join_issue` and `_2`. The satcat side was
  inner-joined to `launch_info` only to stand at `launch_tag` (the
  `~launch_tag` domain's span), then FULL-joined back to `launch_info` on it.
  Now it FULL-joins once: 3 CTEs to 2, and 7 to 6. On the full GCAT data
  the rows are identical (235 and 120).

Corpus: 0 of 205 plans moved.

**A KEY bridge is a sourcing key, not a carry demand (part 35).** Part 34
left `bridge_keys` unplaced: its only guard asserted on the contract. Run the
same way, dropped and then added back to one reader at a time, its two
branches split. The KEY branch (a shared key that is a lineage ancestor of the
consumer's grain) moved 4 suite plans, the USSFO and USSTO city builds: without
it the dedup merge pairs `friendly` and `uneven` on 41 `_virt_filter_` columns
instead of carrying `ussfo_source`, 27 CTEs to 26 and 15% more SQL. Only the
`needed` a consumer adds to its sourcing restores them; the merge plan's
`contracted`, `renders`'s asks and `_joined_on` do not. With the bridge in
`needed` alone, `test_full_join_issue_2` goes back from 6 CTEs to 7, and the
projection reader restores it. The axes branch (a merged attribute a scalar
is grained on) moved no plan: the one test that moved,
`test_declared_join_beside_a_property_of_the_superset_key`, moves between two
runs of `main` too.

So a KEY bridge is read by the sourcing and the projection, and by no
pairing. It moves to `GroupInputContract.sourcing_keys`, which `needed` reads
(`_input_contract_sourcing_keys`) and `projection_grain` includes. The carry
demand, `preserve_keys`, is `required_grain | relation_keys` plus the axis
bridges. `test_city_build_never_pairs_on_filter_columns` guards the city
builds: it fails with the sourcing keys dropped. Suite capture against `main`:
16 statements moved, all of them the known noise (statement counts, column
and GROUP BY order, GCS hash names). Same 25 failures as `main`. Corpus: 0 of
205 plans moved.

**Each reader reads its own set (part 36).** After part 35 the carry demand
was still a union of three sets, `required_grain | relation_keys | axis
bridges`, and all five readers read all of it: `needed` (N), the merge plan's
`contracted` (C), `renders`'s asks (R), `_joined_on` (J) and the projection
(P). Two measurements place each set.

First, a census over one suite run. At each reader it records, per set, the
keys that set adds that nothing else the reader holds already supplies. Where
that is empty for every statement, dropping the set from that reader cannot
move a plan. Three cells are empty: the required grain adds nothing to P
(which reads `required_grain` directly), the axis bridges add nothing to J,
and the relation keys add something to J in 5 tests only.

Second, each nonempty cell was dropped and the files of the tests the census
names were recompiled serially against a serial baseline of the same files.
Under xdist those files move between runs (a statement captured 1 or 4 times,
dashboard statement counts), so the serial pairing is what makes the diff
readable.

| set | N | C | R | J | P |
|---|---|---|---|---|---|
| required grain | | | | 26 plans move, 2 tests fail | adds nothing |
| relation keys | 4 rowset tests fail | rowset pairings move | none | none | none |
| axis bridges | none | none | none | adds nothing | none |

The required grain's N, C and R cells were not dropped; it is the input
grain itself. Two joint drops confirm the singles: the axis bridges dropped
from every reader at once move nothing, nor do the relation keys dropped from
R, J and P together. (The remaining lines in both are TPC-DS tests whose one
statement is captured 1 or 4 times.)

So the contract says which reader reads what:

- `GroupInputContract.preserve_keys` is no longer a field. It is a property,
  `required_grain | relation_keys`, read only by `needed` and the merge plan.
- `renders` asks and `_joined_on` reads only the `required_grain`.
- `projection_grain` is `sourcing_keys` and the sibling grains; the projection
  reads it beside `required_grain`, as before.
- The axis-bridge branch of `_shared_row_parent_join_keys` is deleted.
  `test_merged_derived_bridge_*` and `test_merged_key_reads_summary`, where the
  census saw it add keys, pass with and without it, with the same SQL.

`test_relation_keys_are_sourced_and_paired_never_stitched_or_projected` pins
the reader split; it fails on `main`, where `_joined_on` and the projection
read the relation keys. Suite capture against `main`: 57 statements moved,
all known noise (validation column order, `test_window_laws` ordering, CLI
and dashboard statement counts, temp-table and GCS hash names). The same 25
failures as `main`. Corpus: 0 of 205 plans moved.

**Each side reads its own edge (part 37).** After part 36 the merge plan's
`contracted` still handed every side one set: the union of the consumer's
row-stream contracts' `preserve_keys`. A side need not be a row parent,
though. A region domain, an extent or a basic-input ROOT can be merged in
beside the consumer's row parents with no edge to it, and the union asked
that side to carry keys too. The plan now reads, per side, the carry demand
of that side's own edge (`_input_contract_carry_demands`). A side with no
row-stream edge is asked nothing.

A census over one suite run planned each per-group merge both ways and
compared the plans. Of 12,549 plans, 39 have a side with no edge, and in 31
of them that side's carry demand shrinks. In all 12,549 the pairings are the
same, and no statement's SQL moved, so the widening the union asked of
those sides never changed what they rendered. Only `needed` still reads the
union, and that read is consumer-wide by nature: it is what the consumer
sources, not what a side carries.

`test_a_region_domain_with_no_edge_is_asked_to_carry_nothing` pins the read.
It fails under the union (`main`). Suite capture against `main`, with the
worktree path masked: no plan moved. Serially, `test_two_facts_at_one_grain`
does not move. `test_persisted_value_reads_back` moves between two serial
runs of `main` too (the column order of the grain check). The rest is the
known noise. The same 25 failures as `main`. Corpus: 0 of 205 plans moved.

**An unattributed pair is a planner bug.** Part 32 left a merge-wide
fallback in `_planned_source_pairs`: a pair with a source that holds no
planned group could pair on anything the merge could. With the fallback
replaced by a raise, the full suite fails only that fallback's own unit test.
So it now raises, as the FINAL merge already does for a side the plan never
elected. `test_an_unattributed_pair_is_a_planner_bug` pins the error.

**Each edge is asked for its own side of a relation (part 38).** After
part 37 the merge plan read each side's own edge, but every edge of a
consumer still held the same `relation_keys`: every member of each statement
relation tying a rowset row parent to its siblings. Under `subset join rs.oid
= order_id` the orders scan was asked for `rs.oid` and the rowset for
`order_id`. A rowset handle is a column of its own boundary, so the scan held
it only because it was asked to: it rendered `order_id` a second time, as
`rs_oid`.

Now each edge holds the side of the relation its parent sources
(`_edge_relation_keys`): a rowset parent the members its own handles compute
(`_lineage_rowsets`), any other row parent the members no rowset feeds, and
either one a member it already outputs (a ROOT that carries `sr_data.item_sk`
under a declared join in q29 keeps it). Every relation member still lands on
some edge, so `needed`, the union over the edges, does not change.

A census over one suite run recorded each consumer with relation keys: 32
tests, every member assigned to the edge holding its side, the presence-probe
rowsets of `test_multi_probe_coalescing` included (their members carry the
handle).

Suite capture against `main`, with the worktree path masked, serially where
xdist made a file move between runs: 37 statements in 10 rowset test files
moved, and nothing else beyond the known noise. In 36 of them the CTE and
join counts are the same and the SQL is shorter: the root side no longer
projects the handle as a duplicate column. The 37th,
`test_rowset_alias_collision_distinct_aggregates`, scans `facts` once
instead of twice, and gains one projection CTE (4 to 5) with the same three
joins. The rows asserted are the same. q29 and q64 move under xdist only.
Full suite: the same 25 failures as `main` (credential-gated BigQuery,
Snowflake, GCS and NCAA tests, plus
`the_look::test_wrapped_aggregate_with_dependent_by_grain`); `main` also
failed two `test_q04_generation_determinism` cases under `-n 4`, which pass
serially on both. Corpus: 0 of 205 plans moved. Fuzzer: 522 of 522.

`test_each_side_of_a_declared_join_is_asked_for_its_own_member` pins the
split; it fails on `main`, where both edges hold both members.

**Each edge is asked for the grain its parent can carry (part 39).** After
part 38 every row-stream edge of a consumer still held the same
`required_grain`, the consumer's whole input grain. Two readers then cut
each edge's copy back to what its parent can hold: the merge plan
intersected each side's `contracted` keys with `_widenable` (and what it
computes off those columns), and `renders` intersected each group's asks
with `_widenable` again. The contract now asks only that much
(`join_plan.carryable`). An edge's `required_grain` is the part of the input
grain its parent can carry, and its `relation_keys` are narrowed the same
way. The consumer's whole grain moves to `GroupAttrs.input_grain`. The merge
plan and `renders` read the asks as stated, with no `_widenable` of their
own. The FINAL condition plan (`_condition_join_plan`), which hands every
side one set of asks, narrows them per side with the same function.

The consumer-wide readers keep the whole grain. A census over one suite run
counted 7,682 consumers in 2,753 tests whose input grain holds a key that
none of their edges can carry. So `needed`, `_joined_on`, the dimension
projection and `may_project_dimension` read `input_grain`, not the union of
the edges. Every input grain is set before any edge is narrowed, because
what a parent can carry reads that parent's own `_joined_on`. The same
census recomputed `_widenable` where `renders` reads it and compared it
with the value at contract time. They never differed, so narrowing at the
contract asks no stage-3 fact. 15,300 edges in 5,925 tests are narrowed.

`test_input_contract_declares_projection_grain_with_barrier_sibling` now
pins the per-edge grain: a store dimension beside a by-date aggregate is
asked for `store.id`, not `ride_date`. On `main` it was asked for both.
`test_no_edge_is_asked_for_a_key_its_parent_cannot_carry` pins the
relation side: under `subset join rs.oid = order_id`, the extent scan keyed
by `customer_id` was asked for `order_id`. It held no such column; the
merge plan's intersection had hidden this.

Suite capture against `main`, with the worktree path masked: no plan moved.
`test_assertion_laws::test_check_exact` reorders its generated assertions
between two runs of this branch, and `test_validate_fix_types` changes its
statement count from run to run in either direction. The rest is the known
noise. Full suite: the same 25 failures as `main`. Corpus: 0 of 205 plans
moved. Fuzzer: 522 of 522.

**The projection and sourcing sets are the consumer's (part 40).** After
part 39 every row-stream edge still held the same `projection_grain` and
`sourcing_keys`, computed once per consumer. No reader reads them per edge:
`needed` unions `sourcing_keys` over the edges, and the projection reads
`input_grain | projection_grain` once any present edge has
`may_project_dimension` set. Both now live on `GroupAttrs`, beside
`input_grain`. The edge keeps what is its own: its `required_grain`, its
`relation_keys`, its channel and `may_project_dimension`, which gates the
projection. `sourcing_keys` needs two row parents, so a consumer with none has
an empty set, as the union over no edges was.

Which parent takes the projection is still decided at build time.
`_satisfy_parent_projection_contract` projects no parent that holds a shape
barrier as built (a GroupNode, a WindowNode or a forced group), and projects
nothing unless some parent holds one. A census over one suite run recorded,
at every projection that ran, each parent's `may_project_dimension` beside
its built barrier: 22,551 parent reads in 5,236 tests.

| `may_project_dimension` | barrier as built | parents |
|---|---|---|
| false | yes | 10,041 |
| true | no | 10,859 |
| true | yes | 1,634 |
| no edge | no / yes | 16 / 1 |

The build never projects a parent the flag refuses: every one holds a
barrier. The flag admits 1,634 parents in 791 tests that the build refuses: rowset
reads (595), basic groups over a grouped parent (566), ROOT scans that render
grouped (437), filters (34) and unnests (2). Stage 2 cannot say that yet, so
the barrier check stays. The 17 reads with no edge are a ROOT merged in below
the consumer (a basic-input or extent root) and one unit test.

`test_projection_grain_is_the_consumers_and_gated_by_its_present_edges` pins
the read: the grain is the consumer's, and a merge whose only present parent
may not take the projection gets none.

Suite capture against `main`, with the worktree path masked: no plan moved.
Of the 79 statements that moved, the ones off the known-noise list were
re-captured serially. `test_mock_composite_grain_fills_combination_space`
changes its column order between two runs of this branch, and four more move
between two runs of `main`. The BigQuery temp table's name hashes the
worktree path. Full suite: the same 25 failures as `main`. Corpus: 0 of 205
plans moved. Fuzzer: 522 of 522.

**Whether a per-group parent's rows are shaped is a stage-2 fact (part 41).**
After part 40 `_satisfy_parent_projection_contract` still walked each built
parent for a GroupNode, a WindowNode or a forced group, both to decide whether
any sibling was shaped and to skip the parents that were. A group now carries
`GroupAttrs.shapes_rows`, set once over the group graph in topological order
(`_refresh_shapes_rows`): the group aggregates or windows, it is a rowset
boundary whose body does (any of the rowset's columns crosses a grouping
derivation), or a group below it is shaped. A per-group merge reads that
fact, and one build fact beside it: `_renders_grouped`, a forced group in the
parent as built, the ROOT deduplicated at its datasource that stage 2 does
not choose. The FINAL merge still reads the built parents.

A census over one suite run recorded both answers at every per-group
projection with a projection grain: 29,719 parent reads in 4,766 tests.

| built barrier | stage-2 read | parents |
|---|---|---|
| no | no | 17,969 |
| yes | yes | 11,703 |
| yes | no | 45 |
| no | yes | 2 |

Of the 11,703 agreeing barriers, 864 hold a forced group; the rest are named
by `shapes_rows` alone. The 45 the stage-2 read misses, in 27 tests, are
shapes a condition carries into a scan rather than lineage: an existence
set's dedup (a GroupNode over a ROWSET or FILTER feeder, `then where` stages,
q14, q23), an aggregate a WHERE reads off a derived grain, and a window or
constant group behind a merged derived bridge. The other 2 are ROOT scans in
one test (`test_aggregate_by_a_derived_grain_in_where`): a shaped group is
among their graph predecessors, and the build renders them plain. None of
the 47 moves a plan in the suite capture.

`test_a_parent_whose_stage_two_rows_are_shaped_is_never_projected` pins the
read: a sibling whose built node holds no barrier but whose group is shaped
is not projected, and still licenses projecting its dimension sibling.
`test_parent_projection_uses_declared_contract_not_shape_barrier` now reads
the aggregate's shape off its group, not a forced group on its node.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise and the grain check's column order
(`test_fixed_value_lifecycle_laws::test_persisted_value_reads_back`, nine
cases, the same reorder as `test_grain_check_exact`). Full suite: the same
25 failures as `main`. Corpus: 0 of 205 plans moved. Fuzzer: 522 of 522.

**Which FINAL contributor is shaped is a stage-2 fact too (part 42).** The
FINAL merge was the last caller that walked its built parents for a
GroupNode, a WindowNode or a forced group (`_contains_shape_barrier`, now
gone). It reads what the per-group merge reads, through `_final_parent_shaped`:
the contributor's group is `shapes_rows`, or it renders grouped
(`_renders_grouped`), or the FINAL projected it itself. The last is the
FINAL's own decision: `_resourced_root_contributor` now returns the source
beside its wraps, and a wrap that is not the source is a ROOT the FINAL
grouped to its merge grain. Two stage-2 gaps the census found are closed:

- `shapes_rows` follows LINEAGE edges only. A condition an aggregate answers
  (`where count(order_id) by cust_id > 1`) reaches the ROOT it filters over a
  CONSTRAINT edge; it removes rows and does not shape them. Before this, 83
  FINAL reads of plain ROOT scans in 19 tests were marked shaped that way.
- A bridge a group's merge plan elects joins that bridge's rows into the
  group (`_bridges_shaped_rows`). In q64 the dimension BASIC over four ROOT
  dims pairs them through the `union join` to the `agg_99` rowset, so its
  rows are that rowset's. Without this the FINAL projected it to the merge
  grain and q64 lost rows.

A census over one suite run recorded both answers at every FINAL merge with
more than one contributor: 21,647 parent reads in 4,181 tests.

| built barrier | stage-2 read | parents |
|---|---|---|
| no | no | 11,031 |
| yes | yes | 10,303 |
| yes | no | 313 |
| no | yes | 0 |

584 of the agreeing reads are ROOTs the FINAL projected itself. Of the 313
disagreements, 291 (53 tests) are ROOT scans built as an INNER join to the
condition aggregate that filters them: the built read counted the join as a
shape, and none of them moves a plan. 14 (7 tests) are ROOTs the FINAL
re-sourced or rewrapped where a rowset or subset join reads them. 6 are a one-row scalar subquery
rowset whose body is a constant select, 1 is a non-correlated subselect and 1
is a ROOT in `test_window_clone`.

Four plans moved, all in the 14, all smaller, and every one in a test that
checks rows. A rowset full-joined to a dimension key
(`test_scoped_join::test_rowset_full_join_to_complete_dimension_key_yields_all_rows`)
no longer joins the rowset twice: 5 CTEs to 4, 4 joins to 3. The same shape
drops a CTE and a join in `test_rowset_cross_datasource_outer_read`
(`full_k_aw`, and its matrix case) and in both statements of
`test_rowset_body_limit::test_full_readback_row_identical_under_narrowing`.

`test_a_condition_aggregate_does_not_shape_the_rows_it_filters`,
`test_a_final_contributor_is_shaped_by_its_group_or_its_final_projection`
and `test_a_shaped_bridge_shapes_the_rows_its_consumer_merges` pin the three
reads.

Suite capture against `main`: the four plans above moved. Everything else
that moved is on the known-noise list or compiles identically run alone
(`test_validate_fix_types` and thelook `test_one` compiled fewer duplicate
statements under xdist). Full suite: the same 25 failures as `main`. Corpus:
0 of 205 plans moved. Fuzzer: 522 of 522.

**A per-group merge's bridges are elected before the build (part 43).**
Part 42 shaped a group whose merge joins a shaped bridge, but only once that
group was built (`_bridges_shaped_rows`, now gone). It read the bridge off the
built merge, so it marked the consumer and none of the groups reading it.
Each planned group's bridges are now elected once, after the FINAL cover and
before the build loop (`_plan_group_bridges`), and recorded on
`GroupAttrs.bridges`. The election is the build's own (`elect_bridges`) over
stage-2 inputs:

- the sides are the group's row parents (`read_row_parents`), less one that
  is a graph ancestor of another side holding every input it would supply.
  That is the drop `_parent_nodes_for` makes; without it, a ROOT under both
  aggregates of `test_row_gate_on_the_derived_cluster` pairs them and hides
  the bridge the build joins;
- each side covers what it outputs of the group's `input_concepts`;
- the candidates and carry demands are the build's (`_bridge_candidates`
  over the cover's groups, the consumer's `preserve_keys`). A UNION elects
  none.

`shape_through_bridges` then walks the build order and shapes a group whose
recorded bridge is shaped, and every group its LINEAGE rows flow to from one.
It takes the build order, not the graph's, because no edge puts a bridge
before its consumer. It only adds shapes that start at a bridge. It never
re-derives `shapes_rows` over the folded graph, where a group computed
inline has left its readers.

The build still elects its own bridges at each merge. A census over one suite
run recorded both elections at every per-group merge: 171,398 merges in
24,831 tests, with the same bridges in every one. 25 elect a bridge, and 6
of those bridges are shaped: three q64 plans, two `tree_reporting` city builds
and `test_row_gate_on_the_derived_cluster`. Stage 2 marks all 6.

`test_a_shaped_bridge_shapes_its_consumer_and_the_consumers_readers`,
`test_an_unshaped_bridge_shapes_nothing` and
`test_a_merge_side_covered_by_its_descendant_is_not_a_side` pin the pass and
the sides. They replace `test_a_shaped_bridge_shapes_the_rows_its_consumer_merges`.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat, the declared-join
pairing), the GCS test's random file name, the BigQuery temp table and
`test_a_rust_binary_is_a_datasource`, which ran once its binary was built.
Full suite: the same 25 failures as `main`. Corpus: 0 of 205 plans moved.
Fuzzer: 522 of 522.

**The build joins the bridges recorded before it (part 44).** Part 43
elected each planned group's bridges before the build, but the build still
ran its own election at every merge (`elect_bridges` over
`_bridge_candidates`) and only the shapes read the record. `_merge_plan` now
plans the merge with the consumer's `GroupAttrs.bridges` as its extras
(`plan_final_join(..., extras=...)`); it elects nothing. A side merged below
the consumer still takes no bridge, as before. `_join_elected_bridges` joins
them as it did, skipping one that no longer joins two islands as built.

A census over one suite run planned every per-group merge both ways: 13,723
merges whose sides are all groups. 26 carry a bridge, and in each the plan
and its pairings are the same. One merge differs, in
`test_q64_nested_membership_two_source_agg_clean_error`: a ROOT side the
pre-build election paired the two aggregates through built nothing ("no
connected cover exists"), and the build re-elected a filter as a bridge
around the hole. With the record, the merge stays islanded; the statement is
unsourceable either way and raises the same `UnresolvableQueryException`.

`test_the_build_joins_the_bridges_recorded_before_it` and
`test_a_bridge_left_unrecorded_is_never_elected_at_build` pin the read: with
the record erased after stage 2, `select id, cluster_id` hits the keyless-join
guard instead of electing its bridge at build time.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise (column and GROUP BY order, the GCS and
BigQuery names, `test_a_rust_binary_is_a_datasource`, the dashboard statement
count, CLI ingest) and column order in `test_unmodelled_regions` and
`test_derived_key_domain`, which two captures of `main` alone move as well.
Full suite: the same 25 failures as `main`. Corpus: 0 of 205 plans moved.
Fuzzer: 522 of 522.

**Only a source plan's grouping shapes a parent built grouped (part 45).**
`_renders_grouped` was the last shape read taken off a built node, kept in
part 41 for the ROOT deduplicated at its datasource, the one choice stage 2
does not make. It read wider than that: any forced group anywhere in the
parent's built tree. A census over one suite run counted the reads where it
alone marked a parent shaped: 7,586 projections in 3,162 tests. The forced
group came from about fifteen places. A datasource scan grouped to the asked
columns (`finalize_select_node`) and the source planner's rollups and fan-out
regroups (`source_planning`) are datasource choices. The planner's own
forced groups are not: the FINAL dedup and `_at_declared_identity`, a region
domain's dedup in `_build_group`, `_with_null_members`, `_distinct_rows`, and
a generator's grain claim (`gen_basic`, `gen_filter`, `resolve_rowset`).

Which of them decide a projection, measured by projecting each of those
7,586 both ways and then capturing the 25 files whose projections differed:

- dropping the read outright projects 379 merges (355 tests) differently. It
  moves 199 statements, mostly larger, and fails three tests: both
  `test_q29_existence_feeder_dangling` cases and
  `test_probe_reaches_the_scan_below_a_projected_join_target`, whose forced
  group is a datasource dedup two merges below the parent;
- counting only the forced groups source planning builds moves nothing. Every
  statement that moved under xdist is on the known-noise list, and each file
  off it compiles identically serially: `test_grainless_source_identity`,
  `test_derived_key_domain`, `test_rowset_declared_join_pairing`,
  `test_non_benchmark_queries`, `test_q29_existence_feeder_dangling` and the
  tree-reporting dashboard.

So the planner's own forced groups never decide a projection. `plan_source`
is the one entry to source planning. It now marks each node of its plan that
groups datasource rows (`StrategyNode.grouped_at_source`; copies keep it),
and `_renders_grouped` reads that mark. It still walks the parent's tree,
since the q64 variant's mark sits two merges down. But it reads one decision,
made in one place, instead of any `force_group` in the tree. The mark goes on
the grouping nodes, not on the plan's top node, because `gen_root` may
re-wrap or drop that top node. With the mark on the top only,
`test_tvf_union_order_by_grouped_away_column` lost it and its FINAL full
join became a left join.

`test_a_source_plan_marks_the_rows_it_groups` pins the mark, and
`test_only_a_source_plans_grouping_shapes_a_parent_built_grouped` pins the
read: a dimension beside a planner-forced group is left as built, and beside
a source-grouped one it is projected.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat, the declared-join
pairing, CLI ingest), the GCS and Snowflake names,
`test_a_rust_binary_is_a_datasource`, the statement count of
`test_validate_fix` and `test_validate_fix_types`, and the order of
assertion columns in `test_check_exact` and `TestReadCost`. Two serial
captures of `main` reorder those too. Full suite: the same 25 failures as
`main`. Corpus: 0 of 205 plans moved. Fuzzer: 522 of 522.

**Each row provider is elected over the graph stage 2 built (part 46).**
Open item 3 starts here. `_regraft_group_sources` re-homed one group at a
time: it picked a provider, then rewired the graph before picking the next.
So a later pick could read an earlier one's edges, its adopted grain, or a
ROOT it minted. Picking is now separate from rewiring:

- `_elect_row_provider` picks a group's `RowProvider` and changes nothing. A
  provider is the parent the group reads, the grain it adopts from a grouping
  provider, the lineage parents that parent replaces, and the dimension ROOT
  minted for it. The order of choices is unchanged: a terminal BASIC's
  grouping provider, then a full-coverage sibling, then a dimension ROOT,
  then a partial spine. A grain adopted from a grouping provider that is
  still declined (an independent sibling co-grains on it) is read by the
  later choices, as before.
- `_synthetic_dimension_regraft_parent` returns its choice instead of
  dropping edges and adding the minted bucket itself. `_regraft_candidate`
  takes that adopted grain explicitly.
- `_regraft_group_sources` elects every provider first, then applies them
  all (`_apply_row_provider`).

A census over one suite run applied every regraft both ways, one at a time
and elected-then-applied, and compared the resulting graphs (nodes, edges
and their kinds, every grain, the buckets): 70,608 regraft passes, 7,280
elected providers in 3,331 tests, 308 passes electing more than one. No
graph differed. No pick depended on another, so the order was never load
bearing.

`test_every_row_provider_is_elected_over_the_graph_stage_two_built` pins the
read: every election in a statement sees the same graph and leaves it
unchanged. It fails if the regraft applies each provider as it is picked.
`test_each_elected_provider_becomes_its_groups_lineage_parent` pins the
apply.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat, CLI ingest), the
GCS name, `test_a_rust_binary_is_a_datasource`, the dashboard statement
count, `test_mocking`, and column order in `test_derived_key_domain`, which
compiles identically when run serially. Full suite: the same 25 failures as
`main`. Corpus: 0 of 205 plans moved. Fuzzer: 522 of 522.

**Each window absorber is elected over the graph stage 2 built (part 47).**
`_merge_basic_into_window_parent` was the other half of open item 3. It
folded one scalar BASIC into its WINDOW parent at a time, so a later pick
read the graph an earlier fold had rewritten: the folded group gone, its
readers repointed onto the window. It now elects first and folds after, as
the regraft does since part 46:

- `_elect_window_absorber` picks the WINDOW a BASIC renders inline in, or
  none, and changes nothing. The gates are unchanged: a scalar BASIC with no
  condition atoms and no row-shape barrier member; a full-coverage sibling,
  else a partial spine; the spine a WINDOW at the BASIC's own grain that
  already sources every input the BASIC reads.
- `_merge_basic_into_window_parent` elects every absorber, then folds them
  all (`_absorb_group`). An absorber is a WINDOW, so it is never itself
  folded and no fold removes another's parent.

A census over one suite run folded every pass both ways, one at a time and
elected-then-folded, and compared the resulting graphs (nodes, edges and
their kinds, every grain and member list, the buckets): 70,664 fold passes,
64 folding anything, 68 BASICs folded in 44 tests, 4 passes folding more
than one. No graph differed.

The same run elected the regraft's providers twice in each of those 64
passes: over the graph stage 2 built and over the graph the fold left.
Every surviving group got the same provider both times, and every folded
BASIC would itself have been elected a regraft provider. So the fold is a
regraft onto a WINDOW that also inlines the group, and the two passes can
elect over one graph.

`test_every_window_absorber_is_elected_over_the_graph_stage_two_built` pins
the read over a statement that folds two BASICs into two windows: every
election sees the same graph and leaves it unchanged. It fails if the fold
runs as each absorber is picked. `test_each_elected_basic_folds_into_its_window`
pins the fold.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat, CLI ingest, config
bootstrap), the GCS name and the BigQuery temp table. Full suite: the same 25
failures as `main`. Corpus: 0 of 205 plans moved. Fuzzer: 522 of 522.

**Window absorbers and row providers are elected over one graph (part 48).**
Part 47 left the regraft electing over the graph the fold had rewritten.
Both elections now read the graph stage 2 built, and neither applies until
both are done:

- `_rehome_groups` elects every absorber (`_elect_window_absorbers`), then
  every row provider (`_elect_row_providers`), then folds and applies. It
  replaces the two back-to-back calls in `build_group_graph`.
- A folded BASIC takes no provider: its WINDOW renders it. Part 47's census
  found each folded BASIC would itself have been elected a provider; that
  pick is now never applied.
- No provider is a BASIC (a candidate is neither ROOT nor BASIC, a grouping
  provider is grouped, a dimension provider is a ROOT), so no fold removes a
  provider. A parent a provider drops can be a folded BASIC, whose edge the
  fold moves onto its WINDOW, so the drop names that WINDOW.
- `_merge_basic_into_window_parent` and `_regraft_group_sources` now only
  apply what was elected.

A census over one suite run rehomed every pass both ways, part 47's order
(fold, then elect providers over the folded graph) and one election, and
compared the resulting graphs (nodes, edges and their kinds, every grain,
member list and member depths, the buckets): 70,830 passes, 7,285 elected
providers, 66 passes folding anything, 29 of them also electing providers.
No graph differed.

`test_absorbers_and_providers_are_elected_over_one_graph` pins the read over
part 47's statement: every absorber and provider election sees the same
graph. It fails in part 47's order. `test_a_folded_basic_takes_no_row_provider`
and `test_a_dropped_parent_that_is_folded_is_dropped_as_its_window` pin the
two hand-offs.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat, the declared-join
pairing, config bootstrap), CLI ingest, the dashboard statement counts, the
GCS name and `test_a_rust_binary_is_a_datasource`. Full suite: the same 25
failures as `main`. Corpus: 0 of 205 plans moved. Fuzzer: 522 of 522.

**A window fold is a row provider (part 49).** Part 48 elected absorbers and
providers over one graph, but as two elections with two result maps, and the
provider election had to be told which groups the other one folded. There is
now one election per group, and a fold is one kind of provider:

- `_elect_row_provider` picks, in order: a WINDOW that renders the group
  inline (`_inline_window`), a terminal BASIC's grouping provider, a
  full-coverage sibling, a dimension ROOT, a partial spine. An inline pick is
  a `RowProvider` with `inline=True`.
- `_renders_inline` holds the fold's gates on the pick (a WINDOW at the
  BASIC's own grain that already sources every input it reads);
  `_inline_window` holds the BASIC's own gates (scalar, no condition atoms,
  no row-shape barrier member) and the candidate search.
- `_rehome_groups` folds each inline provider's group, then applies the rest.
  A dropped parent that folds is still dropped as its WINDOW.
- `_elect_window_absorber(s)`, `_merge_basic_into_window_parent` and
  `_regraft_group_sources` are gone.

Why the inline WINDOW comes first, not as a gate on the chain's own pick: a
census over one suite run elected both ways in every pass (70,841 passes).
Of 76 folds, 73 are the chain's own pick, a plain one (no adopted grain, no
drop, no minted ROOT). In the other 3 the chain picks a terminal BASIC's
grouping provider, an AGGREGATE whose grain the BASIC would adopt, and the
fold takes the WINDOW (`test_window_in_having_qualify_render`'s two window
HAVING statements and `test_membership_in_having_no_projected_flag_renders_valid_subselect`).
Gating the chain's pick would move those plans; ranking the fold first moves
none.

The same run classified what each applied provider changes (7,167 applied):
5,000 change nothing (the provider is already a lineage parent, with no new
grain, drop or mint), 1,576 adopt a grouping provider's grain, 1,597 add a
lineage edge (1,102 of them also dropping the parents it replaces), and 237
of those edges are to a minted dimension ROOT. So most of what the regraft elects is already in the
graph stage 2 built.

`test_an_inline_window_outranks_a_grouping_provider` pins the order over the
window HAVING statement: the BASIC's grouping provider exists and is not
picked, and the BASIC folds. It fails with the fold ranked after the
grouping provider. `test_each_inline_provider_folds_its_basic`,
`test_a_provider_that_is_not_inline_keeps_its_group` and the
`_renders_inline` gate tests pin the fold; `test_folds_and_providers_are_elected_over_one_graph`
and `test_a_dropped_parent_that_is_folded_is_dropped_as_its_window` carry
part 48's guards over.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat, the declared-join
pairing), CLI ingest, the GCS name, the BigQuery temp table, the statement
count of `test_validate_fix_types` and `test_a_rust_binary_is_a_datasource`.
Full suite: the same 25 failures as `main`. Corpus: 0 of 205 plans moved.
Fuzzer: 522 of 522.

**A row provider is the rewrite it makes (part 50).** Part 49's census found
most applied picks change nothing. The election now states only the change:
`_as_rewrite` keeps what the graph stage 2 built lacks, and
`_elect_row_providers` keeps a pick only when something is left.

- A parent that is already a lineage parent becomes `None`.
- A grain the group (and its bucket) already holds becomes `None`.
- A drop with no edge left is removed, and so is a mint already in the graph.
- A pick left with nothing is not elected. A fold always rewrites, so it is
  kept as elected.

Applying a pick was already a no-op for every part it drops (`_apply_row_provider`
checks each edge and node first), so no graph can move. A census over one
suite run applied every rehome both ways, unfiltered and rewrite-only, and
compared the resulting graphs (nodes, edges and their kinds, every grain,
member list and member depths, the buckets): 70,902 passes, no graph
differed. Of 7,393 picks, 2,301 are rewrites:

| rewrite | picks |
|---|---|
| new parent, grain and drop | 876 |
| grain only | 553 |
| new parent only | 360 |
| new parent, drop and minted ROOT | 237 |
| new parent and grain | 161 |
| fold | 80 |
| grain and drop | 18 |
| new parent and drop | 10 |
| drop only | 6 |

`test_a_pick_the_graph_already_holds_is_not_elected` pins the filter over
part 46's statement: it holds a pick the graph already has, and that pick is
not elected. It fails with the filter removed. The `_as_rewrite` unit tests
pin each part it drops.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (`test_persisted_value_reads_back`,
the window laws, gcat, `test_derived_key_domain`, the declared-join pairing,
CLI ingest, config bootstrap), the dashboard statement counts, the GCS name,
the statement count of `test_validate_fix` and `test_a_rust_binary_is_a_datasource`.
Full suite: the same 25 failures as `main`. Corpus: 0 of 205 plans moved.
Fuzzer: 522 of 522.

**A projection's grain is stated before the rehome (part 51).** Part 50
left 1,608 rewrites changing a grain. Every one is a terminal scalar
projection adopting its grouping provider's grain, and that grain does not
depend on which row provider the projection is then given. Stage 2 now
states it as its own step, before any provider is elected:

- `_elect_grouping_providers` elects each terminal projection's grouping
  provider over the graph stage 2 built. A projection a WINDOW renders inline
  takes none, as before: it folds and keeps its grain.
- `_state_projection_grains` places each such projection, and its bucket, at
  its provider's grain. No provider is a BASIC, so no statement reads
  another.
- The row-provider election reads the elected provider instead of electing
  it, and its grain is the group's own. `RowProvider` has no `grain`; a rehome
  only adds lineage edges, drops parents, mints a ROOT or folds.
- `_regraft_candidate` and `_synthetic_dimension_regraft_parent` no longer
  take a grain override: the group's own grain is the stated one.

Once the grain is stated, only one read in the provider election can see
it: `_grain_needed_by_independent_sibling`, which reads the grain of other
FINAL-bound groups. Some of those are projections that have just been given
a new grain. A census over one suite run rehomed every pass both ways: as
on main (the grain adopted inside the election) and with the stated grain.
It then compared the resulting graphs: nodes, edges and their kinds, every
grain, member list and member depths, and the buckets. The first run found 5
passes in 3 ROLLUP and window tests (q70's window over a ROLLUP,
`test_derived_key_domain`'s rollup over a carried key, and a rollup region
domain). In each, two projections are placed at the same provider. Stated
at its grain, each projection read the other as an independent arm
co-graining on it, and so kept a ROOT edge that main drops. A projection
placed at the same provider is grafted onto it too, so it follows the
relabeling, and the check now counts it as related. With that change:
70,880 passes, 1,650 projections stated at a grouping provider's grain
(1,606 of them at a new one), and 1,722 rewrites left in the election. No
graph differed. A control run that skipped main's rehome on its copy did
differ, so the comparison can see a grain.

`test_each_projection_stands_at_its_providers_grain_when_providers_are_elected`
pins the order over `select region, coalesce(sum(amount), 0)`: when the
providers are elected, the projection already stands at its provider's
grain, and that grain is not the one stage 2 grouped it at.
`test_a_folded_projection_takes_no_grouping_provider` and
`test_an_inline_window_outranks_a_grouping_provider` pin that a fold keeps
its grain. `test_a_sibling_placed_at_the_same_provider_is_not_independent`
pins the census finding.

Suite capture against `main`, with the worktree path masked: no plan moved.
What moved is the known noise: column and GROUP BY order (grain checks,
`test_persisted_value_reads_back`, the window laws, gcat,
`test_derived_key_domain`, CLI ingest), the dashboard statement counts, the
GCS name, the BigQuery temp table and `test_a_rust_binary_is_a_datasource`.
Full suite: the same 25 failures as `main`. Corpus: 0 of 205 plans moved.
Fuzzer: 522 of 522.

**A projection is placed at its grouping provider before the rehome (part
52).** Part 51 stated each projection's grain before the election, but the
election still gave it its grouping provider as a row provider: the edge
onto that provider and the drop of the parents it was built from. That pick
reads nothing another election writes, so stage 2 now states it with the
grain:

- `_place_projections` states each projection's grain, then reads every
  placement over the graph stage 2 built (`_projection_placements`) before
  applying any. A projection whose provider's grain an independent sibling
  co-grains on is stated but not placed: it keeps its parents and takes a row
  provider, as before.
- Every projection feeds only FINAL, so a placement changes no other group's
  ancestors, and no other election can see it.
- The row-provider election skips placed projections. It now picks only a
  fold, a full-coverage sibling, a dimension ROOT or a partial spine. A
  stated but unplaced projection still takes no inline WINDOW.
- A provider now drops only ROOT parents (a dimension pick), and no fold
  removes a ROOT, so the election no longer maps a dropped parent onto the
  WINDOW it folds into. A placement's drop is applied before any fold, so the
  folded BASIC's edge is already gone when its readers are repointed.

A census over one suite run rehomed every pass both ways: as on main (grain
stated, provider elected and applied in the rehome) and with the placement.
It then compared the resulting graphs: nodes, edges and their kinds, every
grain, member list and member depths, and the buckets. 71,353 passes, no
graph differed. Of 1,636 stated projections, 1,464 were placed; 820
rewrites are left in the election (1,722 in part 51).

`test_each_placed_projection_reads_its_provider_when_providers_are_elected`
pins the order over `select region, coalesce(sum(amount), 0)`: when the
providers are elected, each placed projection already reads its provider
and is not elected. `test_a_projection_an_independent_sibling_needs_is_stated_but_not_placed`,
`test_a_placed_projection_is_not_elected`, `test_an_unplaced_projection_takes_no_inline_window`
and `test_a_parent_a_placement_drops_is_gone_before_it_folds` pin the rest.

Full suite: the same 25 failures as `main` (the credential-gated BigQuery,
Snowflake, GCS and NCAA tests, and `the_look::test_wrapped_aggregate_with_dependent_by_grain`).

**A window fold is made before the rehome (part 53).** Part 52 left the
fold as the election's first pick, and the only one that deletes a group.
Whether a WINDOW renders a scalar BASIC inline reads nothing a placement or
another fold writes, so stage 2 now elects every fold once, with the
grouping providers, and makes them before any row provider is elected:

- `_elect_folds` maps each scalar BASIC a WINDOW renders inline
  (`_inline_window`, gates unchanged) to that WINDOW, over the graph stage 2
  built. A fold target is a WINDOW, so no fold removes another's.
- `_elect_grouping_providers` takes those folds and skips them: a fold
  outranks a grouping provider (part 49), so a folded projection keeps its
  grain. It no longer computes the inline window itself.
- `_fold_groups` folds them after `_place_projections` and before
  `_rehome_groups`. A placement's drop is still applied first, so the folded
  BASIC's edge is gone when its readers are repointed.
- A placement cannot change an inline pick: it moves only edges into a
  terminal projection, which feeds only FINAL and is a BASIC, never a
  candidate. So electing the folds over the graph stage 2 built picks what
  part 52 picked over the placed graph.
- The row-provider election runs over the folded graph and picks only a
  full-coverage sibling, a dimension ROOT or a partial spine. `RowProvider`
  has no `inline` flag; `_folds` is gone, and `_as_rewrite` has no fold
  case. A rewrite now only adds a lineage edge, drops ROOT parents or mints
  a ROOT: none deletes a group.

What can move: a reader of a folded BASIC is now elected over the graph
where that BASIC is gone and its edge comes from the WINDOW. A census over
one suite run (`-m "not adventureworks_execution and not cloud_live"`,
`*_laws.py` ignored, as CI runs it) rehomed every pass both ways: part 52's
order (elect over the placed graph, then fold, then apply) and the folds
made first. It then compared the resulting graphs: nodes, edges and their
kinds, every grain, member list and member depths, and the buckets. 12,540
passes, no graph differed. 66 passes in 52 tests fold 74 BASICs; 2 of them
also elect a row provider, and 467 rewrites are elected in all. Part 47's
census, which elected over the unfolded and the folded graph, found the same
providers for every surviving group.

`test_every_fold_is_made_before_any_row_provider_is_elected` pins the order
over part 47's statement: when the row providers are elected, no folded
BASIC is in the graph and every fold target is. It fails with the fold
applied after the election. `test_a_fold_outranks_a_grouping_provider`
carries part 49's window HAVING guard over, `test_a_folded_basic_takes_no_grouping_provider`
and `test_the_row_provider_election_takes_no_inline_window` pin the
hand-off, and `test_a_parent_a_placement_drops_is_gone_before_it_folds` now
reads `_fold_groups`.

**A dimension is read before the rehome (part 54).** Part 53 left the
dimension pick as the only rewrite that drops parents or mints a group. It
ranks below a full-coverage sibling and reads nothing a sibling or spine pick
writes, so stage 2 now makes it as its own step, after the folds and before
the row-provider election:

- `_elect_dimension_reads` picks, for each unplaced BASIC with no
  full-coverage sibling, the dimension ROOT it reads
  (`_synthetic_dimension_regraft_parent`, gates unchanged), over the folded
  graph. `_read_dimensions` elects every read before applying any.
- A read that changes nothing still settles its group: as before, a group
  given a dimension pick never falls through to a partial spine.
- `_rehome_groups` skips placed and read groups (`settled`). The row-provider
  election picks only a full-coverage sibling or a partial spine, and no
  longer takes buckets or concept attributes. Every rewrite it makes adds a
  lineage edge and nothing else.

What can move: a sibling or spine pick is now elected over the graph where
the reads are applied, so a group downstream of a read sees the minted ROOT
in its ancestors and not the ROOTs dropped. A census over one suite run
(`-m "not adventureworks_execution and not cloud_live"`,
`*_laws.py` ignored, as CI runs it) rehomed every pass both ways: part 53's order (one election of
sibling, dimension and spine, then apply) and the dimensions read first. It
then compared the resulting graphs: nodes, edges and their kinds, every
grain, member list and member depths, and the buckets (compared by value).
12,563 passes, no graph differed, and in every pass the
groups read were exactly the groups part 53 gave a dimension pick. 192 reads
were elected; 103 of them rewrite the graph, in 103 passes over 73 tests, and
101 of those mint a ROOT. 488 rewrites are elected in all. In 73 passes a
dimension pick sits beside another group's row-provider pick, the case where
the order could matter. A control run over `tests/core/processing` that
skipped the reads differed in all 10 passes with a dimension rewrite, so the
comparison can see one.

`test_every_dimension_is_read_before_any_row_provider_is_elected` pins the
order over `adhoc04`, which mints a dimension ROOT: when the row providers are
elected, each minted ROOT is already its reader's parent and every ROOT it
replaces is gone. It fails with the reads applied after the election.
`test_a_full_coverage_sibling_outranks_a_dimension`,
`test_a_read_that_changes_nothing_still_settles_its_group`,
`test_a_dimension_read_takes_no_row_provider` and
`test_the_row_provider_election_takes_no_dimension` pin the hand-off.

**A full-coverage sibling is read before the rehome (part 55).** Part 54
left two picks in the election: a full-coverage sibling and a partial spine.
The sibling pick was already computed over the folded graph, as the reason a
group takes no dimension read, so stage 2 now makes it there, in the same
step as the dimension reads:

- `_elect_reads` picks, for each unplaced regraftable group, a full-coverage
  sibling (`_regraft_candidate`, gates unchanged), else the dimension ROOT a
  BASIC reads. `_apply_reads` elects every read before applying any, and a
  read that changes nothing still settles its group, as before.
- The rehome elects only a spine (`_elect_spine`, `_elect_spines`) over the
  graph with the reads applied, and adds it as one lineage edge.
  `_rehome_groups` no longer takes buckets; `_as_rewrite` is gone, since a
  spine never drops or mints. `RowProvider` is now only a placement or a
  read.

What can move: a sibling is now elected over the folded graph, not the graph
with the dimension reads applied, and a spine is elected over the graph with
the sibling edges added. A census over one suite run
(`-m "not adventureworks_execution and not cloud_live"`, `*_laws.py`
ignored, as CI runs it) rehomed every pass both ways: part 54's order
(dimension reads, then one election of sibling and spine) and the siblings
read with the dimensions. It then compared the resulting graphs: nodes,
edges and their kinds, every grain and group attribute, and the buckets
(compared by value). 12,542 passes, no graph differed. 1,162 sibling reads
were elected; 304 of them add an edge, in 149 tests, the same 304 part 54's
election made. 72 spine edges are added either way. In 17 passes a sibling
edge sits beside a spine or a dimension rewrite. No spine elected was a
full-coverage sibling: in this run every group a sibling covers was read
before the rehome. A control over `tests/core/processing` that left the
sibling reads unapplied differed in all 4 passes with a sibling edge.

`test_every_sibling_is_read_before_any_spine_is_elected` pins the order over
a HAVING on an aggregate grouped by a BASIC key: when the spines are elected,
each sibling read is already its reader's parent. It fails with the sibling
edges added after the election. `test_a_sibling_read_takes_no_spine`,
`test_a_sibling_read_adds_only_its_edge`,
`test_a_held_sibling_still_settles_its_group` and
`test_a_full_coverage_sibling_outranks_a_dimension` pin the hand-off;
`test_a_spine_is_one_lineage_edge` and
`test_a_spine_the_graph_already_holds_is_no_rewrite` replace the
`_as_rewrite` tests.

**A spine is a read (part 56).** Part 55 left one pick in the rehome
election, the partial spine, elected over the graph with the reads applied.
A spine adds one lineage edge and nothing else, and ranks below a sibling and
a dimension, so stage 2 now elects it in the same step as them:

- `_elect_read` picks, for each unplaced regraftable group, a full-coverage
  sibling, else the dimension ROOT a BASIC reads, else a partial spine
  (`_regraft_candidate` with `allow_partial`, gates unchanged), over the
  folded graph. `_apply_reads` elects every read before applying any.
- `_rehome_groups`, `_elect_spines`, `_elect_spine` and the `settled` hand-off
  are gone. The rehome is now three steps over the graph stage 2 built: place
  the projections, make the folds, apply the reads.

What can move: a spine is now elected over the folded graph, not the graph
with the sibling and dimension reads applied, so a candidate downstream of a
read is scored on its ancestors before that read. A census over one suite run
(`-m "not adventureworks_execution and not cloud_live"`, `*_laws.py`
ignored, as CI runs it) rehomed every pass both ways: part 55's order
(sibling and dimension reads, then the spine election over the read graph)
and the spines elected with the reads. It then compared the resulting graphs:
nodes, edges and their kinds, every grain and group attribute, and the
buckets, all by value. 12,513 passes, no graph differed. 172 spine reads were
elected; 72 of them add an edge, in 54 tests, the same 72 part 55's election
added. In 21 passes a spine edge sits beside a sibling or dimension read. A control over `tests/core/processing`
that left the spine reads unapplied differed in all 6 passes with a spine
edge, so the comparison can see one.

`test_every_read_is_elected_over_one_graph` pins the order over
`test_v4_window_input_carry`'s model, which elects spines: every read is
elected over the graph the reads are applied to, and none mutates it. It
fails with each read applied as it is elected.
`test_a_spine_is_asked_for_only_after_a_sibling_and_a_dimension`,
`test_a_sibling_takes_no_spine` and `test_a_dimension_takes_no_spine` pin
the rank; `test_a_spine_is_one_lineage_edge` and
`test_a_spine_the_graph_already_holds_is_no_rewrite` now read `_apply_reads`.

**The rehome reads stated sets (part 57).** Through part 56, the first
`_compute_concept_sets` wrote every group's `output_concepts` and
`input_concepts` onto the group, and the rehome read them back off the
groups. The groups it was reading were also the ones it was rewriting: a
fold absorbs a group, a placement restates a grain and a read mints a ROOT.
So the sets were read as group attributes even though they are a statement
about the graph stage 2 built. Now they are passed as a value:

- `_concept_sets` is the demand pass. It returns a `ConceptSets` (each
  non-FINAL group's outputs, inputs and pin anchors) and writes nothing.
  `_write_concept_sets` writes them onto the groups. `_compute_concept_sets`
  is the two steps together, still run once after injection.
- `build_group_graph` states the sets, makes the folds, places the
  projections and applies the reads, all reading that one `ConceptSets`.
  It writes the sets onto the groups only after that. The fold, the
  grouping provider, the sibling, the dimension read and the spine
  (`_inline_window`, `_renders_inline`, `_terminal_basic_grouping_provider`,
  `_regraft_candidate`, `_synthetic_dimension_regraft_parent`) take the sets
  as an argument. None of them reads a group's sets.
- Condition injection still reads what the demand pass had each group emit
  (`GroupBucket.carried`), so the write stays ahead of it. A group folded
  away since the sets were stated is skipped.

What can move: nothing, if the rehome read no set it had already changed. A
fold leaves the WINDOW's sets as they were, a placement changes only a
grain, and a minted ROOT is never asked for its sets. A census over one
suite run (`-m "not adventureworks_execution and not cloud_live"`,
`*_laws.py` ignored, as CI runs it) built every group graph twice, with `main`'s
`group_graph` and with this one. It compared the results by value: nodes,
edges and their kinds, and every group attribute. 12,410 passes, no graph differed. Over them, 67 folds, 1,124 grouping
providers and 1,457 reads were elected (106 of them mint a ROOT), across
1,301 tests. Within `tests/core/processing`, `main` compared against itself differs in
none of 512 passes, so the comparison is stable. A control that skips
`main`'s reads differs in 21 of them, so it can see a change.

`test_no_group_holds_its_sets_while_the_rehome_elects` pins the order over
`test_v4_window_input_carry`'s model, which elects reads: no group holds
sets while the folds or the reads are elected, and both read one
`ConceptSets`. It fails with the sets written before the rehome.
`test_the_stated_sets_are_written_once_the_reads_are_applied` and
`test_a_folded_group_takes_no_sets` pin the write.

**Injection reads stated sets (part 58).** After part 57, condition injection
was the last step that read the first pass's sets off the groups. It reads what
each group carries (`GroupBucket.carried`: what the demand pass had the group
emit, less its members) off the buckets. So the sets were written onto every
group before injection, and the pass after injection wrote them a second time.
Now injection reads the same `ConceptSets` value the rehome reads:

- `ConceptSets` moves to `models.py`. `ConceptSets.carried_by` replaces
  `GroupBucket.carried` and returns the same columns: a group's stated
  outputs less its members, or its keys if the group was minted after the
  sets were stated (a dimension read's ROOT). `GroupBucket.output_concepts`
  and `GroupBucket.carried` are removed.
- `plan_condition_placements` takes the sets and computes each group's
  carried columns once (`_PlacementScope.carried`). Its two readers of those
  columns, `_uncovered_exposing_output_contributor` and
  `_reads_past_region_domain`, take them from the scope.
- `build_group_graph` no longer writes the first pass's sets onto the groups.
  The pass after injection, `_compute_concept_sets`, now sets each group's
  `output_concepts`, `input_concepts` and `grain_pin_anchors`, and it is the
  only step that does.
- The pass after injection did read the first pass's sets off the groups,
  through a property. A BASIC scalar over the FINAL spine
  (`_widen_mixed_scalar_basic_to_final_spine`) widens to `merge_axes`, which
  reads each contributor's `emitted_grain`, and a non-grouping group's
  emitted grain is its grain within `row_columns`, its inputs and outputs. In
  the first pass those were empty; in the second, the first pass's. That pass
  now takes the first statement as `stated`, and `merge_axes` reads a group's
  columns off it (`ConceptSets.row_columns`, `GroupAttrs.grain_emitted_over`).
  An empty statement reads as the unwritten attributes did. A first cut that
  dropped the write without this changed 157 graphs and failed nine
  `tree_reporting`-shaped builds with a keyless join.

What can move: nothing. For every group, `carried_by` returns what `carried`
returned, because both read the same stated sets and the bucket's current
members, and the FINAL spine reads the statement that was written before.
Raw-filter propagation and phase coloring read no sets. A census over one suite run
(`-m "not adventureworks_execution and not cloud_live"`, `*_laws.py`
ignored, as CI runs it) fingerprinted, by value, every group graph that
`build_group_graph` returned: nodes, edges and their kinds, and every group
attribute. It ran once on `main` and once on this branch, and compared the
fingerprints test by test, pass by pass, both runs under one
`PYTHONHASHSEED` (without one, a run differs from itself). 12,513 passes
over 6,005 tests matched on both sides; 2 differ, both in
`test_trilogy::test_parallel_failure`, whose scripts plan on threads, so
two runs of this branch differ there too. The rest were unmatched because of
the renamed tests in `test_v4_stated_sets.py` and the tpc-ds suite's shared
planning cache, which moves passes between tests. The two runs fail the same
34 tests: the credential-gated BigQuery, Snowflake, GCS and NCAA tests,
`the_look::test_wrapped_aggregate_with_dependent_by_grain`, and the
fingerprinting's own recursion limit on deep set-operation conditions. The
first cut, without the stated spine, differed in 157 passes, so the
comparison can see the change.

`test_no_group_holds_its_sets_until_injection_is_planned` pins the order over
`test_v4_window_input_carry`'s model, with a WHERE added. While the folds, the
reads and the condition placements are elected, no group holds sets, and all
three read one `ConceptSets`. `test_the_sets_are_written_once_after_injection`
pins the single write. These two replace part 57's order tests, and both fail
if part 57's write is restored ahead of injection.
`test_a_group_carries_what_it_is_stated_to_emit` and
`test_a_group_minted_since_the_statement_carries_its_keys` pin `carried_by`.
`test_the_final_spine_reads_the_previous_statement` runs
`test_partition_family_strict_grain_dedup`'s tree model and checks that the
spine reads the statement the pass before it made. It fails if the second
pass is not handed that statement.

**Both passes read one demand (part 59).** The demand pass ran twice with
two configurations. The pass before the rehome took neither the statement's
scoped axis mates nor its relation-edge members (the authored join-axis
members whose collapsed side keeps a computed lineage); the pass after
injection took both, computed between the two passes. Now `statement_demand`
states, once per statement and before the root partition, everything a pass
reads besides the group graph: the concept graph, the select, the rollup-padded
keys, the axis mates and the relation-edge members. That value is a `Demand`,
and both `_concept_sets` calls read the same one:

- `_concept_sets` takes the group graph, its buckets and a `Demand`, plus the
  previous statement. `_compute_concept_sets`, the wrapper that stated and
  wrote in one call, is gone: `build_group_graph` writes the second statement
  with `_write_concept_sets`.
- The relation-edge members are read off the concept graph, which nothing
  after it is built adds a RELATION edge to, so computing them before the
  partition reads the same edges. The axis mates and the rollup-padded keys
  read only the environment.
- The FINAL contract reads the relation-edge members off the same `Demand`.

What can move: the first pass now preserves a grouping group's scoped axis
mates and exposes a relation-edge member's mates, which it did not before. So
the sets the rehome and injection read can differ for a statement with a
scoped join. A census of the group graphs (as in part 58: every graph
`build_group_graph` returned, fingerprinted by value, test by test, both runs
under one `PYTHONHASHSEED`) over `tests/modeling`, `tests/core`,
`tests/complex`, `tests/discovery`, `tests/engine` and the top-level tests
(`*_laws.py` and the credential-gated NCAA suite left out) ran once on
`main` and once on this branch: 9,542 passes over about 4,740 tests matched,
and none differ. 61 passes were unmatched, in tests whose worker the
container ran out of memory on in one run and not the other, and the last
few heavy files did not finish on either side. So the first pass's added
mates change no graph either suite builds: the rehome and injection read
columns no statement in them depends on the mates for.

`test_both_passes_read_one_demand` pins the single `Demand`: both passes over
`test_v4_window_input_carry`'s model, with a WHERE, read the same object.

**Stage 2 reads one demand (part 60).** Part 59 stated the `Demand` once,
but each step of `build_group_graph` still derived its own statement facts
from the concept graph and the environment: both demand passes rebuilt the
lineage parents, the source grains, the pseudonym mates and the authored
relation members a scan may advertise; the input contracts rebuilt the
lineage parents a third time and the key set; injection, the FINAL contract
and the extent-owner election each took the select, the concept attributes
and the environment apart and derived what they read from them. Now the
`Demand` holds all of it, each fact stated the first time a step reads it:

- `Demand` adds `output_addresses`, `key_addresses`, `relation_only`,
  `lineage_parents`, `source_grain`, `pseudonym_mates`,
  `null_extending_relations` (the null-extending statement relations
  condition placement reads) and `scan_relation_members`. A pass filters
  the last by the members its own graph's groups hold.
- `_inject_conditions`, `_refresh_input_contracts` and
  `_refresh_final_contract` take the `Demand` in place of the select, the
  concept attributes and edges, the environment, the relation addresses and
  the relation-edge members. The extent-owner election reads
  `Demand.relation_only`.
- `build_group_graph` states the `Demand` before it assigns groups, which
  read its output addresses.

What can move: nothing. Each fact is the same function of the concept graph,
the select and the environment, and nothing a fact reads is written once the
concept graph is built (`concept_graph.py` writes the concept attributes and
edges; the one later write, `build_group_graph` marking `where_row_arg` before
it states the `Demand`, is a field no `Demand` property reads). So reading a
fact once gives what reading it at every step gave.
A suite capture (`local_scripts/sql_ab/sqlcap.py`, every statement every
test compiles, under one `PYTHONHASHSEED`) ran once on `main` and once on
this branch (`-m "not adventureworks_execution and not clickhouse_server
and not bigquery_execution and not cloud_live"`, `*_laws.py` ignored). Of
13,436 captured tests, every statement matched but one test's, a test that
wrapped `_refresh_input_contracts` by its old signature and now takes the
`Demand`. Both runs otherwise failed the same tests: DuckDB extension
downloads and CLI ingest tests the container cannot run, plus one Rust
build test that only ran on the second because cargo had been built by
then.

`test_every_step_reads_the_statement_once` pins it: over
`test_v4_window_input_carry`'s model, with a WHERE, the lineage parents, the
source grains and the null-extending relations are each derived once per
`Demand`.

**A ROOT's gate rejoin is planned (part 61).** Every FINAL and per-group
merge carries its join plan, but a census of the merges resolve typed keys
for (`generate_joins` with no node joins given), over `tests/modeling`,
`tests/engine` and `tests/discovery`, found stage-3 merges between a group's
rows and a source built for them still unplanned:

| merge | unplanned resolves |
|---|---|
| `gen_root`'s condition-source fallback: a ROOT scan beside the sources it re-plans for its WHERE (`_resolve_root_condition_sources`) | 646 |
| `_project_planned_keys`' key lookup: a parent beside the producers that look an owed key up | 14 |
| a clause applied outside a group graph (`resolve_and_inject_condition`) | 2 |
| an unnest's element filter (`_build_group`) | 1 |

The other unplanned resolves are inside a group: the network search joining
datasources (`source_planning`, about 12,300), which §6 leaves where it is.

The ROOT fallback is the one the comments said was keyed "on whatever the two
share". It was not: each row source is searched for a stated set of keys
beside its arguments, an aggregate gate for its grain
(`_condition_source_search_outputs`, which `_with_condition_source_join_keys`
widens the scan by), any other row argument for its keys, plus the node's own
row identity when the search could be correlated on it. Now those are the
plan:

- `_resolve_row_arg_source` returns each source with its rejoin keys: what it
  was searched for besides the arguments, and the correlation it kept (none
  when the correlated search failed and it was retried without).
- `_resolve_root_condition_sources` returns the plan beside the sources:
  `PlannedPairs.star`, the scan paired with each source on that source's
  keys, and two sources only on keys both pair the scan on.
- Both `gen_root` injections pass it, and `inject_condition_at_node` hands it
  to the merge it builds on either branch.

The key lookup is the same shape: a producer is elected because its grain
is within the parent's outputs (`_key_lookup`), so the merge pairs each
producer on that grain, as a star over the parent.

The last two rows stay unplanned. A clause outside a group graph has no
stated rows to pair, and the element filter's single resolve pairs on the
expanded rows' columns, which nothing states yet.

Suite capture against `main` (`sqlcap`, `-n 4`): the same 25 failures on
both sides. 137 statements moved, every one in a file that moves under
xdist. The oracle sweep (69), `test_derived_key_domain` (21) and
`test_fixed_value_lifecycle_laws` (30) were then captured serially on both
sides. Only `test_persisted_value_reads_back` moved, and it moves between two
serial runs of `main` too (part 37). The rest were `test_window_laws`, the
GCS cache-bust URL and the dashboard statement count. So every rejoin
already paired on exactly its stated keys, and the plan now names the keys
each search was built for.

`test_a_root_gate_rejoins_its_rows_on_the_gate_grain` pins the plan
(`where sum(val) by cat > 10 select id` rejoins on `cat` alone), and
`test_a_star_plan_pairs_spokes_only_on_keys_both_join_the_hub_on` pins the
star.

**The last stated-row merges are planned (part 62).** Part 61 left two
merges unplanned. A probe at each over the whole suite recorded the keys
resolve paired on:

| merge | firings | resolve paired on |
|---|---|---|
| an unnest's element filter (`_join_element_filters`) | 61 | the element and its list (`tree_eco_region`, `native_ecoregions`; `group_region`, `group_regions`) |
| a clause outside a group graph (`resolve_and_inject_condition`) | 2 | the feeder's grain (`state`, a nested select's HAVING over a rowset) |

Neither needs a new fact:

- The element filter's rows are elements read on the rows the unnest
  expands. Those rows' columns are what the build already carries up for the
  filter join (`widen_projection` over the unnest's parents). So the filter
  pairs on the elements (`_unnested_addresses`) and those columns, as a
  `PlannedPairs.star` over the unnest. The list is one of those columns, so
  every firing keeps its keys.
- A clause outside a group graph re-sources its row args as a standalone
  feeder (`resolve_row_sources`). The feeder is a lookup of those args, so it
  pairs on its own grain (`condition_sources.lookup_plan`), the rule part 61
  states for `_project_planned_keys`. A keyless feeder (a `by *` global)
  stays a cross join. Both callers pass the plan: `resolve_and_inject_condition`
  and the rowset boundary filter.

So no merge between stated rows resolves its keys unplanned now. The only
unplanned resolves left are the network search inside a group.

Suite capture against `main` (`sqlcap`, `-n 4`, paths normalized): the same
failures on both sides (credential-gated BigQuery, GCS and NCAA tests; `main`
also failed `test_copy_perf` on timing). 50 tests moved under xdist. The files
that hold planner moves among them (the TPC-DS and TPC-H queries,
`test_where_clause`, `test_complete_binding_lookup`, the tree dashboard,
`test_core_concepts`, `test_merge_discovery`, `test_nullability`) were then
captured serially on both sides. Only TPC-DS `test_queries` moved, and only in
how many times a test compiled its statement (one or four). That count also
moves between two serial runs of `main`, and every statement either side
captured is one the other did. So both merges already paired on exactly
their stated keys, and the plans name them.

`test_an_element_filter_pairs_on_the_element_and_its_row` and
`test_a_having_looks_its_feeder_up_on_the_feeder_grain` pin the two plans.
The second fails on `main`, where the HAVING merge carries no plan.

**Resolve reads the plan pair by pair (part 63).** Part 30 handed the
merge one plan, but `get_node_joins` still read it in two shapes: the
merge-wide union (`PlannedPairs.permitted`), which every shared column was
dropped against, and the per-pair map, which narrowed each pair's
connections. The join tree read the first: a column several sides hold
pivoted all of its holders, though the plan paired only some of them on it,
so a side was gathered under a pivot it could not join on and waited
(`deferred`) or fell through to a keyless join there. Now resolve reads only
the per-pair map:

- `get_node_joins` takes no `planned_keys`. A shared column stays in the
  join graph only where the plan pairs two of its holders on it
  (`_drop_unpaired_keys`).
- A pivot gathers only the holders the plan pairs on it (`_pivot_map`). A
  pair the plan does not state pairs on nothing.
- A side the plan pairs on none of the columns it holds is joined on nothing
  after the tree, so the keyless-join guard judges it. It no longer drops
  out of the tree, or rides a pivot it does not pair on.
- The guard counts the tree's base. A keyless join made before any keyed
  one names no left, so the base never entered the tree, and a keyless join
  onto it was never checked.

`test_misgrained_broadcast_is_loud_not_silent` is the case. With the
exposure guards disabled, a `by *` aggregate carries `cust` and is planned
on nothing. It used to join first, keyless onto the tree's base, which the
guard never saw, and only the render backstop caught the re-grained
aggregate. Now it joins last, and the guard names it while planning. The test
accepts either refusal.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`,
worktree paths normalized): 13,760 tests on each side, the same 25 failures
(the credential-gated BigQuery, Snowflake, GCS and NCAA tests, and
`the_look::test_wrapped_aggregate_with_dependent_by_grain`), plus the
misgrained test above before it accepted the guard's refusal. Three tests moved: a BigQuery
temp-table name, the GCS cache-bust URL and `test_cli_consistency`, each of
which moves between two runs of `main`. So no plan moved: every merge
already paired only where its plan pairs, and resolve now reads that from
the plan instead of from what the sides share.

`test_a_side_the_plan_pairs_on_nothing_joins_on_nothing` pins the fallback.
`test_sides_pair_only_on_their_own_planned_keys` now pivots on `k` between
the two sides planned on it first, with the same pairs as before.

**Resolve searches for no key after its pivots (part 64).** Every column
two sides may pair on pivots its holders (part 63: under a plan, the holders
the plan pairs on it), and every holder of a pivot joins under it. So once
the pivots are joined, a side still unjoined shares no pairable column with
any other side. Yet `resolve_join_order_v2` ran a second key search over the
sides holding a column alone (`solo`), looking for the joined side it shared
the most keys with. That search could never find a key, so every side it
reached joined on nothing. A last loop then did the same for the sides with
no column of their own.

The two passes are now one keyless pass: a side the pivots leave unjoined
joins on nothing, and the keyless-join guard judges it. The pass keeps the
old order (`_keyless_order`): first by the first column only that side
holds, then by identifier. Ordering by identifier alone moved 226 tests, each
only by swapping a cross join's sides (`cte_1 INNER JOIN cte_0 on 1=1`) or a
coalesce's arguments with them. Those moves are not worth a refactor that
changes no key.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`): the same
25 failures on both sides (credential-gated BigQuery, Snowflake, GCS and
NCAA tests, and `the_look::test_wrapped_aggregate_with_dependent_by_grain`).
66 tests moved, each by a statement added or dropped, not changed: the
tree dashboard's `test_dashboard_served_from`, `test_cli_consistency`,
`test_ingest`, the GCS cache-bust URL, a Rust-binary datasource test skipped
on the base run, and `test_validate_fix_types`, which compiles the same
statement on both trees when run alone. So no plan moved.

`test_a_side_sharing_no_column_joins_on_nothing_after_the_pivots` pins the
pass.

**A ROOT's scans join where the search joined them (part 65).** Every
merge between stated rows carries a plan (part 62), but a ROOT's own source
plan did not. The network search picks the scans and records, pair by pair,
the classes it joins them on (`SourceSolution.join_keys`), and the bridge
emitter dropped that record: its MergeNode reached `get_node_joins` with no
plan, and resolve found the keys again by column overlap. A suite probe
counted where every unplanned merge with keys to find was built: 5,416 of
5,790 were bridge merges (`_emit_bridge`), and the rest were the ROOT's other
assemblies, each of which states its own join.

- The bridge carries the search's joins (`BridgePlan.join_keys`), each class
  spelled as every address it holds, and its merge is planned on them
  (`_bridge_plan`): each scan is one side, and two scans pair only on what the
  search joined them on. A regroup of a fanning parent keeps its side.
- A bridge with a parent the search did not choose as a scan stays unplanned:
  a gap-filled scan, whose joins the search never saw (`BridgePlan.scans`),
  and a derived connector. The search binds a connector on its join keys alone, but the
  connector's subplan emits more. Planning such a bridge on the search's keys
  paired the rowset axis with `order_items` on `product_id` alone and gave
  user 3 a padded row (`test_a_bridge_through_a_derived_connector_keeps_its_rows`).
- The assemblies that state their own join are planned on it: a partial
  key's completion against its dimension (`_complete_partial_requested`), the
  finer-filter rollup's key lookup and its joined summary's lookups and gates
  (`_plan_finer_filter_rollup`, `_plan_joined_filter_rollup`), a coalescing
  axis's member sides (`gen_coalescing_axis_node`, every side on the axis
  alone), and a lineage-related cross product, whose components pair on
  nothing (`_cross_component_source`).

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`,
worktree paths normalized): 44,175 statements, the same 25 failures on
both sides (the credential-gated BigQuery, Snowflake, GCS and NCAA tests,
and `the_look::test_wrapped_aggregate_with_dependent_by_grain`). 66 tests
moved, none by a join key or a join type:

- known noise: a BigQuery temp-table name, the GCS cache-bust URL, the tree
  dashboard's `test_dashboard_served_from`, `test_cli_consistency` and
  `test_ingest`, and a Rust-binary datasource test skipped on the base run;
- 42 oracle-law and 15 window-law cases whose select-list, `GROUP BY` or
  coalesce order moved, or which of two equal providers an ON clause reads.
  Run alone, they compile the same on both trees: they move with the order
  the workers run them in.

So no plan moved. An earlier cut planned the connector bridges too, and the
capture caught it: 3 `test_v4_root_partition` and 2 `test_recurse_edge`
plans moved, one of them to a wrong row.

`test_a_bridge_merge_pairs_on_the_search_s_joins` and
`test_a_bridge_parent_the_search_did_not_choose_leaves_the_merge_unplanned`
pin the bridge plan.

**A bridge through a derived connector is planned too (part 66).** Part
65 left a bridge unplanned when one of its parents was a derived connector,
and the suite probe counted 3,318 such merges over 1,721 tests. The search
binds a connector (`connector~<alias>`) on the merged key's class, its
origin's grain and its input keys (`_connector_candidates`), and records its
joins like any scan's. But the connector's subplan returns rows at its own
resolved grain, which can be finer than those keys. In
`test_a_bridge_through_a_derived_connector_keeps_its_rows`, the subplan for
`r.product_id` returns rows at `(product_id, user_id)`. Planned on the
search's keys alone, it paired with `order_items` on `product_id` only and
gave user 3 a padded row.

- `_derived_connector_nodes` records each connector the search chose under
  its search node, so the bridge plan names it as a side (`_bridge_plan`).
- The plan names the groups whose rows are finer than the plan knows
  (`PlannedPairs.grain_paired`, here the chosen connectors). When the merge
  resolves, each pair involving one also pairs on that side's resolved
  grain (`_planned_source_pairs`). This reads the subplan's built grain, but
  only for sides the plan names, never a column two sides merely share.
- Still unplanned: a connector planned for a concept no scan covered, which
  the search never chose (11 merges in the suite), and a gap-filled scan
  (1 merge).

The probe over the branch suite run counts 19,873 bridge merges planned on
scans alone and 3,361 planned through a connector.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`):
the same 25 failures on both sides. 160 tests moved:

- known noise: the GCS cache-bust URL, `test_cli_consistency`, and a
  Rust-binary datasource test skipped on the base run;
- 84 oracle-sweep, 40 oracle-spelling and 16 window-law cases, plus 8
  `test_split_key_gates` and 9 `test_where_hosts` cases. Run on their own,
  all 157 compile the same on both trees (0 moved). They move with the
  order the workers run them in.

So no plan moved. `test_a_bridge_through_a_chosen_connector_pairs_on_its_grain`
and `test_a_grain_paired_side_pairs_on_its_resolved_grain` pin the plan.

**Every merge between scans carries a plan (part 67).** After part 66, a
probe over the full suite (a `get_node_joins` wrapper counting merges that
reach it with no plan, each traced to the site that built it) found 43 such
resolutions, from three sites:

| site | resolutions |
|---|---|
| `gen_select_merge_node`, the pruned-graph scans | 29 |
| a bridge side the search did not choose | 8 |
| a recursive walk joined back to its seed row | 3 |

(The probe counts every resolution, so a merge copied by the rehome or a
consumer counts once per copy.) Each is now planned on what it already
states:

- A connector built for a concept no scan covered (a rowset key merged into
  a dimension key, `rs2.cid` into `customer_id`) pairs with every other side
  on the merged key it supplies (the concept, the alias and the origin) and
  the keys it carries to join (`origin.grain` and `connector_join_keys`).
  Like a chosen connector, it also pairs on its resolved grain. A gap-filled
  scan pairs on the concepts the bridge graph links it to. `_bridge_plan`
  no longer returns None: a side that is no scan or connector the bridge
  planned raises.
- `gen_select_merge_node`'s scans pair on the value classes the pruned graph
  links both to (`_graph_join_plan`). The comparison is by class, never by
  spelling: under a `union join a_x = b_x = rs.a_x` one scan links the merged
  class's virtual concept and the other `b_x`. Scans of separate searches
  (an abstract-grain property sourced on its own) pair on nothing
  (`_joint_plan`).
- A recursive walk whose WHERE reads columns the walk does not carry pairs
  with its seed rows on the edge it walked (the `recurse_edge` arguments).
- A relation rollup (`_plan_relation_rollup`, which the probe missed because
  only the law suites reach it: the guard raised on all 344 of their cases)
  is a star on its summary scan. A lookup joins the scan on its grain, and
  each list's distinct pairs join it on that list's keys.

With every site planned, a merge that reaches join inference with no plan
raises (`MergeNode.generate_joins`). The full suite and the law suites run
with that raise in place, and no test reaches it.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`,
worktree paths normalized): the same 25 failures on both sides. 57 tests
moved:

- known noise: the GCS cache-bust URL, a BigQuery temp-table name,
  `test_dashboard_served_from`, `test_cli_consistency`, `test_ingest`, and
  a Rust-binary datasource test skipped on the base run;
- 47 `test_complete_binding_lookup` cases whose select-list order moved, two
  Snowflake rendering tests and `test_validate_fix_types`, whose CTE names
  moved. Run on their own, all compile the same on both trees (0 of 277
  statements moved).

The law suites, captured the same way: 30,941 statements, 7 tests moved,
all listed noise (`test_value_window_reads_each_row` under xdist, and
`test_check_exact`'s grain-check column order).

So no plan moved. `test_a_bridge_side_the_search_did_not_choose_pairs_on_what_it_carries`,
`test_a_bridge_side_nothing_planned_is_a_planner_bug`,
`test_scans_of_one_search_pair_on_the_classes_the_graph_links_both_to`,
`test_scans_of_separate_searches_pair_on_nothing` and
`test_a_walk_joined_back_pairs_on_the_edge_it_walked` and
`test_the_relation_join_is_planned` pin the plans, and
`test_a_merge_with_no_plan_is_a_planner_bug` the guard.

**Resolve reads every key off the plan (part 68).** With every merge
planned (part 67), `get_node_joins` still took the plan as optional, and
`resolve_join_order_v2` still began from column overlap: it intersected the
columns each two sides share (`find_all_connecting_concepts`) with the plan's
keys for that pair, and pivoted each shared column's holders before filtering
them to the ones the plan pairs on it. With no plan it paired on the overlap
alone.

- `get_node_joins` requires the plan (`planned_pairs`, keyword-only).
  `_planned_pair_nodes` states each pair's keys as the columns both sides
  hold of the classes the plan pairs them on, so `JoinFacts.pair_keys` is the
  merge's whole key set and the only one.
- `resolve_join_order_v2` reads its connections straight off `pair_keys`, and
  its pivots too: a key's holders are the sides the plan pairs on it
  (`_pivot_map`). `find_all_connecting_concepts` is gone. Two sides the plan
  does not pair join on nothing, whatever columns they share, and the
  keyless-join guard judges them.

The join graph still decides one thing: the order keyless sides join in
(`_keyless_order`, by the first column only that side holds). That changes
no key.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`,
worktree paths normalized): 44,826 keys. The failures match (the
credential-gated tests), except a `test_q04_generation_determinism` case
that failed on the base run only. 199 tests moved: 142 oracle-sweep, 18
`test_where_hosts`, 18 `test_complete_binding_lookup`, 10 window-law and
3 `test_split_key_gates` cases, plus a few others. Re-run serially on both
trees, 196 compile the same. The other 3 are listed noise: the BigQuery
temp-table name, the GCS cache-bust URL and `test_cli_consistency`. So no
plan moved.

`test_sides_sharing_a_column_the_plan_does_not_name_join_on_nothing` pins
the rule. The resolve unit tests now state their plans.

**Resolve reads no join graph (part 69).** After part 68 every key and pivot
came off the plan, yet `get_node_joins` still built a networkx graph of
sides and columns, pruned it of every shared column the plan pairs no two
holders on (`_drop_unpaired_keys`), and handed it to
`resolve_join_order_v2`. The graph answered three questions the facts
already held: which sides the merge has, which side holds an active anchor
key, and the order keyless sides join in.

- Each side's columns ride `SideFacts.columns` (the outputs it emits, in the
  merge's spelling), and `resolve_join_order_v2` takes `JoinFacts` alone.
  Its sides are `facts.sides`; an anchor source is a side whose columns hold
  an active anchor key; a keyless side joins in the order of the first
  column no other side holds (`_keyless_order`).
- `get_node_joins` builds no graph. `_planned_pair_nodes` reads each pair's
  keys off the two sides' columns, and `_drop_unpaired_keys` and
  `_paired_holders` are gone: a column the plan does not pair is no key
  whether or not it is pruned.
- A side holding only shared, unpaired columns used to vanish from the
  merge (it had no graph edge left); it now joins keyless and the
  keyless-join guard judges it. The suite never builds one.

Suite capture against `main` (`sqlcap`, `-n 2`, `PYTHONHASHSEED=0`,
worktree paths normalized): 45,145 keys, the same 25 failures (the
BigQuery, Snowflake-dialect and GCS tests this container has no credentials for), 5 tests
moved, all listed noise: `test_cli_consistency`, `test_ingest`,
`test_trilogy::test_unit`, the BigQuery temp-table name and the GCS
cache-bust URL. So no plan moved.

The resolve unit tests state each side's columns instead of a graph.

**Resolve walks the plan's pairings (part 70).** After part 69 resolve read
every key off the plan, but it still ordered the joins key by key: it ranked
each planned key (a pivot), then joined every side the plan pairs on that key
before moving to the next, deferring a side that paired with nothing joined
yet until its partner arrived.

- `resolve_join_order_v2` walks the pairings instead. `_ranked_edges` lists
  each planned pairing once per key it pairs on, ranked by `_pivot_rank` (the
  old pivot order: authored keys, solid value-null pairings, held spans, then
  holder count and name). Each step joins the unjoined side whose pairing onto
  the joined set ranks best, ties going to `_score_join_candidate`. A pairing
  between two unjoined sides seeds a tree only when none touches it.
- So every join's right pairs with a side already joined, and the deferral
  loop is gone. `_join_onto` types one right against the joined sides the
  plan pairs it with, as the pivot loop's body did.
- One order changes. A side joined under one key can expose a pairing that
  ranks better than the key's remaining sides. The walk takes it next; the
  pivot loop finished the key first.
  `test_a_better_pairing_a_join_exposes_is_walked_next` pins it.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`,
worktree paths normalized): 45,145 keys and the same 25 failures (the
credential-gated tests). 152 tests moved: 103 oracle-sweep, 37
`test_complete_binding_lookup`, 6 window-law, 2 `thelook_duckdb`, and the
listed noise (`test_cli_consistency`, `test_ingest`, the BigQuery temp-table
name, the GCS cache-bust URL). Re-run serially on both trees, the 149 that
are not BigQuery, `test_cli_consistency` or `test_ingest` compile the same
apart from the GCS cache-bust URL. So no plan moved: the order change above
is one the suite never builds.

**What decides the walk's rank (part 71).** Before the plan can state the
walk's order, it has to know what the rank reads and which of its terms
decide anything. `_pivot_rank` ranks a pivot (a planned key) on seven terms:
authored, solid value-null, held span, the most other spans a holder holds,
holder count, key length, and name.

- A solid pivot (a key's holders that hold no region, ranked ahead of the
  region's stitch) was a key spelled with a `|solid` suffix. That suffix was
  read back to rank it, to find its key, and to keep it out of the authored
  and held-span sets. A pivot is now a `_Pivot(key, solid)`, and
  `_pivot_rank` states those two exclusions. The order is unchanged: two
  solid pivots tie on every term before the name only when their keys are the
  same length, and there the shared suffix compares equal.
- `_score_join_candidate` boosted a side already joined. Since part 70 it
  scores only unjoined candidates, plus the joined sides `_join_onto` orders,
  which all take the boost, so the boost ordered nothing. It is gone.
- `test_a_solid_value_null_pairing_walks_before_a_held_span` pins the solid
  tier above the held-span tier.

`local_scripts/sql_ab/rank_census_plugin.py` replays every resolve over three
or more sides with one term held constant and counts the merges whose join
order moves. Over all of `tests/` (14,335 such merges), 3,954 move with every
term but the name held:

| term | merges it decides | what it reads |
|---|---|---|
| key length | 2,838 | the key's spelling |
| holder count | 1,272 | the plan: how many sides it pairs on the key |
| authored | 64 | the environment's authored join groups |
| solid value-null | 44 | each side's region marks and value-null provenance |
| held span | 43 | each side's region marks |
| most other held spans | 27 | each side's region marks |

So the plan already holds what decides almost every walk: the keys it pairs on
and how many sides it pairs on each. The region terms decide few merges
(`test_padded_null_pairing`, `test_derived_key_domain` and the dimension-chain
oracle). Only the solid term reads
something the plan does not state: whether a side's NULL on the key is a value
(`provenance.values`) or padding.

Suite capture against `main` (`sqlcap`, `-n 2`, `PYTHONHASHSEED=0`, worktree
paths normalized): 45,488 keys and the same 25 failures (the
credential-gated tests). 120 tests moved: 100 `test_complete_binding_lookup`,
16 window-law, and the listed noise. Re-run serially on both trees they
compile the same, apart from the GCS cache-bust URL. So no plan moved.

**What decides the walk's ties (part 72).** Part 71 counted the rank's terms.
Below the rank, `_score_join_candidate` breaks ties, and it answers two
questions: which unjoined side the walk joins next, and the order of the joined
sides that side is typed against, which decides which of two lefts on the same
keys is kept and which is redundant. The second use is now its own step,
`_lefts_in_order`, so each can be measured alone; the order is unchanged.

`rank_census_plugin.py` now also holds each scorer term constant in one use
at a time (`score_replica`, the scorer rebuilt from its terms, orders all
14,468 merges as the scorer does). Over all of `tests/`, the merges each term
decides:

| term | next side | joined-side order | what it reads |
|---|---|---|---|
| name | 6,308 | 1,574 | the side's identifier |
| grain size | 2,012 | 753 | the built side's grain |
| nullable on the key | 1,557 | 783 | the built side's nullable outputs |
| hosts the region | 1,170 | 280 | the built side's outputs and `~` marks |
| partial on the key | 938 | 71 | the built side's partial outputs |
| several partial holders | 10 | 174 | the same, over the key's unjoined holders |
| scoped-LEFT anchor | 77 | 70 | the environment's anchor keys |

No term is dead, so none is removed. Every term but the anchor reads the
built side, not the plan, and the side's identifier decides the most. The
tie-break is where resolve still reads built nodes for order.

Two probes on the rank itself, over the same suite:

- Key length (2,922 merges) has no semantic stand-in. Over the suite less
  the `*_laws.py` files (928 merges it decides), at the first step it moves
  the key it picks is in more sides' grain in 185, fewer in 147 and as many
  in 596; what the grains determine splits the same way. It is a spelling
  tiebreak (`local.user_id` before `local.product_id`).
- The solid tier (45 merges) reads value-null provenance only weakly: read
  any nullable key in its place and 1 of the merges with a held span moves
  (`test_dimension_chain_select_matches_raw_rows[484]`); drop the nullable
  check and 113 move. So stating the solid tier in the plan needs which
  region-free holder can NULL the key, nearly never why.

The census reruns the whole suite with the extraction in place: the same 25
credential-gated failures as `main`.

**The walk keeps the left it walked first (part 73).** Part 72 split the
scorer's two uses. Before moving either onto the plan, the census asked
whether any built-side term the scorer reads has a plan or contract stand-in
that orders the same merges. None does. Over all of `tests/` (14,321 merges
with three or more sides), the merges each stand-in moves in the pick of the
next side:

| stand-in | for | moves | the term held constant moves |
|---|---|---|---|
| `bool(held_spans)` (region marks) | hosts | 1,152 | 1,170 |
| the key in its `held_spans` | hosts | 1,154 | 1,170 |
| keys the plan pairs the side on | grain size | 2,824 | 1,964 |
| the grain's FD closure | grain size | 3,715 | 1,964 |
| value-nullable on the key | nullable on the key | 1,306 | 1,557 |

Read for every reader, typing included, the region marks move 1,989 merges
in place of `hosts`, and dropping `hosts` moves 2,004. So hosting is not the
region contract restated: the contract is a different fact, and a stand-in
moves about as much as dropping the term.

The second use is different in kind. Which of two lefts on the same keys a
right keeps is not a choice of what to join next; the walk has already
joined both. So the scorer no longer re-sorts the joined sides:

- `_lefts_in_order` is gone. `_join_onto` types a right against the sides
  already joined in the order the walk joined them, and of two lefts on the
  same keys it keeps the one walked first.
- The scoped-LEFT anchor still seeds the tree. Walked first, it is the left
  each co-anchored optional source dedups against.
- `test_of_two_lefts_on_the_same_keys_the_one_walked_first_is_kept` pins it.
  It fails on `main`, where the side nullable on the key outscored the side
  walked first.

The scorer now answers one question, the next side. The left order is the
walk's, and the walk is over the plan's pairings.

Which left an ON reads mattered to one optimizer phase. A first capture
of this change against `main` had 3 statements whose joins loosened
(spellings 262 and 11030 in `test_oracle_spellings_laws`: INNER to LEFT
OUTER, and in 11030 a HAVING moved out to a `coalesce(count, 0) = 6` in the
WHERE), beside about 15 that tightened. Both resolves typed every join the
same. The difference was `upgrade_outer_key_set_equivalence`
(`UpgradeOuterFromKeySetEquivalence`): it upgrades an outer join to INNER
when the key's value set is the same on both sides, and it tested only the
left the pair reads. On `main` that left was a sibling of the right over the
same filtered rows, and the proof held. The side walked first was not, and
the proof failed, though on the rows entering the join the two lefts hold
the same key. The tightened statements were the same luck the other way.

So the proof no longer depends on the spelling. `_equated_reads` collects
the pair's left and every read of its key that an INNER join earlier in the
same CTE equates with it, and any of them may witness the equivalence, as
long as no earlier outer join null-extends it. An outer join pads the side
it extends, so it equates nothing.
`test_a_left_an_inner_join_equates_witnesses_the_key_set` pins the upgrade,
and `test_a_left_an_outer_join_pads_is_no_witness` pins the guard.

Suite capture against `main` with both changes (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated failures,
and 196 tests move. None changes a CTE or join count, and no join loosens:

- 24 statements tighten: LEFT OUTER or FULL to INNER, a `coalesce`
  dropped from an ON, a filter kept in its HAVING
  (`test_row_preserving_grain_invariant`, `test_dimension_chain_oracle_laws`,
  `test_final_gate_lookups`, spellings 12365 and 30670, sweep 80,
  `test_grain_pin_unnest_merge_filter`).
- Of the rest, 32 statements are shorter and 12 longer. 2 of the longer are
  listed noise (`test_cli_consistency`, and a Rust-binary test that ran on
  the second tree only because cargo had been built by then). The other 10
  are 5 spellings in `test_oracle_spellings_laws`, each compiled twice,
  longer by 1 to 80 characters: where the left walked first is a wrap that
  did not carry the key, it now projects it (`tree_form`, spelling 10995).
  Joins and their types are unchanged.

Every moved test passes on both trees, and the law suites compare their
rows against a raw-row oracle, so the moved plans return the same rows.

**A side preserved past an INNER seeds the walk (part 74).** Before the
scorer's terms can be read off the plan, each one has to be a choice of plan
shape only, as `resolve_join_order_v2`'s docstring claimed: "ordering is a
heuristic for plan shape only; `ensure_content_preservation` guarantees the
result set regardless of the order". So each built-side term was dropped in
turn and the whole suite run:

| term dropped | tests whose SQL moves | tests that fail |
|---|---|---|
| grain size | 3,896 | 2 shape pins |
| nullable on the key | 2,829 | 3 shape pins, **10 rule-soundness laws (rows)** |

In the nullable run 5 TPC-DS tests also failed. Each passes when run alone,
so they are not counted.

The order decides rows. With the nullable term gone,
`test_rule_soundness_laws` (`where ((reason is null) or (name is null)) and
(not (sum(amount) by cust_id > 6)) select name, note, count(order_id) by
cust_id`) returns an extra all-NULL row. The walk now joins the
`sum(amount) by cust_id` side first, INNER-joins the count to it, then joins
the customers side RIGHT OUTER:

```sql
FROM cooperative                                     -- sum(amount) by cust_id
  INNER JOIN questionable ON cooperative.cust_id = questionable.cust_id
  RIGHT OUTER JOIN thoughtful ON cooperative.cust_id = thoughtful.cust_id
WHERE coalesce(cooperative.sum > 6, False) = False
```

Customer 2's count was filtered out (its sum is 7), so the INNER drops
customer 2's sum as well. The RIGHT OUTER pads it back as NULL, and the WHERE
reads that NULL as "not over 6". The nullable term had kept the customers
side first, which gives `thoughtful LEFT cooperative LEFT questionable`.
Each partner then hangs off the customer's own row, and customer 2's sum
still filters it out.

The hazard is the shape, not this term. A join that preserves its right
against the sides walked before it (RIGHT OUTER) keeps the right's rows. But
an INNER join among those sides has already dropped each of them wherever
its partner missed, and `ensure_content_preservation` only widens joins
*after* a preserving one, so nothing gives those values back. Three sides on
one key are enough: a region holder `a` and two feeders `b` and `c`, with
nothing on the feeders to break the tie. On `main` the walk gives
`c INNER b RIGHT OUTER a`.

- `_preserved_past_inner` finds the first right a join preserves past an
  earlier keyed INNER. `resolve_join_order_v2` walks again with that side as
  the seed (`_walk`), so each side hangs off the preserved rows on its own
  pairing. It walks again once. Over the suites where it fires, the second
  walk never has the shape again.
- The docstring now says the order chooses plan shape, except for this case,
  which the walk repairs.
- `test_a_side_preserved_past_an_inner_join_seeds_the_walk` pins the
  three-side case above. It fails on `main`.

With the rule in place and the nullable term dropped, the whole suite
fails only on the 3 shape pins, a timing test (`test_copy_perf`) and the 25
credential-gated tests. All 10 soundness laws hold. In that suite the
nullable term was deciding rows only through this shape.

On `main`'s scorer the rewalk fires in about 30 tests. Suite capture against
`main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`): both trees have the same 25
credential-gated failures, and 48 tests move:

- 19 tests turn `x INNER y RIGHT OUTER h` into
  `h LEFT x LEFT y` (`test_padded_null_pairing`, `test_derived_key_domain`,
  8 `test_partial_lookup_fallback_oracle_laws` seeds), for example:

  ```sql
  -- main
  cte_3 INNER JOIN cte_0 ON cte_3.bucket IS NOT DISTINCT FROM cte_0.bucket
        RIGHT OUTER JOIN cte_4 ON cte_3.customer_id = cte_4.customer_id
  -- part 74
  cte_4 LEFT OUTER JOIN cte_3 ON cte_4.customer_id = cte_3.customer_id
        LEFT OUTER JOIN cte_0 ON cte_3.bucket IS NOT DISTINCT FROM cte_0.bucket
                             AND cte_3.customer_id IS NOT NULL
  ```

  The preserved side was already preserved. Only the INNER between its
  partners becomes a LEFT, so a partner's values no longer depend on the
  other partner matching.
- 3 tests tighten: a LEFT OUTER becomes INNER
  (`test_final_gate_lookups` query 4, spelling 31824), and a FULL becomes
  LEFT OUTER (`test_partial_lookup_fallback_oracle_laws` seed 128).
- 4 tests reorder joins whose types are all INNER once the optimizer has run
  (`test_outer_padding_null_not_a_value`, 2 more fallback-oracle seeds).
- The other 22 move where the rewalk never fired: `test_cli_consistency`,
  `test_ingest`, `test_config_bootstrap_dialect`, `test_trilogy::test_unit`, the window
  laws under xdist, `test_complete_binding_lookup`, the BigQuery temp-table
  name, the GCS cache-bust URL, a local HTTP port
  (`test_unbuilt_sources`), and ON-conjunct or column order
  (`test_where_hosts`, `test_total_beside_lookup_gate`).

Every moved test passes on both trees. On the suite's data the INNER never
dropped a row its RIGHT OUTER kept, so no row assertion moves either. The
soundness laws above are the rows that differ.


**The walk no longer reads a side's nullability (part 75).** Part 74 left
the nullable term deciding no rows in the suite. Before moving any scorer
term onto the plan, each of the four built-side terms was dropped and the
whole suite captured (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`). The join counts
below sum over the tests whose SQL moves and whose statement count is the
same on both trees, so tests that compile a varying number of statements
under xdist are left out (TPC-DS `test_queries`, `test_cli_consistency`).

| term dropped | tests moved | INNER | LEFT | RIGHT | FULL | CTEs | tests that fail |
|---|---|---|---|---|---|---|---|
| nullable on the key (from `main`) | 2,425 | +508 | -311 | -176 | -16 | -3 | 3 shape pins |
| grain size (with nullable dropped) | 1,463 | -51 | -53 | +58 | -4 | +9 | 13, incl. 7 TPC-DS SQL pins, `test_q74_stays_join_lean` |
| hosts the region (with nullable dropped) | 717 | +5 | -347 | +288 | +57 | +3 | `test_facts_are_summed_before_they_meet` (asserts no FULL), `test_forked_full_column_set` |
| partial on the key (with nullable dropped) | 4,098 | -879 | -2,953 | +3,596 | +250 | -6 | 12, incl. **6 `test_rule_soundness_laws` cases (wrong rows)** |

So the nullable term goes and the other three stay.

- **Nullable.** The SQL is tighter and shorter (21,920 characters less over
  the moved tests). 132 tests gain an INNER and 2 lose one
  (`test_two_member_and_intersects[join|merge]`: a lookup that was INNER
  beside two FULL-joined probes becomes a LEFT, and the rows are the same).
  `_score_join_candidate` no longer reads `nullables`.
  `test_unnested_dimension_join_preserves_trees` asserted that the trees side
  is the RIGHT of the join onto the unnested species. The trees side now seeds
  that join, `city_trees LEFT OUTER JOIN <unnested>`. The test now asserts
  that the trees side is the preserved one, in either direction, and the new
  form passes on `main` too. `test_a_solid_value_null_pairing_walks_before_a_held_span`
  seeds the solid pair from its other side, and the held span still joins
  after it.
- **Grain size** is a plan-quality term. Dropping it moves more joins toward
  RIGHT and adds CTEs in TPC-DS. Part 73 found no plan stand-in for it.
- **Hosts** is a plan-quality term. Dropping it turns 288 LEFTs into RIGHTs and
  adds 57 FULLs.
- **Partial** still decides rows, through a different shape from part 74's.
  With it dropped, `where reason is null and name is null select name, note,
  count(order_id) by cust_id` walks `notes` (`~cust_id`) before `customers`.
  Typing gives `FULL` in both directions (`partial_domain`). Then
  `narrow_subset_full_join` narrows a FULL only when its *right* is the `~`
  side and its left holds the whole domain, so `customers FULL notes` becomes
  LEFT OUTER while `notes FULL customers` stays FULL. The FULL stamps
  `cust_id` nullable on the merge. The FINAL join then reads both sides as
  nullable on the key, goes FULL, and keeps a customer the WHERE rejected.

  Mirroring the narrowing (a `~` left source no earlier join padded, beside a
  right with the key's whole domain, narrows to RIGHT OUTER) fixes those rows:
  with partial dropped, every soundness case that compares rows holds again.
  Two `test_rule_fires_and_keeps_rows` pins fail, because the rule no longer
  fires on an already tight plan. Measured on its own
  against this PR, the mirror moves 536 tests, removes 115 CTEs and 133,027
  characters, and adds 243 FULL joins. The FULLs come from the stamp the
  mirror corrects. After `species RIGHT OUTER trees`, `genus` is partial on
  the merge (only the genera of the trees' species reach it). The FINAL join
  between two projections of that one merge
  (`test_partial_lookup_fallback_oracle_laws`, `test_complete_binding_lookup`)
  then types `partial_domain` → FULL where it was `nullable` → INNER, though
  both sides hold the same `~` binding. `_extent_free_join` already knows that
  two projections of one binding carry no row intent, through `span_bindings`,
  but only for extent-free spans. So the mirror waits on that rule (part 76).

Suite against `main`: both trees have the same 25 credential-gated failures.
2,430 tests move (`sqldiff`). Every moved test passes on both trees, and the
soundness, fallback-oracle and dimension-chain laws compare their rows with
a raw-row oracle, so the moved plans return the same rows.

**Subset narrowing works in both directions (part 76).** Part 75 found two
rules whose answer depends on which side the walk joined first. This part
fixes both.

- `narrow_subset_full_join` narrows a FULL in either direction. A `~` right
  beside a left source whose every row survives, and which holds the key's
  whole domain, gives LEFT OUTER as before. Now a `~` left source that no
  earlier join padded, beside a right holding the key's whole domain, gives
  RIGHT OUTER. Every stream row then carries that source's key, and the
  right has a partner for each one. `narrow_join_types` tracks which sources
  the joins so far have padded, after each join has been narrowed.
- `_decide_join_type` drops from the partial read any `~` key that both
  sides bind through the same leaf `~` columns (`_one_binding`). Two
  projections of one binding cover the same subset, so the `~` carries no row
  intent between them, and the other rules type the pair. `span_bindings` now
  covers every `~` key a side binds, not only the extent-free spans that
  `_extent_free_join` reads it for. Two sides whose `~` comes from different
  leaf tables are peer facts and stay FULL.
- `test_a_subset_full_join_narrows_whichever_side_is_joined_first` and
  `test_two_projections_of_one_subset_binding_pair_as_complete` pin the two
  rules. Both fail on part 75.

The mirror also corrects a merge's stamps. `species RIGHT OUTER trees` keeps
every tree and pads nothing on the trees' side, where the FULL had stamped
`tree_id`, `species` and `dbh` nullable. So the merge's grain reduces to
`tree_id`. In `test_class_value_beside_partial_lookups` that makes the
SELECT computing the `fallback_class` label the same relation as the
final's tree-grain SELECT, and the two become one shared CTE. The label was
already computed per tree: the old plan evaluated it in the CTE that grouped
the 7 tree rows. The class *value* is still computed once per species and
joined back on species, which is what #972 asks for. The pin, renamed
`test_the_class_value_is_computed_once_per_species`, now checks the CTE
that reads the class fits, and passes on both trees. That file's plans have
one more CTE each.

Suite capture against part 75 (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`): both
trees have the same 25 credential-gated failures, and 654 tests move. Over
the moved tests: INNER +112, LEFT -119, RIGHT -11, FULL -73, CTEs -115,
`coalesce` -1,059, and 146,939 characters fewer. Most of it is in
`test_partial_lookup_fallback_oracle_laws` (355 tests, -84 CTEs),
`test_complete_binding_lookup` (192 tests, 64 LEFTs become INNER), and
`test_global_fallback_oracle_laws` (18 tests, 36 FULLs become INNER). Every
moved test passes on both trees, and those law suites compare their rows with
a raw-row oracle.

With the walk's partial term dropped on top of part 76, the 6 soundness
cases from part 75 return the right rows. The order is still not neutral,
though. In `test_derived_key_domain` and `test_total_beside_lookup_gate` (6
statements) the other order renders a self-join of one CTE without an
alias, and DuckDB rejects the duplicate name. So the partial term stays.

**A merge's copies coalesce an outer join written from either side (part
77).** Part 76 left the partial term deciding one thing besides plan shape:
with it dropped, 6 statements in `test_derived_key_domain` and
`test_total_beside_lookup_gate` rendered `"wakeful" INNER JOIN "quizzical"
INNER JOIN "wakeful"`, and DuckDB rejected the duplicate name. The walk was
not at fault. One merge of `customers` and `orders` is built twice, by two
consumers. A QueryDatasource's identity names the sides its joins NULL-extend
whichever way the join is written (`_null_extended_sides`), so
`customers LEFT JOIN orders` and `orders RIGHT JOIN customers` are one CTE, and
`CTE.__add__` merges the two copies. `coalesce_duplicate_joins` then kept
both joins: it merged mirrored joins only when both were INNER. On `main` the
partial term happened to orient both copies the same way.

- An outer join now folds into its mirror when both NULL-extend the same
  CTEs, with the same pair re-orientation INNER joins used
  (`_absorb_mirrored`). A join that pads the other side is a different
  relation and stays.
- `test_an_outer_join_merges_into_its_mirror` and
  `test_outer_joins_padding_different_sides_stay_apart` pin it. The first
  fails on `main`.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`): both
trees have the same 25 credential-gated failures. 200 tests move, and every
one is column order under xdist: no statement changes length, and no join or
CTE count moves. So no plan moved.

With part 77 in place and the partial term dropped, the whole suite fails
only on 3 shape pins (`test_first_region_holder_is_left_at_plan_time`,
`test_forked_full_column_set`, `test_resolve_join_order_v2`), the two
`test_rule_fires_and_keeps_rows` pins whose rule no longer fires on the
tighter plan, and the 25 credential-gated tests. No row check fails. So the
walk's order no longer decides rows or whether a plan renders anywhere in the
suite. What the partial term still decides is plan quality: 4,324 tests
move, INNER -383, FULL +62, LEFT -3,239 and RIGHT +3,553 (mostly the same
joins written from the other side), CTEs -141.

**A complete right beside a subset partner is not padded (part 78).** Of the
FULL joins the dropped partial term added, most came from one shape. A merge
of a `~?species` side `X` (ecoregions with their species, NULL where an
ecoregion has none), `trees` and `enrichment` (both complete on `species`):

```text
partial term kept:     trees INNER enrichment FULL X
partial term dropped:  X FULL trees FULL enrichment
```

The rows are the same, but the second plan keeps `enrichment` FULL. Typed
against `X` alone it is FULL (`partial_domain`), and the walk reduces its
types over every left. Yet every `enrichment` row has a partner: `trees`
survives the FULL with all its rows, holds every species, and the ON reads
`coalesce(X.species, trees.species)`, which on each stream row is the trees'
species or NULL beside it.

- `narrow_subset_full_join` narrows such a FULL to LEFT OUTER
  (`_pairs_every_right_row`). The rule applies when the join is on one key,
  the right is complete and never NULL on it, some left binds it `~`, and a
  surviving complete left was paired directly with every other left on that
  key by an earlier join (`_equated_by`). `narrow_join_types` passes the
  joins before each one.
- The `~` partner is what makes the narrowing safe. A first version that took
  any non-NULL right failed 88 tests, among them authored full joins
  (`test_scoped_join`), union-join axes (`test_union_join_one_sided_axis`)
  and presence probes. There two complete sources can disagree, and the FULL
  is row intent, not a typing artifact.
- `test_a_full_join_beside_a_surviving_complete_left_pads_no_right_row` pins
  the rule and its three guards: no earlier equating, no surviving complete
  left, and no `~` partner.
- The two `test_rule_fires_and_keeps_rows` pins
  (`DropIdentityGroup`, `UpgradeOuterFromKeySetEquivalence`) no longer fire
  on their queries, because the plan is already tight. They now use short
  random queries on which each rule still fires, with the partial
  term kept or dropped.

Suite capture against part 77: both trees have the same 25 credential-gated
failures. Apart from the 2 re-pinned queries and column-order noise, 5 tests
move, and all tighten: 2 FULLs become LEFT OUTER
(`test_relation_keyed_on_the_grain[beside_property-none]`,
`test_a_property_summed_by_its_own_key_over_a_region_counts_each_once`), and
4 `coalesce` calls leave an ON (`test_derived_key_domain`,
`test_two_facts_at_one_grain`). On the walk's own order the rule rarely has a
FULL to narrow. It matters when the `~` partner is walked first.

With the partial term dropped on top of part 78, the same 3 shape pins and
the 25 credential-gated tests fail, and nothing else. Against part 78, 4,226
tests move: FULL +1, INNER -261, CTEs -141, `coalesce` -47, and 108,358
characters fewer. By file:

| file | INNER | FULL | CTEs | characters |
|---|---|---|---|---|
| `test_unnest_filter_laws` | +144 | 0 | -144 | -105,996 |
| `test_rule_soundness_laws` | +63 | -3 | 0 | -4,250 |
| `test_relation_rollup_laws` | -450 | 0 | -40 | -1,239 |
| `test_dimension_chain_oracle_laws` | -7 | 0 | +41 | +1,018 |
| `test_summary_cover_laws` | -9 | +8 | 0 | +282 |

So the partial term stays, as grain size and hosting did in part 75: dropping
it adds CTEs and FULL joins in two families, and loosens a third. The
relation-rollup loss is not join typing. Both orders type the merge's joins
alike, and part 78 narrows the one that differed. The optimizer narrows
`(trees INNER enrichment) FULL X` to INNER under the WHERE, but narrows
`X FULL trees LEFT enrichment` only to `X LEFT trees`, and the CTE below it
keeps a RIGHT OUTER. Which outer join a phase can tighten still depends on
which side the walk joined first.

**The walk order decided rows after all (part 79).** Parts 77-78 measured
the partial term against the suite's row checks, but the relation-rollup
laws compare the plain model with the summary model *under one planner*: a
plan both models get wrong passes. Planning each law with the term kept
and dropped, over data that holds the model's complete bindings (trees and
lists over the same species), gave different rows in the `element`
spelling under a WHERE. A raw-row oracle then showed `main` itself wrong:
`select tree_eco_region, count(tree_id) where city = 'A'` drops the trees
whose species lists no ecoregion, which the law's own docstring says count
under a NULL one (5 of 10 seeds).

- The unnest of the trees' lists is `~tree_id` (a tree with an empty list
  has no element row). Filtered `city = 'A'`, it joins the trees, filtered
  alike, on `tree_id` alone. `_is_filter_population` read the unnest as
  the WHERE's population, so `tighten_join_for_filtered_branch` made the
  join that keeps those trees INNER. On the partial term's walk it was the
  trees' join that was tightened, on the other walk the unnest's, so the
  two orders lost different rows.
- It already excused a `~` side whose filters read only the join keys
  (`cust_id != 10`). It now excuses one whose partner applied every filter
  it did (`_applies_all_of`): the partner's extra rows passed the same
  filters, so the narrowness is the `~`'s, whatever the filters read.
- `test_a_filtered_relation_keeps_the_empty_list_group` checks the
  `element` spelling against a raw-row oracle, and
  `test_the_walk_order_decides_no_rows` plans every relation-rollup law
  with the partial term kept and dropped (`_score_join_candidate` blind to
  partials) and compares rows. On `main` 7 of the 18 cases fail.

**A complete right beside an unpadded complete anchor is INNER (part 80).**
Part 78 narrowed `X FULL trees FULL enrichment` (`X` a `~?species` side) to
LEFT OUTER on `enrichment`, because every `enrichment` row has a partner. When
the complete anchor (`trees`) is on every stream row, padded by no earlier
join and never NULL on the key, every stream row has a partner too, and the
join pads nothing. `cheerful RIGHT trees LEFT enrichment` becomes
`cheerful RIGHT trees INNER enrichment`, which is what the partial term's
walk types as `trees INNER enrichment`.
`test_a_full_join_beside_a_surviving_complete_left_pads_no_right_row` adds
the padded and unpadded cases.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`): the same
25 credential-gated failures (plus `test_eighty_three`'s order-dependent
SQL-length budget, which passes alone and in its file under `-n 4`; its
captured SQL does not move). The plans that move:

- 100 `test_relation_reads_the_summary` cases (`element` under the city
  and species filters), one INNER becoming LEFT OUTER and one CTE each:
  part 79's fix;
- 4 `test_grain_pin_unnest_merge_filter` cases, the same unnest shape;
- 2 `test_summary_cover_laws` cases, a LEFT OUTER becoming INNER: part 80;
- `test_unmodelled_regions` (one join mirrored) and
  `test_union_dim_pushdown` (one INNER becoming LEFT OUTER);
- the rest are CTE names or column order.

With both parts in place, the relation-rollup laws' two walks differ in one
family only beyond mirrored joins (`merged` × `value`, 6 queries). There the
partial term's walk ends `(trees INNER enrichment) INNER cheerful`, while
the other ends `cheerful LEFT trees LEFT enrichment`. Element (18 queries),
property (6) and merged × city (6) now type the same joins from either side.
After parts 81-82 the `merged` × `value` gap is one join: the kept walk's
INNER on `cheerful` was the unsound proof's, and it is now a RIGHT OUTER.

**An INNER's key proof forces only the joins before it (part 81).**
`UpgradeJoinOnGuards` read an INNER join's key predicate as a proof that the
key is non-null on every surviving row (`_add_inner_join_key_proofs`). It
forced a side present off that proof at *any* join in the CTE. A later outer
join's padded rows never meet the predicate, so `a INNER b FULL c WHERE c.x > 1`
narrowed to INNER and dropped every `c` row with no `a`. This is what made
the partial term's walk tighter in `merged` × `value`: its INNER on
`cheerful` was forced off the `trees INNER enrichment` proof.

- Each proof now carries the index of the last INNER that made it
  (`_ProofState.cte_keys`), and forces a side only at a join before that
  INNER, whose padded rows the INNER does reject.
- `test_an_inner_join_proves_nothing_about_a_later_join_padding_it` pins
  it at the CTE level.

On its own, part 81 exposed a second bug. In `tpc_h` `adhoc07`,
`(lineitem INNER orders) RIGHT OUTER customers` stops being narrowed to
INNER. Without its `LIMIT 20`, the query then returns 500 more rows on TPC-H
sf 0.01: customers with no order, from every nation.

**Padding pairs null-safe only where a solid key or the same filters say
which row is which (part 82).** adhoc07 sums by `order_id`, so the Indonesian
customers with no order collapse into one NULL-`order_id` group. The FINAL
reads `order.customer.id` back off a lookup (`customers LEFT JOIN orders`,
no nation filter) on `order_id` alone. Both sides' NULLs are orders padding,
so `get_modifiers` paired them null-safe ("the padding shares provenance"),
and the one NULL group matched every customer with no order, of any nation.
`main` returned the right rows only because the unsound INNER of part 81
dropped that group first.

- A padded NULL names no member. Pairing two sides' padding with
  `IS NOT DISTINCT FROM` matches the right rows only when another key of
  the join pairs non-NULL on both sides and says which padded row is which,
  or when both sides' rows passed the same filters (`_same_filters`, over
  `collect_applied_conditions`, now in `processing.utility`). An example of
  the first: customer 3's no-order row beside its own, joined on
  `customer_id` too, in `test_alias_of_row_value_is_not_the_pinned_read`.
  Otherwise the key pairs with `=`.
- With `=` on that join, the cross-CTE proof makes the lookup's
  customers-to-orders join INNER, and `questionable`'s join stays INNER, now
  earned by a later rejection. adhoc07 returns `main`'s 666 rows, and its
  INNER pin holds.
- `test_adhoc07_rows_are_the_nations_orders` checks adhoc07 without its
  LIMIT against raw SQL on `dbgen(sf=0.01)`. It fails with part 81 alone.
- Not decided here: `select order.id, order.customer.id` with the same
  WHERE and no aggregate returns the 24 Indonesian customers with no order,
  as `(NULL, customer)` rows. The summed query drops them, on `main` and
  here alike.

Suite capture against `main` with parts 79-82 (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): the same 25 credential-gated failures, nothing else.
231 tests move, adding up to INNER -284, LEFT +99, RIGHT +182, FULL +1,
null-safe pairs -31. Against parts 79-80 alone, parts 81-82 move 123:
- the INNERs only the unsound proof forced become RIGHT or LEFT OUTER
  (152 in `test_relation_rollup_laws`, 19 in the dimension-chain oracle,
  4 in `test_dashboard_queries`, a few in subset-join pins);
- null-safe pairs leave joins whose padding passed different filters
  (`test_rowset_declared_join_pairing`, `test_gcat`,
  `test_padded_population_gate`);
- one `test_rule_soundness_laws` case gains an INNER and a FULL.

Every row-checking law in those files passes.

**The walk's spellings decide no rows (parts 83-87).** Before the plan can
state the walk's order, every term it would replace has to decide plan shape
only. Parts 74-79 showed that for the nullable and partial terms. The terms
that decide the most walks were never checked: the key's length and name at
the bottom of the rank (part 71), and the side's name at the bottom of the
scorer (part 72). Neither is a fact about rows, so a walk that breaks those
ties the other way round must return the same rows.

`tests/helpers/walk_order.py` reverses them, and
`local_scripts/sql_ab/walk_flip_plugin.py` runs the suite that way
(`WALK_FLIP=key,side`). Every other rank and scorer term stays. On `main`,
35 tests fail under the flip and pass without it. 23 are shape pins (SQL
text, a join count, a rule firing; each mirrors or reorders joins). The
other 12, in six families, return different rows, and four of them are planner bugs. In each
the default walk was right only because of how the names sort, or because
the data happened to hide the loss.

- **A second `subset join` member is padded (part 84).** `select
  store_cust.cust_sk subset join web_cust.cust_sk = store_cust.cust_sk
  subset join cat_cust.k_sk = store_cust.cust_sk` types both members FULL
  (`partial_domain`). `UpgradeOuterFromKeySetEquivalence` narrows the first
  to LEFT OUTER, but not the second: its ON reads `coalesce(store, first
  member)`, and `_side_fully_matches` asked every left read to be a complete
  superset. On every row entering the join, the anchor's key is non-NULL
  and the first member's is the anchor's value or NULL, so the coalesce
  is the anchor's value. One fully-matching anchor per right key now
  suffices (`_anchors_coalesce`: the anchor is never null-extended before,
  never NULL on the key, and an earlier join paired it with the other read
  by `=`). On `main` the default walk joins `cat_cust` second, whose keys lie
  inside the store's on the harness data, so the FULL kept no extra row.
  Walked the other way, the web-only customer 4 came back. The q78-family
  chain (`test_chained_left_join_narrows_to_store_anchor`) has the same
  shape.
- **A FULL join's shared column reads one provider (part 85).** `select
  user_id, cost, sum(cost) by product_id as sc where cost is null` FULL joins
  the products and the filtered total, and both carry `cost`. The merge read
  `cost` off whichever provider sorted first (`null_status`, then the
  identifier). The filtered total's `cost` is NULL on every product it
  dropped, so walked that way the WHERE kept every user. Join keys already
  read the coalesce of every FULL provider (`full_join_concepts`). Now every
  column the right of a FULL shares with a side joined before it does too
  (`merge_node._full_join_concepts`). The two copies agree wherever both
  have a row.
- **A side paired with the padding through its own NULL group carries it
  (part 86).** In `test_padded_null_pairing`, `count(customer_id) by bucket`
  holds Cat's padded row in its NULL group, so pairing it with the padding
  is right (`_pads_beside`). But on that row its `bucket` is then the
  padding's NULL. A later join reading `coalesce(orders.bucket,
  count.bucket)` against the targets' value-NULL group escaped the guard,
  because `_absent_key_groups` saw a left that was not absent, and Cat got
  bucket NULL's target. When the count was walked after the targets, the
  guard was already in place. Such a side is now recorded with the padding's
  absent sides (`_padding_guard`).
- **A join keyed off a pruned left keeps its widened type (part 87).** In
  dimension-chain seed 90 the walk joins the seats side first, RIGHT OUTER
  onto the count, then types the model lookup INNER against the seats side
  (the left walked first, part 73). `ensure_content_preservation` widens it
  to LEFT, since the RIGHT padded that side. `prune_outer_join_pairs` then
  keys the join off the count, which no join padded, and
  `restore_unpadded_joins` read that as "the widening holds nothing" and
  restored the INNER. That INNER was typed against the side whose pair was
  just pruned, and it dropped the flight with no tail. A join whose pairs
  pruning drops now keeps no type of its own to restore.

The other two families are not planner bugs:

- `test_bucket_count_rides_the_tree_key` (6 cases) returns the same rows.
  Its fan-out check (`find_fanout_joins`) read the join's right as the
  multiplying side, so `enrichment INNER trees` on `species` failed where
  `trees INNER enrichment` passed. A stream that is unique per the join's
  left keys is the lookup, whichever side it is. The helper now says so,
  and still catches the fan-out it was written for
  (`test_checker_catches_the_fanout_when_the_gate_is_disabled`).
- `test_merged_unnest_bridge_row_projection` loses ecoregion 600's tree-less
  row when the walk's keys are reversed. `merge merged_key into
  ecoregion_id` declares the two domains EQUAL, and EQUAL narrowing is on
  by default. Ecoregion 600 is in no species' list, so the data breaks the
  declaration, and dropping the row is licensed either way. On `main` an
  earlier FULL only happens to pad the narrowing's witness. The test still
  pins the default walk's row.

With parts 84-87 the flipped suite fails only on the 23 shape pins and that
row, beside the 25 credential-gated tests. Each fix has a test that runs
under the default walk and under each reversed spelling
(`flip_walk_spelling`). The five that compare rows fail on `main`.

Suite capture against `main` at `de73028` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated failures.
The only other outcome change is a Rust-binary test that skips on one tree
(listed noise). 692 tests move. The 110 whose joins or `coalesce` calls
change add up to INNER -4, LEFT +12, RIGHT -4, FULL -4, null-safe pairs
-12, `coalesce` +252, and no CTE. The rest are column order, CTE names and
the listed noise.

- Part 85 accounts for almost all the `coalesce`s. A column that two FULL
  sides carry now reads both (`test_rank_beside_partial_merge` 27 tests,
  `test_global_fallback_oracle_laws` 15, the dimension-chain oracle 14,
  `test_padded_population_gate` 6, `test_dashboard_queries` 8).
- Part 84 narrows the FULLs a second subset member was left with to LEFT
  OUTER (`test_three_source_scoped_outer_join`,
  `test_subset_rowset_anchor_rows`, `test_subset_join_between_rowsets`).
- Part 87 leaves a few joins at the LEFT OUTER they were widened to, where
  `main` restored an INNER typed against a pruned left. That is 8
  `test_oracle_spellings_laws` cases, 2 `test_rule_soundness_laws` cases
  and one 60k gate.
- `test_two_namespace_import_merge_hidden_output`: 2 RIGHT OUTERs become
  INNER, and 12 null-safe pairs become `=` pairs over a coalesce.

Every moved test passes on both trees, and the oracle laws among them
compare rows against raw SQL.

**The plan states the walk's tie order (part 88).** With parts 84-87 the
spelling ties decide no rows, so the plan can own them. `PlannedPairs`
carries an `order`: the groups in the order resolve joins them when no fact
decides. `MergeNode` reads each joined source's place in it
(`_planned_source_ranks`), and `JoinFacts.side_ranks` hands it to the walk.
Below the fact terms, the walk now breaks ties on that order instead of a
spelling:

- `_pivot_rank`: the key whose holders the plan puts first ranks first, in
  place of the key's length and name.
- `_ranked_edges` and `_pivot_map`: a key's holders and its pairings are in
  the plan's order, not sorted by datasource name.
- `_score_join_candidate`: of two sides scored alike, the one the plan puts
  first is picked, in place of the larger name.
- `_keyless_order` and a keyless join's left: the plan's order, in place of
  the first column no other side holds.

A name is still the last term, and decides only between two sides the plan
ranks alike, or two keys with the same holders, which pair the same sides
either way.

What the plan states:

- A star (`PlannedPairs.star`) states its hub first: each spoke hangs off the
  hub's rows. Stars are the planned-key projection, an unnest's element
  filters, the partial-completion and finer-filter rollups, the source
  plan's star merge, a recursive walk's join-back and the presence probe.
- A group-graph plan (the FINAL merge and every per-group merge,
  `_planned_join`) states `JoinPlan.precedence`. Suppliers come before the
  sides joined to pair or filter them. A row stream comes before the
  contributors its pairings look up (`JoinPlan.looked_up`: a contributor
  whose identity a pairing's keys cover). Finer identity comes first.
- Separate searches in one merge (`_joint_plan`) keep each search's order,
  in search order.
- Every other plan states none. Its sides rank alike and the name decides,
  as on `main`.

The first cut stated the parents' own order for every plan. That moved more
than half the corpus, and lost plan quality where the spelling had happened
to be good: `_q83_with_sales_measure` lost its predicate pushdown into the
union arms. The parents' order is no more a fact than a name. The cut above
states an order only where the plan knows one.

**An authored axis preserves only the joins keyed on it (part 89).** The
order the plan states found one more order-dependent row bug, which part
83's flip reached only in the corpus, where no test compares rows:

    merge store.customer.sk into web.billing_customer.sk;
    where web.sale_date.year = 2002
    select web.billing_customer.id as cid, sum(web.profit) as val;

The scan merge joins `web_sales` to `customer` and to `date`. The merged
customer key is a coalescing axis, so the customer join is FULL. When the
walk joined the customer first, `ensure_content_preservation` read that
earlier FULL as an authored axis and widened the date lookup to FULL too.
The WHERE on `year` then narrowed it to RIGHT OUTER. So every 2002 date with
no web sale became a row with a NULL customer, and the totals gained a
`(NULL, NULL)` customer. Walked the default way, the date joins first and
nothing widens it. On `main` the reversed key spelling
(`WALK_FLIP=key`) renders that RIGHT OUTER in `_q04_agent_rowset_union_join`.

**Root cause:** an authored FULL declares row intent for the axis's content
on both sides. A date that one channel's rows look up by its own key is not
that content: its unmatched rows are the date domain's. Only
`get_join_type` can license preserving them.

**Fix:** an earlier FULL on an authored axis no longer widens a later join
to preserve its right when that right is a lookup (its grain is inside the
join's keys) keyed off the axis. A fact joined after the axis still is
widened: the first cut, which widened only joins keyed on the axis, lost
the b-side totals in `test_property_hop_alignment_matrix`, where each
family's facts hang off its own customer key.
`test_axis_preserves_only_its_own_joins` checks the rows under the default
walk and each reversed order. It fails on `main` under the reversed keys.

**A FULL folds into its mirror (part 90).** Part 77 folds an outer join into
its mirror when both NULL-extend the same CTEs. It keyed a FULL's padded
sides as `left + right`, though, so `ss FULL JOIN sr` and `sr FULL JOIN ss`
never matched. Both QueryDatasource identities used the same order-bound
list (`_null_extended_sides`, `_null_extended_names`). The two arms of
`test_union_arms_having_on_union_join_key` each build the merge of `a` and
`b`. Under the stated order the second arm walks it the other way round, the
copies land in one CTE, and it rendered `"abundant" FULL JOIN "cheerful"
FULL JOIN "abundant"`, which DuckDB rejects. Two `test_multiway_matrix`
cases did the same. A FULL pads both sides, so its padded set is now
order-free in both helpers. `test_a_full_join_merges_into_its_mirror` pins
it, and fails on `main`.

**One key read off several sides stays one key at INNER (part 91).** The
suite run with the stated order reversed found one more, in
`test_store_and_web_or_catalog_with_property_and_aggregate`: `store AND (web
OR catalog)` over three `union` members of the customer axis. Walked from
the catalog, the merge is `catalog FULL web FULL store`, and store's join
reads the key off whichever of web and catalog the first FULL kept:
`coalesce(c.ck, w.wk) = s.sk`. The WHERE forces store, and its OR forces web
or catalog, so the optimizer narrowed store's join to INNER. That is right.

**Root cause:** an outer join groups a key's pairs by the right column and
coalesces the lefts. An INNER join, in the renderer and in the upgrade's
key proofs (`coalesced_key_groups`), grouped them by the left column too.
`c.ck` and `w.wk` are two spellings of one key, so the INNER rendered
`c.ck = s.sk AND w.wk = s.sk`, which demands both channels. The proof then
read each as proven and narrowed `catalog FULL web` to INNER as well. The
query returned no customers. The default walk joins store first, so its
web and catalog joins each read one left and stay LEFT.

**Fix:** pairs of one right key read off different sides are one coalesced
group however each side spells it. Two columns of one side are still two
keys. The renderer and the upgrade read the same grouping, so an INNER
renders `coalesce(c.ck, w.wk) = s.sk` and proves only the right key.
`test_or_of_members_survives_any_walk` runs it under every walk. It fails
under the reversed order without the fix.

Suite capture against `main` at `b29a609` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated failures.
The only other outcome change is the Rust-binary test's skip (listed noise).
Read over each test's distinct statements (the TPC-DS benchmark harness
re-generates a statement a timing-dependent number of times), 863 tests
change joins, CTEs or `coalesce` calls. Together they add up to CTEs -81,
FULL -95, RIGHT -104, LEFT +86, INNER +51, null-safe pairs +24 and
`coalesce` -144.

- The relation-rollup walk-order laws lose 18 CTEs and their FULLs in each
  property case: the two copies of a FULL merge now fold (part 90).
- The dimension-chain oracle nets about 20 fewer CTEs over 336 seeds. A
  handful gain two, more lose two.
- In the corpus (`corpus_sql`, 205 statements) 89 statements move. Three
  change join counts: q97 trades a FULL for a LEFT in each spelling, and
  `_q04_agent_rowset_union_join` gains one CTE, which the HAVING semijoins
  read in place of a wider one.
- Every moved test passes on both trees, and the oracle laws among them
  compare rows against raw SQL.

The suite run with the stated order reversed (`WALK_FLIP=key,side`) fails
20 tests beside the 25 credential-gated ones, and none on rows. 11 are the
walk's own unit tests, which pin the order. The other 9 pin shape: a join
or table count, a rendered join, or that an optimizer rule fired (each
`test_rule_soundness_laws` case checks its rows first, and they hold).

The shape pins the stated order moved now read either orientation of the
same join (`test_adhoc07`, gcat's `test_join` and `test_join_discovery`, the
landmark merge, the raw-column fold, the BigQuery merged key, the MySQL
lowering message). `test_merged_unnest_bridge_row_projection` pins the
three rows the data conforms on and accepts ecoregion 600's row either way,
as part 83 found the EQUAL declaration licenses.

**Scan plans state their order (parts 92-93).** After part 88, the two
plans that still stated no order were both plans between datasource scans:
a ROOT's source plan (`_bridge_plan`) and the select search's scans
(`_graph_join_plan`). Their sides ranked alike, so the scan's name decided.
Both now state `scan_precedence`, the scan analogue of
`JoinPlan.precedence`, through `PlannedPairs.over_scans`:

- a scan of rows comes before the lookups its pairings identify (a scan
  whose declared grain lies inside the keys some pairing joins it on);
- a scan binding its pairing keys completely comes before one binding them
  as a subset (`~`);
- finer declared grain comes first;
- a derived connector, whose rows are its subplan's, states nothing and
  comes after.

Each term reads the datasource's declaration (grain, `~` bindings), which
source planning chose. They are facts of the plan, not of a built node.

The subset term is not optional. Without it, `_q83_with_sales_measure`
walked from the returns union (two lookups of the same grain, so the name
decided) and paired the item lookup on the returns' `~` item key. That
join stayed LEFT with a coalesced key, and the return-date pin no longer
pushed into the union arms. This is the loss part 88's first cut hit. With
the sales scan stated first the plan is `main`'s again.

In the corpus (205 statements) 35 move, all orientation. `order_items`
now leads and `products` hangs off it (thelook q01-q05), and TPC-DS q17
joins `store_returns` straight after `catalog_sales`, ahead of the sale it
returns. No statement changes its join or CTE count.

**The stated order replaces the scorer's built grain (part 94).** Below the
fact terms (anchor, partial, hosting), `_score_join_candidate` ranked two
candidates by the size of each side's *resolved* grain, a built-node read,
and only then by the plan's order. Every plan that orders its sides already
states finer rows first (`JoinPlan.precedence`, `scan_precedence`; a star
states its hub). So the grain term is gone and the plan's order follows the
fact terms directly. With the order stated above it, the grain term moved
one corpus statement (an orientation of `_q04_agent_rowset_union_join`'s
FULL chain). Dropping it then moved none. One unit test pinned the grain
term as the walk's seed. It now states that preference as the plan's order.

What is left of the scorer is the three fact terms and the plan's order.
Sides a plan ranks alike (a star's spokes) fall to the name, as before.

**A finished walk carries no next edge (part 95).** The stated order found
a row bug in `recurse_edge`, under the self-imported tree:

    auto app_pid <- recurse_edge(pid, parent.pid);
    select app.pid, sum(mem) -> m;

The walk output both arguments, `pid` and `parent.pid`. Its recursive arm
rebinds the next argument to each ancestor's edge, though, so on a finished
(terminal) row `parent.pid` is the root's parent, which is always NULL. It
is never the seed row's parent. The per-group merge pairs the walk back to
the process scan on both, as the plan says the two share them. Walked from
the walk, pair reduction dropped `parent.pid` (the scan's grain `pid`
determines it), and the rows were right. Walked from the scan, the walk is
the right side, `parent.pid` is in its grain, and the join kept
`parent_pid IS NOT DISTINCT FROM parent_pid`. Only roots matched, so the
totals fell from `(1, 16), (4, 10)` to `(1, 10), (4, 7)`.

**Fix:** a walk carries its value and its seed, not the next argument
(`gen_recursive`). A reader of `parent.pid` reads it off the seed row, and a
walk joined back to its parent pairs on the seed alone.
`test_a_finished_walk_carries_no_edge_of_its_seed` runs every walk.

**No pushdown into the FROM base of a RIGHT join (part 96).** Under the
stated order TPC-DS q78 returned an extra row. Its merge now reads
`returns RIGHT OUTER JOIN sales`, the mirror of `main`'s `sales LEFT OUTER
JOIN returns`. Predicate pushdown then pushed `is_returned is null` into
the returns union's arms, where it reads `true is null` and empties them.
`_consumer_outer_joins_union` decides whether a consumer NULL-pads a union.
It checked a RIGHT join's explicit `left` only, and a join off the FROM
base has none. It now reads `null_padded_nodes`, which names the FROM base
and every side before a RIGHT or FULL. The unit test gains that case and
fails without the fix. q78 matches `main`'s rows again.

**A right that every left row partners keeps all its rows (part 97).** The
stated order moved no rows in the partial-lookup shapes
(`test_complete_binding_lookup`, the partial-lookup fallback oracle), but
it made their plans worse. It added FULL joins and null-safe pairs a level
up. The chain is `trees` (complete `species`), `enrichment` (`~species`,
`?genus`) and `genus_fits` (`~genus`). Walked from `trees`, the scan merge
reads `trees LEFT enrichment`, then `enrichment FULL genus_fits`. The
subset narrowing (`narrow_subset_full_join`) narrows a FULL onto a `~`
right only when its left source keeps every row (`preserved_sources`), and
it counted the right of a LEFT as dropped. So the FULL stayed. It padded
`tid` and `species`, the merge resolved at grain `(genus, species, tid)`
with `species` nullable, and the per-species readers above it joined FULL
on a null-safe key. Walked from `enrichment` (as on `main`), the later
RIGHT join to `trees` forced the narrowing instead.

**Root cause:** a right a join does not preserve still keeps every row
when each row has a partner. A join on one key from a surviving left that
holds the key's whole domain (`complete_key_domain`) partners every right
row whose key is not NULL.

**Fix:** `preserved_sources` counts such a right as kept
(`_partners_every_right_row`). Both walks now resolve that merge at grain
`tid` with both lookups LEFT.

Suite capture against `main` at `ee74856` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated failures.
The only other outcome change is the Rust-binary test's skip (listed
noise). One law moved to a new shape: the key-set upgrade's firing case
in `test_rule_soundness_laws` is now narrowed at resolve (part 97), so the
rule never runs on it, and the law reads another shape the rule still
narrows. Read over each test's distinct statements, with pytest's temp
paths normalized, 1,563 tests change joins, CTEs or `coalesce` calls.
Together they add up to CTEs -185, FULL -177, RIGHT -909, LEFT +747,
INNER +177, null-safe pairs -10 and `coalesce` -332. Most RIGHTs become
LEFTs: the fact now leads and its lookups hang off it.

- The partial-lookup fallback oracle and `test_complete_binding_lookup`
  turn their RIGHT joins into LEFT (387 and 192) and change no counts
  otherwise.
- The global-fallback oracle loses 113 FULLs and the relation-rollup laws
  150 CTEs. Each case checks its rows against raw SQL, and they hold.
- In the corpus 35 statements move, all orientation.

The suite run with the stated order reversed (`WALK_FLIP=key,side`) fails
35 tests beside the 25 credential-gated ones, and none on rows. 12 are the
walk's own unit tests, which pin the order. Six TPC-DS benchmarks check
their rows first, which hold, and then fail their generated-SQL size
budget (q04, q11, q35, q69, q74) or a table pin (q10). The other 17 pin
shape: a join or CTE count, a LEFT where the reversed walk renders the
mirrored RIGHT, or that an optimizer rule fired (the semi-join and
existence-fold pins, two `test_rule_soundness_laws` firing cases). Part
88's reversal failed 20. The extra 15 are the scan plans, which state an
order now and so have one to reverse.

**Which scorer terms still decide (census).** With the grain term gone,
the rank census (`rank_census_plugin.py`, its scorer replica brought up to
date) replayed every resolve over three or more sides, 15,326 merges in
the suite. Holding one scorer term constant moves the anchor term's order
in 120, the partial term's in 510, the several-`~` term's in 31, the
hosting term's in 985, and the plan's order in 671. All four built-side
terms still decide. A second replay put the plan's order *above* the
built terms (anchor kept on top). That moves 1,021 merges, but none of
the 520 the hosting term decides in `test_rollup_resolution_laws`. There,
every side hosting or not ranks alike in the plan (projections of one
split group, held at one rank), and hosting only breaks that tie.

**A row stream leads its lookups, bridge or not (part 98).** Most of the
other disagreements were one FINAL shape. In

    auto s <- count(id) by g, h; select id, s, count(id) as m;

the plan's contributors are `s` (identity `g, h`), `m` (identity `id`)
and the ROOT bridge that pairs them, which holds the rows at `id`. Both
aggregates are suppliers and both are looked up: the bridge's pairings
identify their rows. `JoinPlan.precedence` put suppliers first, so the
bridge holding the rows came last. Below that, it ranked `s` before `m`
because two identity keys outnumber one, though `g, h` is no finer than
`id`. The hosting term (+3) overrode that order and seeded the walk on
the bridge. The plan now states a row stream before the lookups its
pairings identify ahead of the supplier term: whoever holds the rows the
others are looked up on comes first, supplier or bridge. On its own the
swap moves no SQL (hosting already chose that seed). It makes the plan
agree with hosting in the window-group, fixed-aggregate, fixed-value
lifecycle and rank-beside-partial laws. Over the census's 16 heaviest
files, ranking the order above hosting then moves 30 merges, down from
several hundred.

**The plan's order outranks hosting (part 99).** `_score_join_candidate`
now ranks, in order:

1. the authored anchor (a statement fact);
2. a lone `~` binding of the root key after its complete holders;
3. the plan's order;
4. hosting, which only breaks ties among sides the plan ranks alike;
5. the several-`~` bump (several `~` bindings of the key meet first);
6. the name.

The several-`~` bump moved below hosting with it. Hosting (+3) had
always outranked it (+2). Left above the order, it seeded
`test_multi_fact_preaggregation`'s two `~cust_id` aggregates ahead of the
customer domain, so the facts met FULL before the domain arrived. Below
hosting, the domain seeds again and both facts hang LEFT off it. It was
the one regression the first cut found.

What was tried and left out: stating the plan's order in *tiers*, so that
groups whose precedence terms tie share a rank and hosting, not a
spelling, breaks the tie. Over the top-moving files that cost INNER
-174, LEFT +217 and `coalesce` +244. Group plans and scan plans each
account for about half, and it lost the q64 semi-join probe. The
positional order inside a tie is the cover's contributor order for a
group plan and the datasource name for a scan plan. It is still a
spelling, but it orients the walk better than the identifier the scorer
would fall to. Until a fact replaces it, the tie order stays positional.

Suite capture against `main` at `8072ea0` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated
failures, and the only other outcome change is the Rust-binary test's skip
(listed noise). 41 tests change joins, CTEs or `coalesce` calls. Together
they add up to FULL -12, null-safe pairs -6, `coalesce` -4, INNER +8,
LEFT -15 and RIGHT +25.

- The partial-key assembly and predrop-chain FULLs become LEFT joins off
  the side holding the rows.
- The padded-population gate and partial-lookup oracle cases gain an
  INNER join where a LEFT join had stood, and drop a null-safe pair.
- `test_derived_key_domain` mirrors about a dozen LEFT joins to RIGHT,
  the same rows from the other end.
- The CTE count rises by 14 in the tally, but none of it is in a shipped
  plan. The predrop-chain cells compile each query optimized and
  unoptimized, and the two compiles now differ. The rest is the CLI
  consistency and validation tests, which are listed noise.

Every moved test passes, and the oracle laws among them compare rows
against raw SQL. In the corpus (205 statements) one statement moves
(hackernews `adhoc03`, orientation only).

**Which scorer terms still decide, after part 99 (census).** Replayed over
the suite at `239cd0a` (14,981 merges of three or more sides), holding one
`_score_join_candidate` term constant moves the lone-`~` penalty's order in
640 merges, hosting's in 590 (520 of them the rollup laws' ties), the
authored anchor's in 92 and the several-`~` bump's in 9. Of the two terms
still above the plan's order, the anchor reads a statement fact (declared
`subset` relations) and the lone-`~` penalty reads the `~` bindings of the
datasources stage 3 picked.

**Two members joined before their anchor narrow onto it (part 100).**
Dropping the anchor term failed two tests on rows. One is a real order
bug. In

    subset join web.cust = store.cust
    subset join cat.k = store.cust
    where web.cust is not null or cat.k is not null

walked from the members, `cat FULL web` meets `store` FULL on
`coalesce(cat.k, web.cust)`. Resolve types every `~` pairing FULL and the
optimizer's declared-subset narrowing
(`UpgradeOuterFromKeySetEquivalence._narrow_directionally`) narrows it. A
member left of the join must not have been null-extended by an earlier
outer join (`_null_extended_before`), and here each member was padded by
the other's FULL. So the FULL stayed, and a web-only customer outside the
anchor survived. Walked from `store`, each member hangs LEFT off it and
the narrowing drops that customer. **Fix:** when every CTE joined before
the target is a left it reads its one key off (`_lefts_alone_before`), a
row entering the join is some member's own row, and the ON's coalesce of
the members is that row's key. The members' padding of each other is no
NULL there, and the FULL narrows to RIGHT onto the anchor.

The other failure had no bug. `test_three_source_chained_outer_join`
declared `subset join web = catalog = store` (web ⊆ catalog ⊆ store) over
data where web holds a pair catalog does not. Walked from catalog, web's
extra row was no member of catalog and dropped; seeded on store, it
survived. A lying declaration is author error (subset/union design), so
neither walk is wrong. The test now declares the chain its data holds
(catalog ⊆ web ⊆ store) and runs under every walk, as does the star form.

**A declared subset member follows its anchor (part 101).** With the term
gone, four `subset join` shapes walked their members first: `a FULL b`,
then RIGHT onto the anchor, where the anchor had seeded two LEFTs. In
`test_subset_rowset_anchor_rows` the plan put the anchor (`orders`) last,
because its identity `id` lies inside the keys the members pair on, so it
reads as their lookup. The anchor term had overridden that. The plan now
states it: `JoinPlan.subset_sides` names the contributors holding a
declared `subset` member whose anchor another contributor holds, read off
the domain graph's declared pairs and each side's carries at stage 2.
`precedence` ranks them after every other side, ahead of its other terms.
Scan plans already state a `~` scan after the complete ones
(`scan_precedence`).

**The walk reads no anchor, and no several-`~` bump (part 102).** With the
members' order stated and the narrowing order-blind,
`_score_join_candidate` ranks:

1. a lone `~` binding of the root key after its complete holders;
2. the plan's order;
3. hosting, between sides the plan ranks alike;
4. the name.

The several-`~` bump (below hosting since part 99) decided 9 merges, none
on rows. Dropped, the files it decided pass as they stand. `anchor_keys`
remains a typing fact (authored and axis keys).

**A superset of the sup side is no spelling of it (part 103).** With the
anchor no longer seeding, the reversed walk found a row bug in
`subset join kc = kb = ka` (kc ⊆ kb ⊆ ka, data conforming) and in its
pairwise form (`kb = ka`, `kc = ka`). Walked `c, b, a`, the optimizer
narrowed `c FULL b` to LEFT, keeping `c`, then to INNER, and `b`'s bucket
40 (no `c` row) lost its value. `_proven_subset_of` matches the sup side
through its pseudonym closure, since rendering may re-address it. Both
forms spell `kc` as `ka` too, so `kb ⊑ ka` read as `kb ⊑ kc`. It already
excluded union siblings for the same reason. **Fix:** a spelling the sup
side is itself a subset of is no stand-in for it either (its own address
stays). The chain narrows `c FULL b` to RIGHT, keeping `b`; the pairwise
form, which declares nothing between `b` and `c`, keeps it FULL.
`test_chained_equality_join_matches_pairwise` now runs under every walk.
A first cut that also dropped the sup side's own address (subset proofs
are reflexive) grew 13 TPC benchmarks past their SQL budgets.

Suite capture against `main` at `239cd0a` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated
failures, and nothing else changes outcome. 25 tests change joins, CTEs
or `coalesce` calls, adding up to FULL -12, INNER +7, LEFT -13, RIGHT +20,
`coalesce` +3 and CTE +2.

- The rollup cases in `test_derived_key_domain` (12) lose a FULL each:
  two facts joined before their padded region narrow onto it (part 100).
- The `~` cluster lookups in `test_cluster_key_lookup` turn a LEFT into
  INNER, and several `subset join` shapes mirror LEFT to RIGHT.
- Two shapes pay for the anchor term. `test_double_partial_key_join_order`
  is a scan plan over `merge x into ~y` facts. `scan_precedence` ranks the
  complete dimension last as their lookup, so the facts meet FULL and the
  dimension joins RIGHT, where the anchor had seeded two LEFTs. In
  `test_duckdb_subset_join_pivot_axis` the FINAL gains a wrap CTE: there
  the ROOT side carries both the anchor and the member, so
  `subset_sides` names neither. Stating the anchor in scan plans needs the
  declared pairs plumbed into `PlannedPairs.over_scans`' two callers.

In the corpus (205 statements) two statements move, q97 one and two: the
two channel facts pair first and the customer domain joins after them
(same CTE and join counts).

The suite with the stated order reversed (`WALK_FLIP=key,side`) fails 41
tests beside the 25 credential-gated ones, none on rows. That is `main`'s
39 at `239cd0a` plus two that pin the stated order: the anchor-shape pin
above, and `test_resolve_join_order_v2_multi_partial`, which pinned the
several-`~` bump's order. Before part 103 it also failed the chained
parity test, on rows.

**A scan plan states a `~` binding after its anchor (part 104).**
Part 102 dropped the anchor term, and with it the shape of a scan plan
over `merge x into ~y` facts: `scan_precedence` ranked the complete
dimension last, as their lookup, so the facts met FULL and the dimension
joined RIGHT (`test_double_partial_key_join_order`). A scan plan knows
its datasources, so it can state what the anchor term read. A scan
binding a key as `~` now comes after a scan it pairs with that binds
the key completely (its anchor), ahead of the row/lookup term. The facts
hang LEFT off the customer dimension again, and q97 one/two seed on the
customer domain again (part 102 had moved them).

One fact needed care. A union arm's datasource is partial on every
column, its grain included: its rows are a subset of the union's rows,
and none of its keys is a `~` binding. The first cut read every partial
column as a binding, so in q69 the date dimension anchored each sales
arm and a WHERE on the date left the scan merge (+3 CTEs each in q35 and
q69). A scan partial on its own grain now anchors only on a key that a
declared `subset` relation names. The declared keys come from the domain
graph; `PlannedPairs.over_scans` reads the environment for them.

**The walk reads no `~` binding a scan plan orders (part 105).** With
the anchor stated, a scan plan states every binding the lone-`~` penalty
read, so the walk drops the penalty for such a plan
(`PlannedPairs.orders_bindings`, read through `JoinFacts`). One kind of
`~` stays unstated: a scoped `subset join` on a derived key. No
datasource binds it, and the scan cannot see which side computes it.
A plan pairing on one says it orders no bindings, and the walk reads
them as before (`test_scoped_left_join_on_nonrowset_derived_key` found
it). A group plan still cannot state the penalty, since stage 3 picks
the bindings. So the penalty now reads only group plans, and scan plans
pairing on a scoped derived key.

Suite capture against `main` at `dc69dcf` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated
failures, and the only other outcome change is the Rust-binary test's
skip (listed noise). Two captures of one tree differ in
`tpc_ds_duckdb/test_queries.py` and `thelook_duckdb/test_thelook_queries.py`,
which are left out of the tally. The corpus covers those queries. The
rest adds up to FULL -24, INNER -2, LEFT +54, RIGHT -34, `coalesce` -17,
null-safe pairs -1 and CTE -5.

- `test_derived_key_domain` loses 15 FULLs and 21 `coalesce` calls:
  two `~customer_id` facts no longer meet FULL and then join RIGHT onto
  the customers they hang off; each joins LEFT off customers.
- The dimension-chain oracle loses 5 CTEs and a null-safe pair.
- gcat's launch/vehicle shapes lose 6 FULLs.
- `test_partial_key_join_order` hangs both facts LEFT off the customers
  again (now pinned).
- The rest mirrors RIGHT to LEFT, the same rows from the other end.

Every moved test passes. The oracle laws among them compare rows against
raw SQL.

In the corpus (205 statements), seven statements move. q97 one/two
re-seed on the customer domain (part 104). Five turn an INNER around or
mirror a LEFT/RIGHT onto the row stream (part 105): q83, TPC-H q09, and
thelook adhoc02, query18 and query22. CTE and join counts are unchanged.

The census (`rank_census_plugin`, the whole suite) replays each merge of
three or more sides with one scorer term held constant. At `dc69dcf`, the
lone-`~` penalty moves the walk in 694 of 14,901 merges; here it moves
486 of 15,117. The rest are group plans and scoped derived keys. The
plan's order now decides 6,067 (5,362 at `dc69dcf`), and hosting is
unchanged (593). The scorer replica differs from the scorer in 12 merges
on both trees, so that mismatch predates this change.

Reversing the stated order (`WALK_FLIP=key,side`) now fails 14 tests
beyond the 66 that `main` fails under the flip at `dc69dcf`, and none
fails on rows. The anchor and the `~` order used to live in the scorer,
which the flip leaves alone. Now they live in the scan plan's order,
which the flip reverses, so the shapes they set get pinned as order:
- LEFT-off-anchor pins: `test_subset_join_renders_left_outer`,
  `test_join_keyword_tracks_authored_type`, the gcat launch/vehicle
  pins, `test_double_partial_key_join_order`, and the existence-HAVING
  fold;
- q64's no-RIGHT pin, checked after its rows;
- two rule-fires checks in the rule soundness laws, whose rows still
  match;
- this part's own walk unit test.

**Resolve states every `~` binding's place, once per merge (part 106).**
After part 105 the `~` order had two owners and a flag between them. A
scan plan stated it from its datasources (`scan_precedence`'s anchor and
partial terms). For every other plan the walk's scorer read it key by key
(the lone-`~` penalty), with state carried across steps (`multi_partial`,
judged when a key first ranked best). `PlannedPairs.orders_bindings` told
the walk which of the two applied, through the MergeNode and `JoinFacts`.

Part 105 found why no plan can own it: the `~` bindings are the built
sides'. A group plan does not know which datasources stage 3 picks, and a
scan plan does not see a scoped `subset join` on a derived key. Resolve
knows both, since each side's facts carry its partials. So resolve now
states the order, for every plan, before the walk
(`_binding_order`, called where `get_node_joins` builds the facts):

1. the plan's order, as before;
2. after those, the sides partial on a key they pair on;
3. after those, the sides binding a pairing key as `~` whose partner
   binds it completely (the anchor rule of part 104).

A union arm keeps part 104's exception: it anchors only on a key a
declared `subset` relation names. An arm is now a side fact
(`SideFacts.arm`: the side reads one datasource that is partial on its
own grain). Two cuts of that fact failed first, and the suite capture
found both:
- read off the built side's grain, an aggregate by a `~` key
  (`sum(...) by part_id` over a `~part_id` fact) read as an arm, and a
  dimension joined RIGHT onto it (TPC-H adhoc04);
- read as "every column is partial", an unnest over an INNER join with a
  `~` enrichment read as an arm, and 432 LEFTs in the unnest filter laws
  mirrored to RIGHT.

What this removes: `PlannedPairs.orders_bindings` and its plumbing through
`MergeNode`, `get_node_joins` and `JoinFacts`; the partial, anchor and
declared-key terms of `scan_precedence`, which is now grain only; the
environment parameter part 104 added to `PlannedPairs.over_scans` and its
two callers; the walk's `multi_partial` state. The scorer's first term is
gone.

**The scorer reads no binding (part 107).** With the order stated,
hosting was the scorer's last binding read (`hosts and not is_partial`,
on the root key). The census replayed it held constant: it moved no
merge in the suite. Hosting already requires the host keys bound
completely. It is dropped, and with it the scorer's `root` argument:
`_score_join_candidate` is the stated order, then hosting, then the name.

Suite capture against `main` at `a705d3c` (`sqlcap`, `-n 4`,
`PYTHONHASHSEED=0`): both trees have the same 25 credential-gated
failures, and the only other outcome change is the Rust-binary test's
skip (listed noise). Left out of the tally: the two capture files that
move between two runs of one tree (part 105), the script tests and
`test_duckdb_validate`, which compile a varying number of statements,
and the flipped-walk variants, which reverse the order this part states.
Of the rest 292 tests move, adding up to FULL -4, INNER +3, LEFT +29,
RIGHT -28, `coalesce` -5, null-safe pairs +1 and CTE 0. Join counts are
unchanged. 131 of the 292 are the oracle spellings laws, which move by
the column order of one CTE and by a different number between two
captures of this tree (50, then 131).

- `test_derived_key_domain` (14) loses 4 FULLs, and 19 RIGHTs mirror to
  LEFT off the customer rows.
- The dimension-chain oracle (23) turns 6 LEFTs into INNERs and gains one
  null-safe pair.
- `test_cluster_key_lookup` (19) turns 4 INNERs into LEFTs.
- `test_unmodelled_regions` mirrors 4 RIGHTs to LEFT and loses 3
  `coalesce` calls.
- The rest move by an INNER turned around or a mirrored outer join.

Every moved test passes. The oracle laws among them compare rows against
raw SQL.

In the corpus (205 statements) two statements move, both thelook: query18
hangs its aggregate LEFT off users where it joined users RIGHT, and
adhoc02 hangs the pair rollup LEFT off products the same way. CTE and
join counts are unchanged.

The census (`rank_census_plugin`, the whole suite) gained a
`binding_order` variant: each merge replayed under the plan's order with
no binding placed. The stated binding order moves 1,857 of 15,159 merges.
That is more than the penalty's 486 at `a705d3c`, since it now also
counts what scan plans stated themselves. Hosting is unchanged (594),
and so are the region terms (solid 24, held 31, max-held 29). The scorer
replica differs in 13 merges (12 at `a705d3c`), all flipped-walk tests.

**A forced LEFT states its seed (part 108).** Reversing the stated
order (`WALK_FLIP=key,side`) found a row bug in 72 relation-rollup law
cases, all of the `element` spelling. A relation rollup reads a summary
through its list's pairs: `_star_merge` joins the pairs to the summary
with `force_join_type=LEFT_OUTER`, since a species listing no ecoregion
keeps its trees under a NULL one. The MergeNode stamps that type on
whatever join the walk returns. LEFT keeps the side walked first, so the
type was right only while the summary seeded. The plan put it first, as
the star's hub, but only as a tie-break. Until part 106 the lone-`~`
penalty also kept the pairs (a `~` binding of the species) behind it, in
the scorer, where the flip did not reach. With the binding order stated
and reversed, the pairs seeded, the LEFT kept them, and the species with
no pair lost its rows:

    select tree_eco_region, count(tree_id) as n;
    -- walked from the summary: (0, 3) ... (4, 6), (NULL, 2)
    -- walked from the pairs:   (0, 3) ... (4, 6)

**Fix:** where a merge forces LEFT, the side its plan states first is a
fact about rows, not a tie-break. The MergeNode names it
(`_forced_seed`), `JoinFacts.seed` carries it, and the walk starts from
it whatever the rank says. A forced FULL or INNER is symmetric and names
none. `test_the_summary_keeps_its_rows_whichever_side_is_walked_first`
runs every shape under each walk.

With the seed stated, the reversed walk fails 97 tests against the 80
`main` fails under the flip at `a705d3c`: 19 new, 2 gone (the
`orders_bindings` unit tests), and none on rows. The `~` order of group
plans used to sit in the scorer, which the flip leaves alone; it now
sits in the stated order, which the flip reverses, so the shapes it set
are pinned as order:
- LEFT-off-anchor pins in the scoped and rowset `subset join` tests
  (`test_scoped_left_join_on_nonrowset_derived_key`,
  `test_scoped_derived_rowset_join_correct`,
  `test_subset_join_blends_two_rowset_measures` and four more shape
  pins of that family);
- the plan-trace and region-typing pins of a first LEFT
  (`test_first_region_holder_is_left_at_plan_time` and two trace tests);
- TPC-H q13's no-`count(CASE` pin, a rule-fires check in the filtered
  count join, and the city build's no-null-safe-filter-column pin;
- three walk unit tests that pin the order they state.

**The walk reads one order (part 109).** After part 108 the walk still
ranked twice. It ranked every planned pairing by its key
(`_pivot_rank`: authored, solid, held span, most other held spans,
holder count, the holders' places, the key's name), took the best key
touching the joined set, and scored that key's unjoined holders
(`_score_join_candidate`: the stated order, hosting, the name). Only the
second read the order resolve states.

The key rank chose nothing but the next side: `_join_onto` joins a side
onto every walked side the plan pairs it with, on all their keys,
whichever key picked it. So the question was how much of the walk the
key rank decides at all. Two measurements at `ed9a5ad`:

- The census (15,193 merges of three or more sides) with each fact term
  held constant: authored 548 (477 of them `test_cluster_key_lookup`),
  solid 24, held 31, most-held 29, hosting 594.
- The whole suite under a walk with no key rank (join the best-scored
  side the plan pairs with a joined one): 24 tests fail beyond the 25
  credential-gated ones. Sixteen are in tests that assert rows (the
  padded-NULL, property-hop, scoped-join and derived-connector
  families); the rest are four walk unit tests and four shape pins
  (q75's SQL budget, the q64 semi-join probe among them).

So the walk is one order of sides, and the key rank is a statement about
where a side comes. The walk now reads only that
(`_walk`): each step joins the side stated first (`facts.rank`) among the
unjoined ones the plan pairs with a joined one, and a side pairing with
none seeds a tree only when none does. `_Pivot`, `_pivot_map`,
`_solid_value_null_pivots`, `_pivot_rank`, `_ranked_edges` and
`_score_join_candidate` are gone, with the per-step scan of ranked
edges. The name breaks ties in one direction: the scorer took the last
name and every other read took the first, and now all take the first.

**Resolve states the whole order, rows above shape (part 110).**
`_stated_order` (where `_binding_order` stood) states it once per merge:

1. A side comes where the best key the plan pairs it on puts it.
2. A key is placed by what its holders' rows are (`_key_row_terms`):
   an authored key, then a value-nullable key between region-free
   holders, then a held span, then the fewest other regions held, then
   the fewest holders.
3. Keys alike on those are placed by where the plan's shape puts their
   holders, so a lookup follows the rows it hangs off.
4. Sides one key places alike come in the shape's order
   (`_shape_terms`): the `~` tiers of part 106 (`_binding_tiers`), the
   plan's order, then hosting.

Hosting moved from the scorer into the shape. Held constant on `main`,
all 170 tests it decides pass, so it is shape.

A first cut placed each side by its own facts alone (pairs on an
authored key, holds no region, how many regions it holds), with no
holders' places. It passed the suite's rows but moved 18 of the corpus's
205 statements and lost the q64 semi-join probe: the two year aggregates
met before the enrichment both pair with, and the mirror no longer
fired. Placing keys by their holders' shape moves 1 statement (TPC-H
q09, one INNER lookup in another position) and keeps the probe.

The flip reverses what is shape and nothing else.
`tests/helpers/walk_order.py` reverses `_shape_terms` (`shape_flipped`:
tiers, plan order, hosting, and with them the holders' places) or the
name (`names_flipped`); the row terms stay. The old flip left hosting
alone and reversed no term above the holders' places.

**Which terms decide rows (part 111).** Each row term held constant on
this tree, over the 148 tests the earlier censuses and the no-key-rank
run named:

| term | fails on rows | witness |
|---|---|---|
| authored | 3 | `test_property_hop_alignment_matrix`: two facts sharing `sku`, the authored key on their dimensions |
| solid | 1 | `test_address_only_rows_stay_their_own[two_value_null_aggregates]` |
| held span | 1 | `test_where_kept_region_row_beside_a_solid_total_by_another_key` |
| most other held spans | 1 | `test_an_aggregate_holding_both_regions_pairs_after_they_are_stitched` |
| holder count | 3 | `test_multileg_sql_parity_via_not_null`, and two `test_padded_null_pairing` WHERE cases |

Holder count is the new one. Part 71 read it as the plan's shape (how
many sides pair on a key), and the flip never reversed it, since it
reversed only the two terms below it. It decides rows:

    union join ss.ticket = sr.ticket and ss.item = sr.item and ss.customer = sr.customer
    union join ss.customer = cs.customer and ss.item = cs.item

The plan pairs `cs` with `sr` on (customer, item), by transitivity; no
relation was authored between them. `ticket` has two holders and the
other keys three, so the fewest-holders term joins `sr` and `ss` first.
With the term held constant the plan's order walks `cs`, `sr`, `ss`,
the first FULL pairs the two sides no relation was authored between,
and `test_multileg_sql_parity_via_not_null` returns no row where it
expects one. Held constant over the 444 tests it decides, the term
fails those three on rows and four shape pins (q75's budget, the q64
probe, a refused FULL lowering, a rule-fires check).

These five terms are what is left of the order that decides rows, and
each now has a named witness. The authored one is a property of a
left-deep tree (the authored equality has to be in the tree before the
cheaper key pairs its sides). The other four are typing that is right
in one direction only, the kind parts 74 to 108 fixed for the scorer's
terms.

Suite (`-n 4`, without adventureworks/clickhouse/bigquery/cloud_live):
the same 25 credential-gated failures as `main` at `ed9a5ad`.

Suite capture against `main` (`sqlcap`, `-n 4`, `PYTHONHASHSEED=0`):
`sqldiff` reports 790 of 46,106 tests moved. About 220 are the worktree
path in file-source SQL (the dashboard and script-source tests). Over
the rest, with the listed noise left out, the statements that changed
add up to FULL 0, INNER +5, LEFT -3, RIGHT 0, `coalesce` 0, null-safe
pairs +32 and CTE 0.

- The rollup-resolution laws (182) turn 6 LEFTs into INNERs.
- The partial-lookup fallback oracle (142) walks its two row CTEs in the
  other order, the same INNERs from the other end, and gains the
  null-safe pairs.
- The oracle spellings laws (82) are the run-to-run movers of part 106.
- The rest turn an INNER around or reorder lookups under one row stream.

Every moved test passes, and the oracle laws among them compare rows
against raw SQL. I did not capture `main` twice, so the split between
noise and reordering in the law grids is by reading diffs, not measured.

Reversed (`WALK_FLIP=shape,name` here, `key,side` on `main`): 101 tests
fail against `main`'s 97. Outside the two walk unit files, which pin the
order they state (21 here, 17 on `main`), the two runs fail the same 55
tests, none on rows.

The census (`rank_census_plugin`, rewritten over the stated order: it
holds one term constant and walks again) over the whole suite, 15,196
merges of three or more sides: the plan's order decides 6,095, the `~`
tiers 1,869, holder count 1,086, authored 544, the holders' places 359,
held span 41, most-held 39, solid 34 and hosting 13. Hosting fell from
594 because the name now breaks its ties toward the first name, where
the scorer took the last.

One thing the reversed run showed and nothing here uses: it left 14 TPC
benchmark logs ratcheted to shorter SQL (q02 from 4,817 to 4,403
characters), so for those queries the reversed shape renders less text
than the stated one. Whether it is the better plan was not measured.

**Holder count read a transitive pairing (part 112).** The multileg
witness of part 111 is a plan bug, not an order. The source search pairs
two scans on every value class both bind, and `union join` merges
`ss.customer`, `sr.customer` and `cs.customer` into one class, so the
bridge plan paired `cs` with `sr` on (customer, item), which no relation
states. Walked `cs`, `sr`, `ss`, that pairing is a FULL join that pads
before `ss` arrives: a `cs` row with no `sr` partner carries a NULL
`sr.ticket`, so it never meets its `ss` row.

A pairing implied through a third side holds only when that side holds
every value of both: `ss ⊑ customer` and `cs ⊑ customer` imply
`ss.c = cs.c` (q97 pairs on it), but `ss ∦ sr` and `ss ∦ cs` imply
nothing about `cs` and `sr`. The bridge plan now drops a class two scans
share only through a side that is no anchor of both
(`join_key_groups.without_transits`, over the declared `subset`/`union`
edges and each datasource's native binding). It is a plan fix: the walk
in either order now pairs `cs` only with `ss`.

**A filtered total met first was typed INNER (part 113).** Two witnesses
of holder count and the held-span witness are one shape: a
WHERE-filtered aggregate (`sum(amount) by customer_id` under
`where amount is null`) beside the rows it rolls up (orders, or a filtered
copy of products). Neither side is nullable or partial on the key, so
`_nullable_join` typed them INNER. Walked after an outer join,
`ensure_content_preservation` widened that INNER to LEFT; walked first,
nothing widened it, and the INNER applied the WHERE to the orders before
the region joined them. The region's members those orders matched (bucket
`b`, order 102) became unmatched region rows, and a WHERE their padded NULL
passes kept them.

The pair is now typed preserving the unfiltered rows wherever it is
walked (`_unfiltered_copy`): one side past filters the other skipped, all
of which the merge applies itself after every join (`JoinFacts.applied`,
the merge's own condition atoms), its grain determined by the other's
rows. Rows the INNER dropped fail the merge's WHERE, except where a later
join changes what they read, which is the bug. A filter the merge does
not re-apply (a recursive walk's `_terminal`) types as before.

**Coalesced null-safety reached padding (part 114).** The solid witness:
two value-NULL aggregates (`sum(amt) by cust_sk`, `sum(pay) by cust_sk`)
and an addresses-preserving customer side whose NULL `cust_sk` is padding.
Walked with the padded side first, the second aggregate joins on
`coalesce(addresses.cust, returns.cust) is not distinct from pay.cust`:
the FULL key coalesces every left, and the null-safety the returns side's
value NULL earned reaches the padded addresses too, pairing them with the
payments' NULL group. The join now carries a guard for each such left
(`_coalesced_padding_clauses`): its rows pair only where it holds the key,
or has no row (`addresses.cust is not null or addresses.addr_sk is null`).
It fires only when the right's NULL is a value, another left's NULL is a
value, and the right does not pad for the same spans (the dimension-chain
law's flight with no tail pairs its NULL group with its own count).

**Most other held spans is the tree's (part 115).** Its witness is
`count(customer_id) by bucket` beside the customers and their orders: the
count holds both regions, and a customer with no order is in its NULL
bucket group. That pairing exists only once customers and orders are
stitched, so the stitch has to be in the tree before the count joins.
Walked count first, the customer's padded NULL bucket is minted by a
later join and nothing pairs it back. No typing restores it: like the
authored term, it is a property of a left-deep tree.

So of part 111's five row terms, holder count, solid and held span now
decide only shape on their witnesses, which are pinned under each of them
held constant (`tests/helpers/walk_order.ROW_WALKS`; the walk-flip plugin
takes `WALK_FLIP=solid,held,holders`). Authored and most-held remain, both
a stitch the tree needs first.

Corpus (205 statements, against `26ee111`): 1 moved, `tpc_ds/queries`
query 1, where a filtered per-store average joins LEFT instead of INNER
beside rows the same WHERE keeps (row-identical). Suite (`-n 4`, without
adventureworks/clickhouse/bigquery/cloud_live): the same 25
credential-gated failures as `main` at `26ee111`. Fuzzer: 522/522.

With all three terms held constant together
(`WALK_FLIP=solid,held,holders`), the suite fails those 25 and 7 more,
none on rows: three walk unit tests that pin the order they state, and the
four shape pins part 111 named (q75's SQL budget, the q64 semi-join probe,
a FULL lowering refused for another reason than the one pinned, the
`DropIdentityGroup` rule-fires check).

**Most-held is a stitch the walk waits for (part 116).** Part 115 read
the term as a property of a left-deep tree. That is right about the tree
but not about the rank. The count in its witness pairs only on `bucket`,
with the orders. It holds the customer region's rows too, on a span the
plan pairs it on with no side, so its padded rows are those of a stitch
two other sides make: customers and orders on `customer_id`. The term
placed `bucket` after `customer_id` because the count holds one more
span, a rank over keys. What the tree needs is narrower: the count may
not join before the sides the plan pairs on that span.

So the walk reads it as that (`JoinFacts.stitches`). A side holding a
region's rows on a span it pairs on with no side waits for every side
the plan pairs on that span, and the walk joins the side stated first
among the paired sides whose stitches are walked (`_stitched`), or among
all of them when none is. A seed is picked the same way, since a seed
the walk starts from is joined before everything.

The term stays in the order, now as shape. A first cut dropped it. That
kept the corpus, and the suite's rows, but the suite capture moved 8
statements, each worse: in the dimension-chain oracle and
`test_the_solid_rows_keep_the_region_spans`, a side holding two spans
seeded the walk and two LEFTs became FULLs. So the count of other spans
a key's holders hold still places the key, as plan quality, below what
the stitch guarantees for rows.

The witness is now pinned under every walk, the reversed shape included.
With the stitch disabled, it fails in 6 of its 12 walks.

**Solid, held span and most-held are the key's shape (part 117).**
Holding a term constant (part 111) shows only that it decides no rows on
its witnesses. Reversing it is the stronger test. `_key_terms` now states
each key as authored, then a shape (`KeyShape`: solid, held span, most
other spans held), then its holder count. The order is the one
`_stated_order` already read, so nothing moves (corpus 0, suite
unchanged). `shape_flipped` reverses the key's shape along with the
side's (`tests/helpers/walk_order.flipped_keys`), and every
walk-parametrized test (2,795) passes that way. `ROW_WALKS` and the
held-constant walks fold into `WALKS`.

Over the whole suite (`WALK_FLIP=shape,name`), the reversed key shape
fails nothing on rows that `main`'s flip does not. Reversing one term at
a time, with and without the side shape, rows move only when the holder
count is reversed.

**An INNER pair two left nodes share is one pair only where they agree
(part 118).** The first whole-suite run with the reversed key shape
found a row bug in the expressions oracle (seed 8, `summary`).
`reduce_concept_pairs` dropped a second INNER pair on the same
(right, left) columns read off another left node, as if every left were
already equated on it. Walked the other way, a store summary met the
gated trees on `species` alone. Then the per-tree value joined both on
`tree_id`, but kept only the trees' pair, so the summary's rows fanned
out across the species. The FD pruning and grain restriction in the
same function already ask `_Agreement`; this dedup now asks it too.
Corpus: 0 moved. The case is pinned under every walk
(`test_two_lefts_met_on_another_key_each_keep_their_pair`).

**Holder count still decides rows (part 119).** Part 112 fixed the
witness part 111 named, a transitive plan pairing. Reversing the holder
count over the whole suite finds six more, each passing on `main`'s
flip and failing with the count reversed (all but one also need the
side shape reversed):

- `test_derived_key_domain::test_completion_keeps_its_padding_beside_a_domain_on_its_span`
  (two cases: a customer with no order gains a padded row);
- `test_unmodelled_regions::test_two_facts_at_one_grain`;
- TPC-DS q40, q75 and q80 (row values), and
  `test_truncated_search_seed_fallback_returns_correct_rows`.

So the holder count stays a row term, below the key's shape, and
`WALK_FLIP=holders` (`walk_order.HOLDER_FLIPS`) reverses it to
reproduce them. These six are the next witnesses: each is a typing or
pairing that holds in one direction only, the kind parts 112 to 114
fixed.

The order the walk reads is now two row terms of a key amid shape.
Authored is a key rank a left-deep tree needs. Its witness is a 4-cycle:
two facts share `sku`, and the authored key sits between their
dimensions. The authored edge has to be a tree edge, so the cheap `sku`
edge closes the cycle together with the dimension key instead of pairing
the facts alone. The holder count has the six open witnesses above.
Most-held is a stitch the walk waits for, and otherwise shape.

Gates, against `7c54bf1`:

- Corpus (205 statements): 0 moved.
- Suite (`-n 4`, without adventureworks/clickhouse/bigquery/cloud_live):
  the same 25 credential-gated failures as `main`. Suite capture: join
  counts unchanged outside the known statement-count noise. About a dozen
  tests outside the noisy oracle laws (the sweep shapes, the padded
  population gate, where-hosts) move by part 118: an INNER join now keeps
  a second left's pair on a key and a later pass keeps one of two pairs
  that are equal by FD, so the ON reads the key off the other left. Every
  one passes its row assertions.
- Whole suite under `WALK_FLIP=shape,name`, which now reverses the
  key's shape too: the same failures as `main`'s flip (101), plus one
  walk unit test that pins the order it states. Before part 118 it also
  failed the expressions oracle; with the holder count reversed as well,
  it also fails the six witnesses of part 119.
- Fuzzer: 522/522.

**A `~` side meets a coarser one only through its anchor (part 120).**
All six witnesses of part 119 are one shape: a returns side bound `~` on
the keys it pairs its sales (or lines) on, and a coarser side (items,
orders) the plan pairs with both on part of those keys:

    returns ⋈ lines on (order, item)
    lines ⋈ orders on order
    returns ⋈ orders on order

Walked returns first, returns meets orders on `order` before lines
arrives. The join preserves orders, so an order with no return is a row
with a padded `item`. Lines then pairs on (`returns.item`, `order`) and
finds no partner for that row, so the order's lines come back as a second,
unmatched row (`test_derived_key_domain`: bob gains a row with no item).
TPC-DS q40 is worse: there returns met items INNER and then sales INNER, so
every sale without a return was dropped.

The third pairing says nothing the other two do not. Every return has its
sale (lines binds both keys completely, returns some of them as `~`), and
the sale pairs with the order on the same key. It is a *transit*, implied
through an anchor (`JoinFacts.transits`), and the walk now uses it only
once an anchor is in the tree (`JoinFacts.walked_pair`). Before that,
returns meets orders only through lines. A key either side can NULL is
no transit: a NULL has no anchor partner. The value-NULL `product_id`
group a filtered count pairs with null-safely
(`test_a_where_read_off_a_filtered_aggregate_misses_the_rejected_members`)
is the case that showed it, when a first cut dropped the pairing from the
plan instead.

That first cut also moved TPC-H q09: supplier and part hung off partsupp
(lineitem's anchor) instead of lineitem, and nation turned LEFT. Waiting
for the anchor keeps every pairing the plan states and only gates when it
is walked, so the stated walk is unchanged. The six witnesses are pinned
under every walk (`test_completion_keeps_its_padding_beside_a_domain_on_its_span`,
`test_returns_meet_the_items_only_through_their_sales`, and the q40, q75
and q80 benchmark rows under `WALK_FLIP=shape`).

**Holder count is the key's shape (part 121).** With the transit gated, a
reversed holder count moves no rows in the suite. `KeyShape` now ends with
it (solid, held span, most-held, holders), so `shape_flipped` reverses it
with the rest, and `HOLDER_FLIPS` is gone. The order is the one
`_stated_order` already read, so no plan moves. The walk reads one row term
of a key, authored, which a left-deep tree needs (part 111's 4-cycle). The
rest is stitch, transit and shape.

Gates, against `54571d9`:

- Corpus (205 statements): 0 moved.
- Suite (`-n 4`, without adventureworks/clickhouse/bigquery/cloud_live):
  the same 25 credential-gated failures as `main`.
- Whole suite under `WALK_FLIP=shape,name`, which now reverses the holder
  count too: the same 102 failures as `main`'s flip, plus one shape pin. `test_guarded_full_join_is_refused_not_dropped`
  is still refused, but its FULLs now key off two concepts before
  the `~` region's padding is reached, the reason part 115 saw under the
  held-constant count.
- Fuzzer: 522/522.

**Open.** No stage-3 decision reads a built node. What §6 designs and is
not built yet:

1. The MergeNode is given its keys pair by pair, but resolve still orders the
   joins; `node_joins` are not built from the pairings. A pair with an
   unattributed source now raises (part 37). A ROOT's rejoin onto the
   sources its WHERE re-plans and a key lookup are planned too (part 61),
   and so are an unnest's element filter and a clause applied outside a
   group graph (part 62): every merge between stated rows carries a plan.
   Resolve reads that plan pair by pair, its pivots included (part 63), so
   no key it equates is one it found by column overlap, and it searches
   for no key outside its pivots (part 64). A ROOT's source plan is planned
   too: a bridge pairs where the search joined its scans, and each other
   assembly on the join it states (part 65). A bridge through a
   derived connector the search chose is planned as well, with the connector
   also pairing on its resolved grain (part 66). So is a side the search did
   not choose, `gen_select_merge_node`'s scans, a recursive walk's
   join-back and a relation rollup, so every merge between scans carries a plan, and one reaching
   join inference without one raises (part 67). Resolve takes every key and
   pivot from the plan's pairs, with no column-overlap search left (part 68),
   and reads no join graph: its inputs are the plan's pairs and each side's
   facts (part 69). It orders the joins by walking the pairings, not key
   by key (part 70). What is left is the rank it walks them in: still a
   resolve heuristic over each pairing's keys (`_pivot_rank`), not an order
   the plan states. Of its terms, the key's spelling and the plan's holder
   count decide almost every walk. The region terms decide a few, and only
   the solid term reads a fact the plan does not state: value-null
   provenance (part 71), and of that, nearly only whether a region-free
   holder can NULL the key. Ties below the rank (`_score_join_candidate`)
   read the built sides, the side's name deciding the most (part 72), and no
   plan or contract fact stands in for any of them. They pick only the next
   side: which of two lefts is kept follows the walk (part 73). The order
   decided rows in one shape, a side preserved past an INNER among the sides
   walked before it. The walk now seeds that side instead, so the nullable
   term no longer decides rows in the suite (part 74), and the walk no longer
   reads it (part 75). Of the three built-side terms left, grain size and
   hosting decide only shape, and the shape they pick is the better one. The
   partial term decided rows through subset narrowing that worked in one
   direction only. Narrowing now works in both directions, and two
   projections of one `~` binding pair as complete (part 76). Whether a
   merge built twice renders no longer depends on the walk either: an
   outer join folds into its mirror (part 77). So no term decides rows or
   rendering in the suite, and with the partial term dropped only shape pins
   fail. A complete right beside a `~` partner narrows whichever side is
   walked first (part 78). Those row checks compared two models under one
   walk, though. Comparing the two walks over conforming data found a `~`
   side read as the WHERE's population, which lost rows on `main` (part 79).
   A complete anchor on every stream row now makes its join INNER (part 80).
   The partial term still decides plan quality: dropping it adds 41 CTEs in
   the dimension-chain oracle and 8 FULLs in summary cover. In relation
   rollup the two walks now differ by one join, in `merged` × `value`
   only: one INNER on the kept walk against a LEFT OUTER on the other. That
   INNER had come from an unsound optimizer proof, an earlier INNER forcing
   a later join's side (part 81). The proof's fix exposed a padded NULL
   paired null-safe across sides that had passed different filters, which
   `main`'s unsound INNER had hidden (part 82). The spelling tie-breaks
   decided rows too. With each spelling reversed, four planner bugs
   surfaced, and each is fixed (parts 84-87). The flipped suite now differs
   only in shape, apart from one test whose data breaks a declared EQUAL
   merge. So the plan can state those ties without moving rows. It now
   does (part 88). A star states its hub first. A group-graph plan states
   its suppliers first, then row streams before the lookups they pair with,
   then finer rows first. Stating the order, and reversing it, found three
   more order bugs: an authored axis's FULL widened a lookup hanging off one
   family (part 89), a FULL written from the other side did not fold into
   its mirror (part 90), and an INNER read one key's two spellings as two
   keys (part 91).
   Scan plans now state an order too (parts 92-93): rows before the
   lookups they pair with, a complete binding before a `~` one, finer
   grain first. With every ordered plan stating finer rows first, the
   scorer's resolved-grain term is gone (part 94). The new order found a
   recursive walk carrying an edge it had rebound (part 95), a pushdown
   into the padded FROM base of a RIGHT join (part 96), and a subset
   narrowing that counted a fully partnered right as dropped (part 97).
   A row stream now leads the lookups its pairings identify even as a
   bridge (part 98), and the plan's order outranks hosting, which breaks
   only ties the plan leaves (part 99). Two `subset join` members walked
   before their anchor now narrow onto it (part 100), the plan states a
   declared member after its anchor (part 101), the walk reads no anchor
   and no several-`~` bump (part 102), and a superset of a narrowing's
   sup side no longer stands in for it (part 103). A scan plan now states
   a `~` binding after its anchor (part 104), and the walk reads no
   binding a scan plan orders (part 105). No plan can state the rest of
   the `~` bindings, which are the built sides': resolve states them
   once per merge, over the plan's order, for every plan (part 106), and
   the scorer reads no binding (part 107). A merge forcing LEFT states
   the side its rows are, which seeds the walk (part 108). The walk now
   reads one order and ranks nothing itself: the key rank and the scorer
   are gone (part 109), and resolve states the order once per merge, what
   each key's holders' rows are above the plan's shape, hosting included
   (part 110). What is left is in that statement. Five of its terms
   decide rows, each with a named witness (part 111): authored, solid,
   held span, most other held spans, and holder count, which had been
   read as shape. The authored one belongs to a left-deep tree; the other
   four are typing that holds in one direction only. Holder count read a
   pairing the plan stated through a side holding no anchor (part 112),
   held span a filtered total typed INNER against the rows it rolls up
   (part 113), and solid a coalesced null-safe key reaching padding (part
   114); each now holds in both directions. Most other held spans is the
   tree's, like authored (part 115), and the walk now waits for it as a
   stitch (part 116). It, solid and held span are the key's shape, which
   the flip reverses (part 117). Reversing them found one pair-dedup bug
   (part 118). Holder count still decides rows in six suite witnesses,
   so it stays a row term beside authored (part 119). All six were a `~`
   side meeting a coarser one before its anchor. The walk now meets them
   only through the anchor (part 120), and the holder count is the key's
   shape, leaving authored as the one row term (part 121). Ties the plan's
   terms leave are still broken by the position the plan lists them in
   (cover order, scan name), and stating them as ties costs plan quality
   (part 99).
2. The contract's sets are named, and each reader reads the ones it needs
   (parts 34-36). The merge plan reads them per edge (part 37), each edge
   holds its own side of a statement relation (part 38), and each edge is
   asked only for what its parent can carry (part 39). `projection_grain` and
   `sourcing_keys` are the consumer's, not copied onto every edge (part 40).
   A per-group parent's shape is the stage-2 `shapes_rows` (part 41), and
   so is a FINAL contributor's (part 42), and a shaped bridge's, elected
   before the build (part 43) and joined as recorded (part 44). What is left
   of which parent takes the projection: a ROOT's datasource grouping is
   read off the mark its source plan sets (part 45). Stage 2 does not choose
   datasources, so it cannot state that one. The shapes a condition carries
   into a scan are not in `shapes_rows` either (45 per-group reads in
   part 41, none moving a plan).
3. Every group's rehome, a window fold included, is one row-provider pick
   over the graph stage 2 built, and no pick reads another (parts 46-49).
   A pick is elected only as the rewrite it makes (part 50). A projection's
   grain is stated before any pick (part 51), and so is its edge onto its
   grouping provider (part 52), so a rewrite never changes a grain and the
   election never places a projection. Every window fold is made before
   the election too (part 53), so no rewrite deletes a group, and so is
   every dimension read (part 54), so no rewrite drops a parent or mints a
   group. So is every full-coverage sibling (part 55), and every partial
   spine is a read elected with them (part 56): there is no rehome election
   left. The stated grain, placement, folds and reads still follow the demand
   pass, since the grouping provider, the sibling and the spine are read off
   the groups' input and output sets. Those are now one stated `ConceptSets`
   value that the rehome reads, not group attributes it reads back (part 57),
   and condition injection reads the same value (part 58): the sets are
   written onto the groups once, by the pass after injection. Stating the
   rehome when stage 2 groups needs those sets first. The demand pass still
   runs twice, before the rehome and after injection, since injection adds
   the condition inputs a group must emit, but both runs read one `Demand`
   (part 59): they differ only in the graph they read and the statement
   before them. Every other stage-2 step reads that `Demand` too, and each
   statement fact in it is derived once (part 60): what is left to state
   before the rehome is what a pass reads off the group graph.

## 8. Not claimed

- Not that the concept graph or the grouping rules are wrong. The grouping by
  `(depth, grain)` is sound; the plan reads it.
- Not that `strategy_builder.py` shrinks to nothing. Join typing, nullability,
  extent hosting, condition placement and the generators stay.
- Not that stage 3 never widens a ROOT. It does, but because a pairing asked
  for a key, not because a sibling's grain happened to contain one.
