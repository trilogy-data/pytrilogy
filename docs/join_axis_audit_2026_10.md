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
suite run (CENSUS_SCOPE) built every group graph twice, with `main`'s
`group_graph` and with this one. It compared the results by value: nodes,
edges and their kinds, and every group attribute. CENSUS_RESULT
Within `tests/core/processing`, `main` compared against itself differs in
none of 512 passes, so the comparison is stable. A control that skips
`main`'s reads differs in 21 of them, so it can see a change.

`test_no_group_holds_its_sets_while_the_rehome_elects` pins the order over
`test_v4_window_input_carry`'s model, which elects reads: no group holds
sets while the folds or the reads are elected, and both read one
`ConceptSets`. It fails with the sets written before the rehome.
`test_the_stated_sets_are_written_once_the_reads_are_applied` and
`test_a_folded_group_takes_no_sets` pin the write.

**Open.** No stage-3 decision reads a built node. What §6 designs and is
not built yet:

1. The MergeNode is given its keys pair by pair, but resolve still orders the
   joins; `node_joins` are not built from the pairings. A pair with an
   unattributed source now raises (part 37).
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
   value that the rehome reads, not group attributes it reads back (part 57).
   Stating the rehome when stage 2 groups needs those sets first. Condition
   injection still reads the first pass's sets off the buckets, and the full
   pass then runs again after injection.

## 8. Not claimed

- Not that the concept graph or the grouping rules are wrong. The grouping by
  `(depth, grain)` is sound; the plan reads it.
- Not that `strategy_builder.py` shrinks to nothing. Join typing, nullability,
  extent hosting, condition placement and the generators stay.
- Not that stage 3 never widens a ROOT. It does, but because a pairing asked
  for a key, not because a sibling's grain happened to contain one.
