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

**Open.** No stage-3 decision reads a built node. What §6 designs and is
not built yet:

1. The MergeNode is given its keys pair by pair, but resolve still orders the
   joins; `node_joins` are not built from the pairings. The merge-wide
   fallback for a pair with an unattributed parent remains, but no suite
   merge reaches it (part 32).
2. `GroupInputContract.preserve_keys` is still the union of five sets.
3. The regraft functions still rewrite the group graph instead of picking a
   provider.

## 8. Not claimed

- Not that the concept graph or the grouping rules are wrong. The grouping by
  `(depth, grain)` is sound; the plan reads it.
- Not that `strategy_builder.py` shrinks to nothing. Join typing, nullability,
  extent hosting, condition placement and the generators stay.
- Not that stage 3 never widens a ROOT. It does, but because a pairing asked
  for a key, not because a sibling's grain happened to contain one.
