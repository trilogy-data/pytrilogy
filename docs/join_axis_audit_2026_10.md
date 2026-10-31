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
  carry demand. A merge assembled below the consumer (no group behind a side)
  still discovers its keys at resolve.

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

**Open.**

1. The FINAL merge re-decides, over `built`, what its plan already elected:
   `_filter_arg_parents(…, built, …)` picks each filter-only argument's
   producer and `_gate_key_row_stream(…, built)` the row stream for a gate
   key, the same choices `_final_extra_sides` made over the planned groups.
   The `unplanned` check after them only validates the agreement. Join the
   plan's extras instead (and `_apply_final_conditions`, on the sole
   contributor path, the same way).
2. `grouping_sibling` reads `node_nulls_grouping_keys` off each built
   contributor; a grouping-sets group is a stage-2 fact.
3. With no `per_group`, the FINAL takes `next(iter(built.values()))`: the
   first group built, where the planned groups name it.
4. A non-FINAL consumer's bridge candidates (`_bridge_candidates(…,
   ctx.unbuilt, gid)`) differ from the planned ones only where the plan
   raises (above).
5. The fold widen still projects the aggregate's merge after
   `_pre_merge_parents` assembles it, inside the aggregate's build. No
   decision reads the built node; asking a ROOT for the derived column
   instead changes source selection.

The rest of `_assemble_final_node`'s reads of `built` apply decisions: the
passthroughs and projections, each contributor's node, bridge nodes by the
planned id, and the ROOT re-source.

## 8. Not claimed

- Not that the concept graph or the grouping rules are wrong. The grouping by
  `(depth, grain)` is sound; the plan reads it.
- Not that `strategy_builder.py` shrinks to nothing. Join typing, nullability,
  extent hosting, condition placement and the generators stay.
- Not that stage 3 never widens a ROOT. It does, but because a pairing asked
  for a key, not because a sibling's grain happened to contain one.
