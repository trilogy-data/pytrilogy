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

## 7. Status (PR #773) and what is left

**Landed.** `v4_helper/join_plan.py` computes a `JoinPlan` from stage-2 facts
twice per multi-contributor FINAL: over the elected cover (ROOT carry demands,
merge grain, widening) and over the sides the merge actually joins (after
folds, bridges, filter-arg producers). The FINAL MergeNode carries the second
plan's `permitted_keys`, and `get_node_joins` drops every shared column off
it before ordering and typing joins. Join typing, padding and hosting stay at
resolve.

The contract, as the census plugin checks it: a pairing's *keys* are decisive
(what widening carries onto that side); its *implied* columns are what the
keys determine, which resolve may also equate (a side's physical grain is
never pruned). Resolve may prune a planned key; it may never pair off the
plan, nor go keyless where the plan pairs.

Retired: the stage-2 per-group `FinalContributorContract`, `_final_merge_grain`,
`_group_final_grain_contribution`, the ROOT `sibling_grain` block and
`_relevant_root_preserve_keys`, and merge-grain-wide widening (each side now
carries only its own pairings' keys, `JoinPlan.keys_for`).

Rules the plan states that §6 did not name, each from a suite shape:

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

Stage 3 now builds what stage 2 declares in three places: a non-grouping
contributor sourced finer than its declared identity is projected to it when
an independent sibling exposes the finer row key (`_at_declared_identity`); a
rename-only contributor renders on the group it reads; widening is per side.

**Open.**

1. Cover election still runs in stage 3 off built nodes
   (`_cover_groups_for_mandatory` and the add/fold passes); the second plan
   exists because folds and bridges change the sides after it. Moving the
   cover to stage 2 makes one plan enough.
2. The bridge and island family (`_bridge_unpaired_parents`, `_island_bridges`,
   `_key_bridged_members`, `_raise_if_rowset_islanded`) still decides
   connectivity from built outputs. It is shared with per-group merges, so it
   goes when per-group merges take a `JoinPlan` (their `GroupInputContract.
   preserve_keys` is the same union-of-sets shape the FINAL contract was).
3. The gated global aggregate (`avg(x) by * ? yr = 1999`) is grouped at the
   statement grain and filtered per row; the plan matches it only because the
   rename now renders on the filter group. Its declared grain should be a
   single row.

## 8. Not claimed

- Not that the concept graph or the grouping rules are wrong. The grouping by
  `(depth, grain)` is sound; the plan reads it.
- Not that `strategy_builder.py` shrinks to nothing. Join typing, nullability,
  extent hosting, condition placement and the generators stay.
- Not that stage 3 never widens a ROOT. It does, but because a pairing asked
  for a key, not because a sibling's grain happened to contain one.
