# Query-generation simplification audit (2026-10)

Scope: `trilogy/core/processing`, `trilogy/core/optimizations`,
`query_processor.py`, `dialect/base.py`. Five axes were audited in parallel
against `main` c9a96ac66, each with the same method: instrument every
candidate site, count corpus firings (204 statements over `tests/modeling`,
`tests/engine`, `tests/complex`, `tests/discovery`), ablate, diff the rendered
SQL, run the pinning test files, and run the fuzzer. A site landed only when
the ablation was byte-identical or every moved statement was row-verified,
the pins stayed green and the fuzzer stayed 260/260.

Two invariants framed the audit:

1. The engine trusts the model. A declared fact (`~`, `?`, grain, `merge`,
   `complete where`) is true; code that re-verifies one from physical
   evidence or stays conservative in case the declaration lies is a
   deletion candidate.
2. A rowset is an opaque row source with a declared grain. A rowset branch
   that restates what a ROOT scan or an AGGREGATE barrier already gets from
   the generic rule is a deletion candidate.

## What landed

| axis | files | lines | corpus effect |
|---|---|---|---|
| domain graph consumers | 9 | +105 / -312 | byte-identical |
| nullability | 6 | +58 / -63 | 6 tpc_ds statements smaller (5 fewer `is not distinct from`, 2 fewer `coalesce(count, 0)`), rows verified |
| partial bindings and region domains | 8 | +15 / -241 | byte-identical |
| rowset discovery | 2 | +41 / -231 | q54 INNER JOIN operand order, rows identical |
| rowset assembly | 5 | +34 / -262 | byte-identical |
| follow-up review | 12 | +172 / -231 | byte-identical (206 statements incl. tpc_ds aggregates) |

Per-axis detail:

**Domain graph.** The PROPERTY-beside-its-keys prune in `reduce_concept_pairs`
was a special case of the FD-closure prune (every one of its 16 corpus firings
was also FD-pruned). `_concept_covers_grain` and the two
`grain_satisfied_by_pregrain` fast paths were implied by the coverage test
after them. The `_relative_key_subset` proof family in
`value_set_join_upgrade` never fired; with it went the `subset_join_map` and
`scoped_canonical` parameters threaded through four functions. Two
model-distrust guards are gone: `_authoritative_scan` evidence no longer
requires an EQUAL declaration (a binding without `~` declares the full
domain), and a resolved EQUAL at a rowset boundary is accepted whether
declared or structural (`DomainGraph.declared_equal` deleted; a new join
matrix cell pins the self-filtered rowset anchor). `scoped_join_key_groups` is
a property over `domain_graph.join_key_groups()` instead of a copied field.

**Nullability.** `find_nullable_concepts` marked a join's right side nullable
whenever the join was keyed on a nullable column; only outer-join padding
introduces NULLs, so that was a pure over-approximation. Intrinsic and
argument-propagated nullability are now stamped once at the producer
(`with_propagated_nulls` at the four resolve sites) and `side_nullable` is a
single stamp lookup. Of the five NULL provenances (`value`, `extent`, `guest`,
`span`, `rollup`), only `extent` ever decides a join type on the corpus, but
each of the others has engine row pins and none is deletable.

**Partial bindings.** `membership_complete_grain_keys`,
`SelectNode.complete_proofs` and the `exempt` argument of `scan_stamps` were
superseded by `partial_bridging.decide_heal` (0 corpus, 0 pin firings). The
`deep_extent_free_spans` subtraction in `MergeNode._join_proofs` was dead. The
rowset-content term in `get_node_joins`' host test restated the boundary's own
stamp. The `solid_groups` subtraction in `elect_extent_owners` restated what
`feed_region_domains_to_present_scalars` already decides. The three
`scan_partial_addresses` readers agree on every shared key (27 of 27).

**Rowset discovery.** `_relation_crosses_rowset_boundary` tests the row-shape
barrier derivations instead of rowset lineage. `_scoped_group_sides` keys by
namespace. The ancestor-rowset clause of `_constraint_crosses_rowset` was
subsumed by the boundary clause. The licensed-only grain override in
`_add_rowset_handle_relations`, the collapsed-origin rowset skip, the
`keys`/select-grain lookup in `_lineage_pinned_grain`, the rowset exclusion
and authored-relation pin in `_final_merge_grain`,
`_final_gate_rowset_base_keys`, `gate_pair_keys`, the FINAL-deferred rowset
filter exposure and the rowset carried-spans capability are deleted.

**Rowset assembly.** Three generic facts cover almost every rowset branch in
assembly: a handle is a leaf whose value exists only on its own boundary; a
boundary is a row source with its own grain; a rowset-side scoped-join member
is partial by relation, not by binding. Sites that only restated one of them
are gone: `_relation_licenses_handle` and the mangled body-local guards in
`_carry_join_keys` and the FINAL preserve-key carry (`_synthesizes_handle`
already refuses to compute a handle a scan does not carry),
`_rowset_base_join_keys` and the FINAL gate widening on rowset base keys,
`_drop_unadvertised_rowset_handles`, the ROWSET-only filters on completion
contributors and ROOT preserve keys, the region-domain RowsetNode skip, the
ROWSET exemption from `satisfiable_outputs`, the ROWSET host deferral under
a global merge and the ROOT-mate-via-keys lookup in condition placement, the
body-WHERE exemption from the disconnected-gate error, the `MergeNode`
rowset/TVF `force_group` carve-out, the "ROWSET stamps are not genuine" test
in `_genuine_partial_stamp`, and the handle-content term in `_extent_free`.

**Follow-up review.** Argument-null propagation is folded once in
`QueryDatasource._fold_inferred_nullables`, beside intrinsic nullability,
instead of at four node resolve sites. `partition_rowsets`,
`partition_constants` and `partition_unions` share one scope-keyed rule
(`_partition_by_scope`; seam 9 below, first half). `_upstream_concepts` no
longer splices a rowset's `derived_concepts` in: a handle is a leaf.
`output_rowset_grain_keys` lost its last callers in the assembly axis.

**Bugs the veto probe found** (all present on `main`; pinned by
`test_outer_padding_null_not_a_value.py::test_address_only_rows_stay_their_own`,
corpus byte-identical). Hand-built variants of the q30 shape turned up three
defects in how an address-only region (an address no customer has) is read:

- `_pairs_region_padding` read every join's padding, including the padding
  the current join itself adds, so two FULL-joined value-NULL aggregates
  paired null-safely with the padded addresses (`(2, NULL, 15, 50)`). It now
  reads only the joins before the one it types.
- A region's `reach` included spans absent on its rows: the address-only
  region listed `cust_sk`, so a `cust_sk` output counted as carried there
  and a WHERE on `state` manufactured `(NULL, NULL)` rows. `Region.own_reach`
  is the reach of the spans present on the rows (or valued by a coalescing
  relation); a region is demanded through an output only when its rows hold
  one (`Keyspace.carries_output`).
- `evaluated_over_region` let an aggregate grouped by an absent span read the
  region, so `count(ret_id) by cust_sk` folded the address-only rows into the
  unknown-customer group. It now asks whether the grouping key is HELD there
  (`Keyspace.held_on`); `reach` keeps grouping a region's rows under the
  absent span's NULL for aggregates that do count it (`count(addr_sk) by
  cust_sk`).

## Not deletable (corpus-silent, unit-pinned)

Check this register before calling anything here dead. Each entry fired in
zero corpus statements or was corpus-identical under ablation, and a named
pin caught the ablation.

Domain graph:
- `_complete_values` passthrough recursion: a 1:1 rename CTE carries its
  parent's completeness. `tests/test_scoped_derived_rowset_join_matrix.py::test_composite_mixed_key_subset_join_should_not_widen`.
- `_complete_values` BASIC/ROWSET lineage transfer: a derived key's domain is
  the image of its inputs. `tests/join_matrix/test_composite_matrix.py::test_composite_key_join[plain_derived-subset]`.
- `scoped_partial_derived`: a `subset join f(x) = y` has no column binding to
  carry `~`; this set is the only carrier. 7 join-matrix cells
  (`test_two_source_single_key[derived-subset-*]`, `test_three_way_derived_key[subset-*]`).
- Stamp heuristics beside `_proven_subset_of`: a `~` binding is a population
  fact on ONE address, which a two-concept domain edge cannot express
  (graph proves 10 of 69 evidence hits, stamps 59).

Nullability:
- `get_modifiers` asymmetric-provenance veto: zero firings across the
  corpus, fuzzer and suites once q30's own plan stopped reaching it, yet a
  padded address beside an unknown-customer NULL group pairs without it
  (`test_outer_padding_null_not_a_value.py::test_padded_key_does_not_pair_with_a_value_null_group`,
  4 cells). The clearest case of a corpus gap reading as dead code.
- `guest_padded`: `tests/engine/test_duckdb_rowset_null_group_rejoin.py`.
- `rollup_padded` and the `rollup_padding` join rule: six engine tests in
  `test_duckdb.py`, `test_duckdb_fuzzer_regressions.py`,
  `test_duckdb_rollup_rowset_binding.py`. Re-ablated behind the TVF (seam 10,
  seventh slice): the rule and `_null_alike`'s term still hold a pin each;
  the keyless guard's exclusion is gone.
- `span_padding` modifier path: four engine tests. Its type-rule branch in
  `_nullable_join` is reached by its unit pin alone (instrumented over 3,044
  engine, join matrix and processing tests): a side that span-pads a key
  normally carries it partial and routes to `_partial_domain_join` first.
  Kept until that is proven, not merely unobserved.
- `_pairs_region_padding`: `tests/engine/test_padded_null_pairing.py`.
- `value_nullables` as a fact distinct from `nullables`: 26 engine tests flip
  LEFT/RIGHT to FULL without it.
- `extent_nullables`: the only provenance that decides on the corpus (q51,
  q98, tpc_h adhoc07). `test_multi_fact_nullable_fk_extent.py`.
- `nulls_are_values` "no carriers" branch: reachable (127 calls, 20
  statements) and correct; it is the self-computed-derivation case, not
  conservatism. The comment was the defect.

Partial bindings and region domains:
- Resolve-time partial stamp recompute in `SelectNode`: covers scans widened
  after construction (2 plans move without it). DESIGN: re-stamp on output
  widening would make the construction stamp authoritative.
- `partial_binding_sources` rowset fallback: `test_duckdb_rowset_null_group_rejoin.py::test_unsold_item_counts_no_lines`.
- `_extent_free_join` both-sides branch and `_partial_domain_join` host
  branch: 8 plans move, 2 row failures each.
- `_partial_domain_join` fact-to-dimension grain anchor: the clearest case of
  row intent re-derived from physical grain rather than the stamp, but
  load-bearing for an undemanded `~` key
  (`tests/engine/test_duckdb_partial_key_assembly.py::test_forked_full_column_set`, tpc_h adhoc03).
- `absence_atoms`: `tests/generators/test_condition_routing.py`; an `x is
  null` atom over a `~` scan's own column must stay a merge-level test.
- `split_carried_only_row_streams`, `detach_final_span_domain_producers`,
  `carry_region_spans` condition-scan loop, `Keyspace.unread_spans`,
  null-member domains: 8, 21, 1 (`FULL JOIN on 1=1`), 5 and 5 row failures
  respectively; pins in `test_derived_key_domain*.py`,
  `test_padded_null_pairing.py`, `test_unmodelled_regions.py`,
  `test_rowset_unread_region.py`, `test_null_member_domain_filtered.py`.
- `DomainKind.BOUNDARY`: a rowset boundary already padded the region in its
  body; building an OWN domain beside it pads twice (15 row pins). The other
  non-OWN kinds are trace-only labels (DESIGN below).

Rowset discovery:
- `partition_rowsets` (one bucket per rowset, keyed by scope not label):
  6 corpus statements break outright under the default rule; 11 pins under
  label keying.
- The presence-probe and multiselect-output `rowset_name` tags: 21 and 1
  pins (`test_duckdb_rowset.py::test_tvf_union_order_by_grouped_away_column`).
- `_constraint_crosses_rowset` boundary clause and ROWSET as a D0 barrier:
  q29 replans, q64 cycles or doubles its CTE count, q77 errors.
- `_add_rowset_handle_relations` RELATION edges: 9 pins.
- `_rowset_relation_keys`, `_arg_rowset_populations`, the scan-advertisable
  rowset exclusion, the `_walk_aggregate_grain_inputs` ROWSET leaf, the
  scoped grain-identity cluster (`_rowset_row_identity`,
  `_rowset_local_grain_identity`, `_aggregate_authored_grain`): 1 to 5 pins
  each; q64 goes from 8 to 16 CTEs without the grain-identity cluster.
- `RowsetWitness` sources in the keyspace: 24 pins, 0 corpus moves.
- `island_rowsets_for_connectivity`: q29 reports disconnected without it.
- `_unsourced_relation_mates` SUBSET-anchor request: 0 corpus firings, but a
  demanded raw `~` member stays partial without its anchor rowset
  (`test_filtered_rowset_anchor.py::test_filtered_rowset_anchor_subset_join_narrows`).
- `join_plan.row_identity` returning the merge axes for a ROWSET (was
  `_group_final_grain_contribution`): q44, q46 and q68 gain a redundant
  `GROUP BY` without it.

Rowset assembly:
- Two rowset boundaries flowing straight to FINAL defer their WHERE to FINAL
  (`rowset_final_groups`): a derived-key union join leaks next-year-only rows
  (`test_independent_rowset_matrix.py::test_union_join_derived_key_filtered_on_derived_side`).
- A handle is computable only on its own boundary: 3 row pins. Was
  `_synthesizes_handle`, an after-the-fact refusal; now the leaf rule and
  `projection.renders_at` (seam 3).
- The passthrough fold veto for a rowset-bearing side, the partial stamps on
  keys carried onto a rowset-bearing parent and on a licensed handle over a
  coalescing scan, rowset islanding and the bridge-rowset diagnosis, the
  boundary-handles-first ordering, and the region-domain dedup over a
  boundary: 1 to 13 red pins each.
- Condition placement: the ROWSET host deferral with a non-FINAL consumer,
  own-side keys by rowset identity, and the active-relation survival and
  FINAL routing of ROWSET hosts: q77 moves and 2 to 28 pins go red.
- Optimizer: the unbound-rowset-rename, subset-anchor-boundary and
  aggregate-arg re-exposure fold vetoes in `collapse_single_parent`; the
  rowset-definition-boundary completeness and lineage transfer in
  `value_set_join_upgrade`: 1 to 20 row pins each.
- `resolve_rowset` obligations: subset-source and owned-span partial stamps,
  presence-probe and derived outer-relation member materialization,
  undemanded pseudonym handles, the multiselect align grain, coalesced
  content re-exposure and nullability through handle content: 1 to 54 row
  pins each; dropping the pseudonym handles also raises 5 keyless-join
  errors.
- `query_processor`: ORDER BY on a non-projected rowset measure carried as
  hidden grain (1 pin).

## Design seams (not built unless marked landed)

Ranked by lines a seam would free, across the axes. Status as of seam 2's
ninth slice: 3, 4, 6 and 7 landed (6 smaller than estimated, its planner half
folded into 2), 5 landed its ownership half, 2 landed its plan-time EQUAL,
`~`-subset, graph-subset, twin, unnested-element, recursion-walk, per-key
subset and rule-B proofs, decided NULL membership and retired its first
optimizer helper, `_unnested_parent`, and the recursion-seed branch (the
optimizer rule's firings over the non-law suite: 912 before the first slice,
796 after it, 522 after the second, 407 after the third, 377 after the
fourth, 299 after the fifth, most of that drop refusals of unsound
narrowings, 256 after the sixth, measured from 324 once the null-group
matrix joined the suite, 101 after the seventh, from 255, 53 after the
eighth, 46 after the ninth and 35 after the tenth, measured from 40; the eleventh deleted the nine proof branches of the rule the planner already owns, moving no firing; over the whole suite, laws included, its CTE
rewrites fell from 1,045 to 496 in the fourth) and lost its per-side origin premise, 8 closed,
9 half done, 10 landed every keyed read (rowset, subquery, statement), the group-owned grouping sets below the planner, a pure arm (step (a)), a standard arm whose pass is the select's, with the per-aggregate mode deleted (step (b)), and the reader ablation (step (c): 14 of 28 sites deleted, the rest pinned to a row or plan-size test); 11 landed its first slice, 12 landed (the zero-filled COUNT owned by its source) and 13 landed (one owner for a coalesced key read), 14 landed (the planner narrows on its own WHERE: `UpgradeJoinOnGuards` flips 1,539 to 1,080), 15 landed its first slice (the planner narrows on its consumers' filters: 1,059 to 608); 1 and the rest of 2 remain, and
hold nearly all of the estimated lines. Seam 1 was re-measured: its blocker is the ranked owner
election, not the padded fallback.

1. **Extension rows as a first-class row source** (about 2,100 lines:
   `region_domains.py`, `extent_ownership.py`, `region_reads.py`, the
   `SpanScope` carriers, `_extent_free_*` stamps and joins). The keyspace
   already names each region; a region with rows of its own as a ROOT-like
   leaf would collapse the owner election, the extent-free suppression and
   the held-span join contract into "a leaf joins on its key, preserving the
   leaf". Blocked by the padded-plan fallback every region row pin protects.

   Re-measured (`local_scripts/sql_ab/domain_kind_plugin.py`, non-law suite,
   14,400 tests) the premise is half right:
   - **The padded fallback is small.** Of 1,927 region-domain decisions,
     1,681 (907 tests) are already OWN: the region IS a ROOT-like leaf.
     PADDED is 89 decisions in 55 tests, all targeted pins: a ROLLUP key (19
     tests), a MATERIALIZED aggregate over the region (5), and a gate no host
     filters the region's rows by (31).
   - **The ranked election is the blocker.** Of 1,743 extent elections, 1,440
     elect only domains; 303 (208 tests) still rank an owner. They serve
     BOUNDARY (45 tests), ROW_STREAM (22), RELATION (5) and PADDED (53)
     regions, and in 70 tests a base-region completion (`live_completes`)
     with no domain at all. Deleting the election needs each of those to
     name its own leaf first, the completions included.
   - **Nothing in the plumbing is dead.** Branch coverage over the whole
     suite (48,200 tests, laws included) leaves 2 of 243 statements in
     `region_domains.py`, 0 of 82 in `extent_ownership.py` and 2 of 60 in
     `region_reads.py` unreached; dropping the extent-free carried spans
     alone fails 6 region row pins. The seam is a redesign, not a harvest.
2. **Plan-time narrowing (first half landed; no per-side origin node
   needed).** The estimate was about 520 lines of `value_set_join_upgrade.py`
   plus `scoped_partial_derived` and the `JoinScope` registries, gated on a
   per-side origin node for the same-address arbitrations the planner could
   not decide. A firing census of `UpgradeOuterFromKeySetEquivalence` over
   the whole suite (912 firings in the 14,400 non-law tests, 1,478 in the
   33,800 law tests) re-measured that premise.
   - **The origin arbitration was dead.** `_side_origins` (a `getattr` walk of
     a side's CTE chain for `origin_address` stamps) took part in one firing
     across the whole suite (`test_double_partial_key_join_order`), and that
     firing's SQL is identical without it: another pair decides it. It is
     deleted. A same-address pair now narrows only on a genuine one-sided `~`
     stamp (`_genuine_partial_stamp`). So a per-side origin node is not what
     blocks seam 2.
   - **The planner proves EQUAL completeness (seam 6's leftover).** An EQUAL
     key (`merge a into b`, `merge equal`, `equal join`) between two sides
     that each carry its whole domain (`SideFacts.complete_equal`:
     `complete_key_domain`, through BASIC lineage) is typed like any other
     key, not FULL by the `full_join_keys` rule. The optimizer's EQUAL
     upgrades fell from 332 to 216 firings and its total from 912 to 813.
     The plans narrow where the optimizer never could after the FULL had
     shaped them. Over the suite SQL capture, about 990 FULL JOINs became
     LEFT, RIGHT or INNER and about 100 key coalesces went, across 827 moved
     statements, every one under a row oracle that stayed green (mostly
     `test_relation_rollup_laws`, `tree_reporting` dashboards and fixtures).
     `test_equal_join_plan_time_completeness.py` pins it (red on `main`).

   Not landed:
   - **An unnest of complete lists is not complete at plan time.** Landed
     in the seventh slice, below: the first cut's owner loss was the
     one-binding rule reading an unnest as a projection of its owner's `~`.
   - **`scoped_partial_derived` is not a graph query.** Recomputed from
     `domain_graph.subset_sources()`, the collapsed endpoints and the build
     concepts' derivations, it disagreed with the Factory's set in 80 suite
     tests in both directions (rowset outputs, unnest endpoints, namespaced
     merges). It carries author-time facts the build environment no longer
     holds, so it stays until the Factory hands those over.
   - **The rest of the optimizer rule.** What it still proves needs the
     rendered plan: twin groups over one filter chain (`keyset_equiv`), a
     recursion seed, a parent every join keeps (`joined_complete`), and
     conditions the optimizer itself pushed. These remain seam 2's open half.

   **Second slice (landed): the planner proves subset completeness.** A
   firing census (`local_scripts/sql_ab/upgrade_census_plugin.py`, proof
   helpers that held per firing) put 796 firings over the non-law suite in
   four classes: a declared subset into a rowset at its own boundary (193), a
   one-sided `~` stamp beside a complete authoritative scan (about 160), an
   unnest of complete lists (153, left in the optimizer above) and twin key
   sets over one filter chain (88). The first two are knowable while
   planning, and the walk now drops the subset side's preservation itself
   (`join_resolution._narrow_covered`):
   - `JoinFacts.covered` names each (sub, sup, key) where `sub` binds the key
     `~` and never NULL, `sup` binds it completely and carries every value
     of it (`complete_key_domain`, through BASIC lineage, and with
     `rowset_boundaries` a rowset's boundary for its own handles), and the
     domain graph puts sub's spelling within sup's (SUBSET or EQUAL: two
     rowsets subset-joined to one anchor share a canonical key, not a
     subset).
   - A join narrows only when that pair is its whole ON clause (a second
     left's equality can still drop a covered row:
     `test_property_hop_alignment_matrix::test_member_unprojected_cross_side_grain`)
     and the covering side's rows are all still in the stream (`_intact`).
     A FULL-declared key keeps both sides (`test_join_merge_parity`'s
     `yoy_independent_rowsets`): a rowset's handle is complete for itself,
     not for a domain an EQUAL declaration merges it into, so the boundary
     rule stays out of `complete_equal`.
   - `_extent_free_join`'s own "the other side carries the span's whole
     domain" INNER and `SideFacts.complete_spans` are gone: they were this
     proof restricted to extent-free spans.

   Firings fell from 796 to 522 (rowset boundary 193 to 77, `~` stamp about
   160 to 109). What is left of both classes is a multi-left join, a covering
   side an earlier join thinned, or a side whose completeness only the
   optimizer's rewrites show (`joined_complete`). No optimizer helper reached
   0, so none is deleted. `test_subset_join_plan_time_completeness.py` pins
   both shapes with the optimizer rule stubbed (red on `main`).

   Gates, against `ca275e3`: suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`,
   48,494 statements, laws included) moved the join shape of 199 tests
   (FULL -71, RIGHT -40, INNER -14, LEFT +190 across their statements; the
   rest of the textual moves are worktree paths and xdist CTE naming), each
   under a row oracle that stayed green, mostly
   `test_dimension_chain_oracle_laws`, `tree_reporting` dashboards,
   `test_rank_beside_partial_merge` and `test_rule_soundness_laws`. Where a
   narrowed INNER now precedes a RIGHT the walk re-seeds
   (`_preserved_past_inner`) and the join reads LEFT from the preserved
   side. Corpus (209 statements): 2 moved, q72's key read off its dimension
   and tpc-h adhoc07's INNER chain reordered. Fuzzer 522/522; the suite
   fails the same 25 tests as `main`.

   **Third slice (landed): graph-related subsets, and a covered join thins
   nothing.** The 522 firings left after the second slice
   (`local_scripts/sql_ab/upgrade_census_plugin.py`, re-run on `3694067`)
   held 65 rowset-boundary narrowings and about 50 `_proven_subset_of` ones
   the planner missed for three reasons, each now closed:
   - **Subset evidence without a `~`.** `JoinFacts.covered` asked the subset
     side to bind its key `~`. The optimizer also accepts a domain-graph
     relation between two distinct concepts: an unfiltered rowset over its
     anchor (`with b as select id, ...` beside `id`) is EQUAL to it, so the
     anchor's rows all have a partner on the rowset's boundary. A side now
     qualifies by either; one concept bound complete on both sides still
     does not (`test_dim_peel_composite_grain_axis`: the dimension keeps the
     member a `?` fact lacks).
   - **A relation stamp is not a coverage fact.** A `subset join r.k = k`
     stamps `r.k` `~` on every read of it, the rowset boundary included, so
     `complete_key_domain` refused the boundary that carries every value of
     its own handle. A stamp at a declared subset end (`subset_sources`,
     as the optimizer's `_own_coverage_partial` reads it) now defers to the
     read's leaves and joins; a leaf's own `~` and an extent-free boundary
     still refuse.
   - **Both sides, and the stream after.** `_narrow_covered` drops each
     covered side's preservation, not only the first (two sides each
     carrying the other's domain join INNER), and records it on the join
     (`matched_left`/`matched_right`, carried onto `Join`). `_intact`,
     `preserved_sources` and `_narrowed_sources` read the record: an INNER
     the proof typed drops no row of a covered side, so the covering side's
     rows stay intact for the next join and its outputs are not stamped
     partial. Without the last, the INNER beside the relation-stamped
     rowset stamped the customers partial and a later join typed INNER on
     it, losing the customer with no order
     (`test_rowset_declared_join_pairing`).

   Firings fell from 522 to 407: rowset boundary 65 to 17 (8 of them
   `union join` vetoes the planner leaves alone, the rest nullable keys,
   multi-left and multi-key joins), and the joins those narrowings fed.
   No helper reached 0. What remains: the unnest of complete lists (153,
   kept in the optimizer), twin key sets over one filter chain (about 90),
   value-NULL `~?` keys a null-safe pair matches (57, all
   `test_padded_null_pairing`), and multi-left or thinned-stream joins.
   `test_subset_join_plan_time_completeness.py::test_unfiltered_rowsets_beside_their_anchor_are_typed_inner`
   pins the graph case with the optimizer rule stubbed (red on `main`).

   Gates, against `3694067`: suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`,
   laws included, 49,899 tests) moved the distinct join shapes of 63 tests
   (FULL -35, RIGHT -2, LEFT +3, INNER +37; the rest of the textual moves
   are worktree paths and statement counts under xdist), each under a row
   oracle that stayed green: `test_partial_lookup_fallback_oracle_laws`
   (28), `test_rank_beside_partial_merge` (25),
   `test_class_value_beside_partial_lookups`, `test_rowset_declared_join_pairing`,
   `test_unmodelled_regions`, `test_rowset`. The one move with no row oracle,
   gcat `test_full_join_issue_2` (no local data), narrows a payload FULL to
   LEFT because `launch_info` binds `LV_Type: ~vehicle.name` beside the
   complete `lv_info`: every launch keeps its vehicle row, so the vehicles'
   stream carries every launch. Corpus (205 statements): 0 moved. The suite
   fails the same 25 tests as `main` (credential-bound and
   `the_look::test_wrapped_aggregate_with_dependent_by_grain`). Fuzzer
   522/522.

   **Fourth slice (landed): twins.** The optimizer's equivalence upgrade
   (`_pair_key_sets_equivalent`) reads two sides as one key set when each is
   complete on it: twin rollups, two facts each binding a key complete, two
   aggregates by a dimension attribute. `JoinFacts.covered` refused that
   shape (one concept, no `~`, bound complete on both sides), so the planner
   left it FULL or LEFT for the optimizer. Now two reads of one concept that
   each carry every value of it (`_carries_every_value`) cover each other
   (`_twins`) when they NULL it alike (`_null_alike`): on no row, or only as
   a value on both, which the pair already matches null-safely
   (`get_modifiers`). Guest, rollup and span padding are not values: a
   span-padded `pstatus` beside order 100's value-NULL group typed INNER
   loses cat's row
   (`test_derived_key_domain::test_where_beside_a_region_fed_output_aggregate_filters_the_rows`).
   A side that NULLs the key beside one that never does still keeps its
   preservation (`test_dim_peel_composite_grain_axis`).

   Census over the non-law suite: 407 to 377 (twins 82 to 56; the rest are
   twins over a filter chain, which `complete_key_domain` refuses, and
   multi-left or later joins). Over the whole suite, laws included, the
   rule's CTE rewrites fell from 1,045 (645 tests) to 496 (359), and the
   rules that cleaned up after its late INNERs with it:
   `DropIdentityGroup` 220 to 53, `SimplifyNullSafeJoins` 203 to 121,
   `PrunePreservedJoinKeys` 1,280 to 878, `UpgradeJoinOnGuards` 3,704 to
   3,487, `OrderInnerJoinsFirst` 589 to 446. Neither the upgrade nor
   `DropIdentityGroup` fires on any shape of the rule-soundness model now
   (2,000 random queries and the regressions, both addressings), so their
   `test_rule_fires_and_keeps_rows` cells are gone; both still fire under
   other row oracles. `test_subset_join_plan_time_completeness.py::test_twin_reads_of_one_complete_key_are_typed_inner`
   pins a value-NULL twin with the rule stubbed (red on `main`).

   Gates, against `8033411`: suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`,
   laws included) moved the join shape of 437 tests whose statement count
   held (INNER +726, LEFT -710, FULL -42, RIGHT -34: 60 fewer joins), each
   under a row oracle that stayed green, mostly
   `test_global_fallback_oracle_laws` (115), `test_oracle_spellings_laws`
   (55), `test_fixed_aggregate_select_laws` (36),
   `test_oracle_expressions_laws` (33), `test_rank_beside_partial_merge`
   (20); the moved plans are smaller or equal, except below. Corpus (209
   statements): 1 moved, tpc-h q09's INNER chain reordered. The suite fails
   the same 25 tests as `main`. Fuzzer 522/522.

   Open, found by the gates:
   - **Six `test_dimension_chain_oracle_laws` plans grow** (269, 299, 324,
     350, 533, 560: 3 to 5 CTEs, `sum(group(seats) by id)` regroups by
     `(id, seats)` and joins back on `id`). All three sides now join INNER,
     so the walk keeps its stated order and starts from the aircraft; on
     `main` a RIGHT after an INNER re-seeded it at the flight
     (`_preserved_past_inner`), and the plan folded the group. The joined
     pregrain is the same on both, so the regroup is decided elsewhere in
     the group's assembly. `DropIdentityGroup` can drop the GROUP BY with an
     orientation-free walk over INNER joins (prototyped), but not the
     self-join it leaves; the fix belongs where the walk's orientation
     reaches the group, not in the optimizer. Rows are right.
   - **Is NULL a member of a complete domain?** Decided in the fifth
     slice: no.

   **Fifth slice (landed): a NULL group is no value of a complete key's
   domain.** The question above had a row-level answer. With orders holding
   a NULL customer and visits none, `select cust_id, sum(amount),
   sum(pages)` dropped the NULL-customer order: the fourth slice's planner
   twin typed the join INNER null-safe, and so did the optimizer's
   equivalence upgrade on its own. Three shapes lost rows on `main`, each
   now pinned in `test_subset_join_plan_time_completeness.py` (red on
   `main`): the twin above (planner and optimizer), a `~?` subset side
   beside a complete `?` read (`_pair_fully_matches`'s null-safe arm
   narrowed FULL to LEFT, losing the subset side's NULL row), and two facts
   whose undated rows NULL a derived year over one shared date dimension
   (`_pair_key_sets_equivalent`: `year + 1` lost the undated store tickets
   whenever web had none).

   A complete binding says the side carries every value of the key's
   domain. Whether it also holds a NULL group is data, so null-safe equality
   may find that group no partner. The rule, in both stages:
   - Two reads of one key whose NULL groups are those of the same rows
     (`same_rows`: the same leaf tables through the same filters, the twin
     rollups of one table) still pair: one holds the group exactly when the
     other does.
   - A declared relation (`DomainGraph.declared_relation_keys`: `subset` /
     `equal` / `union` joins and merges) speaks for the two keys'
     populations, NULL groups included, which is the join matrix's
     nullable-axis contract. The optimizer keeps its null-safe arms for those
     pairs only, and the planner now proves the same: a declared subset side
     that NULLs its key alike (`_null_alike`) is covered, so the nullable
     matrix cells type at plan time (that subset's firings 132 to 116).
   - Anything else keeps its preservation: the planner's `_twins` and the
     optimizer's `_upgrade_to_inner` refuse a nullable key off those two,
     and `_pair_fully_matches` refuses a nullable subset side.

   **The systematic case.** The suite never saw this because every NULL
   group in its data sits on both sides or on neither.
   `tests/join_matrix/test_null_group_matrix.py` sweeps two facts reading one
   key with no declared relation: each side's binding (`?k`, `~?k`), where the
   NULL-key rows are (left, right, both, neither), the key kind (`k`, `k + 1`,
   a dimension attribute `year`, `year + 1`) and whether a dimension holding
   the whole domain is in the model, against a null-safe FULL oracle. 33 of
   its 96 cells lost rows on `main`, and the twin and subset rules above fixed
   24. The other 9 were the planner's base typing, which predates seam 2:
   - `_nullable_join` typed two sides that each NULL the key as a value
     INNER null-safe (a derived `k + 1` the planner joins row-wise). Two
     sides joined on no key solid on both, that read other tables through
     the same filters (`_null_groups_unanchored`, over `SideFacts.leaves`
     and `filters`), now keep both, FULL, unless the key is authored. A solid
     key says which row is which (a `?genus` beside the `species` that
     determines it pairs by species). Sides whose filters differ keep their
     typing: one may be a WHERE's gate and the other its gated rows, which
     the INNER filters by intent (`test_oracle_expressions_laws` seeds 13 and
     2948 leak a rejected row as FULL). Padding NULLs, absence rather than
     values, stay as they were (tpc-h adhoc07's customers with no order).
   - A `~?k` fact's undated rows reach the year aggregate as guest padding
     (a value-NULL key with no dimension row). `_nullable_join` read that
     aggregate as the dimension's lookup (#972) and as a sole host's feeder,
     and both dropped the guests. A guest NULL is a fact row: it now vetoes
     the lookup reading, and a feeder's guests keep the join FULL when the
     host reads other rows (`_null_groups_unanchored`).
   `test_two_source_matrix.py`'s nullable axis now also puts the NULL row on
   one side only (`harness.matrix_rows(null_sides=...)`), pinning that a
   declared relation treats a one-sided NULL group like any side-exclusive
   key.

   Census over the non-law suite: 373 to 299. About 57 of those are
   refusals, not moves to the planner: the `~` stamp class fell 72 to 20 and
   twin equivalence 56 to 42 because the joins they narrowed now stay
   preserving. About 17 are the planner's new declared arm. What is left:
   unnest of complete lists (163), twin equivalence over a filter chain or
   an INNER-joined partner (42, `test_split_key_gates`-style: a side reads a
   stream INNER-joined to the other side's filtered group), graph subsets
   (31), and multi-left or later joins.

   Gates, against `4bf7383`: suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`,
   laws included, 49,914 tests) moved the distinct join shapes of 125 tests
   (INNER -85, FULL +76, RIGHT +6, LEFT +3), each under a row oracle that
   stayed green: `test_padded_null_pairing` (43), `test_oracle_spellings_laws`
   (19), `test_fixed_value_lifecycle_laws` (8), `test_oracle_expressions_laws`
   (7), `test_summary_cover_laws` (6), the rest one to five each. The suite
   data holds NULL groups on both sides or neither, which is why it never
   saw the loss. The one narrowing,
   `test_rollup_grand_total_survives_a_subset_join_back` (FULL to RIGHT), is
   the planner's declared arm; its rows are asserted. Corpus (207 statements,
   tpc-ds aggregates included): 1 moved, q84 inlines one CTE and its
   `is not null` guard moves across a null-safe INNER to the WHERE. The suite
   fails the same 25 tests as `main` (credential-bound and
   `the_look::test_wrapped_aggregate_with_dependent_by_grain`). Fuzzer
   522/522.

   Gates for the base-typing half, against the twin and subset rules alone:
   suite SQL capture moved the join shapes of 148 more tests (INNER -218,
   FULL +165, RIGHT +37, LEFT +17), each under a row oracle that stayed
   green: `test_partial_lookup_fallback_oracle_laws` (24),
   `test_rollup_resolution_laws` (24), `test_rank_beside_partial_merge` (18),
   `test_shared_attribute_aggregate_matrix` (12), the rest ten or fewer
   each. Corpus: unchanged (q84 alone, as above). Fuzzer 522/522; the suite
   fails the same 25 tests as `main`. The census reads 325 over the non-law
   suite, the new matrix cells included.

   Found, not chased (predates seam 2, same on `ae34eb2`): `where year = 2024
   select cust_id, count(order_id), count(visit_id)` over orders and visits
   sharing one `date_id` types the final join INNER at plan time and returns
   no rows when no customer both ordered and visited in 2024.

   **Sixth slice (landed): twins of one stream, EQUAL keys both sides carry
   whole, handles through their content.** The census on `aebcc35`
   (`upgrade_census_plugin.py`, non-law suite) read 324 firings: unnest of
   complete lists 163, twin equivalence 88, graph subsets 43, `~` stamps 6,
   recursion seeds 7, `_joined_complete` 17. Three planner gaps held most of
   the twin class, each now closed:
   - **Twin aggregates of one stream** (`_one_read`). Two groups over the
     same merged rows, joined back on their key, read one value set whatever
     NULLs it: the null-group matrix's derived cells FULL-join the two facts
     on `k`, group each sum by `k + 1` over that stream, and rejoin. `_twins`
     refused the guest NULL both sides hold. Two sides that read the same
     rows (`same_rows`) and take the key off the same leaf tables, every row
     of which their shared filters pass is still on each (`key_providers`,
     through BASIC and ROWSET lineage), now cover each other. A NULL group
     pairs when both sides NULL the key, the pair is null-safe
     (`get_modifiers`) and no span pads it for one side alone. A grouping
     set's subtotal NULL is no row of the stream: a ROLLUP beside its own
     input keeps its RIGHT (`test_rollup_label_over_union_joined_rowsets`
     lost its grand total in the first cut). Same leaves are not enough on
     their own: a FULL and a LEFT of the same two tables can each carry the
     key complete off different providers.
   - **EQUAL keys both sides carry whole.** `_independent_full_keys`
     exempted an EQUAL key both sides carry every value of
     (`complete_equal`), but `_covered_sides` still refused any key in
     `full_join_keys`, so the walk typed such joins by `_nullable_join` and
     left the narrowing to the optimizer. Both now read `_full_keys`.
   - **A handle carries its content's domain.** `_carries_every_value`
     transferred completeness through BASIC lineage only; the optimizer
     (`_COMPLETENESS_TRANSFERRING`) also goes through ROWSET. A handle whose
     body reads every row of a complete binding, no filter or limit on the
     way, carries the whole domain, so `equal join ra.k = rb.k` over two
     unfiltered rowsets types INNER at plan time. A filtered body still
     refuses (`test_join_merge_parity`'s `yoy_independent_rowsets` keeps its
     FULL), so a handle is still complete for itself only at its boundary.

   **A bug on `main`, found by the twin census:** under `merge w_year into
   s_year` the optimizer's equivalence upgrade typed the year join INNER
   null-safe whenever both sides NULLed the year, and the undated store sales
   vanished when web held no undated row. The fifth slice let a declared
   relation speak for its keys' NULL groups; it speaks for value NULLs, not
   for a guest (a fact row whose key found no dimension row).
   `_null_groups_differ` now refuses a guest NULL under a declared relation
   too, unless both sides read the same rows. The shared-dimension and
   `union join` forms of the same query were already right.
   `test_a_merged_attribute_keeps_one_facts_undated_rows` pins it (red on
   `main`).

   Census over the non-law suite: 324 to 256 (twin equivalence 88 to 31,
   unnest of complete lists 163 to 155, `_joined_complete` 17 to 14: the
   twins' joins fed those). What is left of the twin class: a side whose filter only the
   merge applies (`facts.applied`, `test_split_key_gates`: the gate group
   joins counts over the stream INNER-joined to it, and the HAVING reaches
   the gate only when the optimizer pushes it down), multi-left and later
   joins, and twins over an unnest connector. No optimizer helper reached 0.
   `test_subset_join_plan_time_completeness.py` pins the three planner
   proofs with the optimizer rule stubbed (all red on `main`).

   Gates, against `aebcc35`: suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`,
   laws included, 50,207 tests) moved the join shape of 98 tests
   whose statement count held (INNER +44, FULL -42, LEFT -34, RIGHT +32), none
   gaining a CTE or a join, each under a row oracle that stayed green:
   `test_dimension_chain_oracle_laws` (43), `test_summary_cover_laws` (10),
   `test_rollup_resolution_laws` (10), `test_grain_pin_unnest_merge_filter`
   (6), `test_oracle_compositions_laws` (5), the rest four or fewer each.
   Most of the planner's new INNERs were already the optimizer's, so they
   move no SQL. One law seed changed outcome: compositions 83051 (summary),
   an `OPEN` wrong-rows lead, planned right (two aggregates over one stream
   typed INNER); main fixed it independently in 0.3.525 before this landed.
   Corpus (205 statements, tpc-ds
   aggregates included): 0 moved. The first cut of the guest refusal moved
   hackernews adhoc05 (twin groups over one stream INNER to LEFT): the
   optimizer reads the two CTEs' sources, and one predates the WHERE the
   optimizer pushed into their shared parent, so it judges the rows by the
   CTE filter chain too (`_same_filtered_rows`). The suite fails the same 25
   tests as `main` (credential-bound and
   `the_look::test_wrapped_aggregate_with_dependent_by_grain`). Fuzzer
   522/522.

   **Seventh slice (landed): elements of complete lists, and the first
   optimizer helper retired.** The census on `33f9816` read 255 firings over
   the non-law suite, 155 of them the optimizer reading an unnest's elements
   as complete (`_unnested_parent`), every one a single-left first join, so
   all within the planner's whole-ON proof. Three changes moved them:
   - **An unnest's elements are complete when its lists are.**
     `complete_key_domain` reads a source that only unnests one source
     (`_unnested_list`) as carrying every element when that source carries
     every list, through BASIC and ROWSET lineage as before. The owner
     columns stay incomplete: the `UnnestNode` stamps them `~`, since an
     owner whose list is empty or NULL yields no row.
   - **The lists count only where the joins keep them by type or by a
     declared subset** (`declared_only`). Read INNER beside the trees, the
     lists of a species with no tree are gone, and the trees' complete
     `species` binding is no promise that every enrichment row found a tree
     (`test_unnest_filter_laws::test_element_pairs_with_the_row_it_expands`:
     a region only a treeless species lists pads in, 12 cells red in the
     first cut). A twin match or `_partners_every_right_row` beside another
     complete binding does not keep the lists; a right binding the key `~`
     beside a complete left does (the native-range build's
     `species LEFT JOIN ~enrichment`). That is the optimizer's own line:
     `_keeps_rows_of` asks `_pair_side_fully_matches`, which wants subset
     evidence.
   - **An unnest is its own binding of the owners it thins.** The first cut
     of seam 2's unnest half (and this slice's) lost the empty-list species of
     the native-range build: an aggregate over the unnested rows and the
     enrichment read both bind `~species` through the same leaf, so
     `_one_binding` read them as two projections of one subset and
     `_nullable_join` typed them INNER. On `main` that join stayed FULL only
     because the eco join's padded row NULLed `species` on the aggregate.
     `partial_binding_sources` now adds the unnest source itself when its own
     unnest stamps the address, so the two reads are peer subsets and keep
     both.

   With the class at 0, the optimizer's UNNEST branch of `_complete_values`
   and `_unnested_parent` are deleted, the first optimizer helper seam 2
   retires. Census over the non-law suite: 255 to 101 (graph subsets 31,
   twins 23, `_joined_complete` 14, rowset boundaries 12, `~` stamps 6,
   recursion seeds 4, the rest 11). Over the whole suite, laws included:
   134. `test_subset_join_plan_time_completeness.py` pins the element proof
   and the empty-list owner with the optimizer rule stubbed (both red on
   `main`).

   Gates, against `33f9816`: suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`,
   laws included, 50,237 tests) moved the join shape of 1,949 tests (FULL
   -4,530, RIGHT -1,700, INNER +4,537, LEFT +1,767, 9,318 fewer
   coalesces), mostly `test_relation_rollup_laws` (1,240) and
   `test_unnest_filter_laws` (576), each under a row oracle that stayed
   green; no test changed outcome. 37 plans gained a join or a CTE
   (`test_grain_pin_unnest_merge_filter` 18, `test_summary_cover_laws` 7,
   the merge-bridge tests 10, `test_unnest_partial_stamp` 2): with the
   unnest-side join narrowed at plan time the element CTE no longer carries
   the dimension's attributes, so the lookup is re-joined downstream. Rows
   are right; folding the attribute back onto the narrowed join is open.
   (`tpc_ds_duckdb/test_queries` also reads as moved: its statement count
   varies with the fixture under xdist.) Corpus (207 statements, tpc-ds
   aggregates included): 0 moved. The suite fails the same 25 tests as
   `main`. Fuzzer 522/522.

   **Eighth slice (landed): recursion walks, per-key subsets, EQUAL keys and
   rule B.** The census on `08cd7f1` read 101 firings over the non-law suite
   (graph subsets 31, `~` stamps 20, twins 20, rowset boundaries 12, EQUAL
   keys both sides carry 11, recursion seeds 7). Five planner gaps held about
   half of them:
   - **A finished walk carries its seeds.** `complete_key_domain` refused
     the recursion's gated read for its condition. The gate picks the one
     row each seed row's walk ends on (`_recursion_seed`), so the walk carries
     what its seed does, and a recursive value's domain is its image over the
     seed's start column (`_walk_carries`). The walk's value was also stamped
     nullable off its edge argument, so `merge root into app_id` paired
     null-safely and the EQUAL cover refused it: `recurse_edge` is NULL only
     where its start is (`_function_is_nullable`), and the join renders a
     plain `=`.
   - **A read of a wider spelling carries the key.** A scan reading
     `launch_info.id as payload_launch_id` (`merge payload_launch_id into
     ~id`) or `procs.pid` renamed into its merged `app.pid` carries every
     value of what it reads. `complete_key_domain` matched by exact address;
     its `aliases` are now the spellings the graph puts the key within
     (`_wider_spellings`: SUBSET or EQUAL), at every lineage step of
     `_carries_every_value`.
   - **Per-key subsets on a composite join.** `_partners_every_right_row`
     took one-key joins only. A right that binds every key of the ON `~`
     beside lefts complete on each (`launches ~vehicle, ~variant` beside the
     vehicles) keeps every row, the optimizer's own per-pair reading
     (`_keeps_rows_of`), so the launch columns stay complete past the LEFT.
   - **An EQUAL key is no veto.** `_covered_sides` refused every outer key,
     EQUAL ones included unless both sides carried them whole. An EQUAL
     relation between two distinct concepts is subset evidence like any other
     (`merge st into store_id`: a filtered sales group beside the complete
     stores), so only the optimizer's veto set (`full_relation_keys`) refuses,
     carried as `JoinFacts.equal_keys`. Not off a rowset boundary
     (`boundary_covered`): a handle is complete for itself, not for a domain
     an EQUAL declaration merges it into (`yoy_independent_rowsets` keeps its
     FULL).
   - **Rule B at plan time.** A vetoed key still narrows where the graph
     proves a directed subset path between the two concepts
     (`proven_covered`): a filtered rowset `union join`ed to an unfiltered one
     over the same column. `_proven_subset_of` moved to
     `DomainGraph.proven_within`, which the planner and the optimizer now
     share.

   The optimizer's recursion-seed branch of `_complete_values` is deleted
   (`_recursion_seed` stays for `_accumulate_filter`: a gate is no filter for
   the twin test). Census over the non-law suite: 101 to 53 (twins 20, graph
   subsets 16, of them q64's 12 lookups, `~` stamps 6, EQUAL keys 6, rowset
   boundaries 4, the twin over hackernews adhoc05's pushed WHERE 1). The
   rowset-boundary branch is still load-bearing: ablated, 4 multi-left plans
   move and 3 of them lose rows. q64's 12 are lookups beside a rowset whose
   `union join` keys `ensure_content_preservation` keeps as axes after the
   cover drops them; that is where the next slice starts.
   `test_subset_join_plan_time_completeness.py` pins the walk, the per-key
   subsets, the EQUAL cover and rule B with the optimizer rule stubbed (all
   four red on `main`).

   Gates, against `08cd7f1`: suite SQL capture (`sqlcap`,
   `PYTHONHASHSEED=0`, laws included, 51,760 tests) moved 982 tests (FULL
   -411, LEFT +402, INNER +18, RIGHT -9 across their statements, 501 fewer
   coalesces, 120 fewer null-safe equalities), mostly
   `test_oracle_spellings_laws` (414), `test_relation_rollup_laws` (142),
   `test_oracle_expressions_laws` (108), `test_complete_binding_lookup` (82)
   and `recurse_edge` (42), each under a row oracle that stayed green; no
   test changed outcome. 132 plans lost a CTE or a join; 4 gained a CTE
   (`test_merge_derived_bridge[aggregate]` 2, `test_spellings_60k_gates` 2):
   every join now INNER at plan time, the walk keeps its stated order from
   the ecoregions and the count regroups by `(tree_id, bucket)` above it, the
   fourth slice's open shape. Rows are right. Corpus (205 statements, tpc-ds
   aggregates included): 2 moved, hackernews adhoc03 and adhoc05 read
   `root_parent.id` bare where they coalesced it with the walk's root across
   an INNER equality. The suite fails the same 25 tests as `main`. Fuzzer
   522/522.

   **Ninth slice (landed): rule B over a related member, and an EQUAL key
   both sides carry whole dedups.** The census on `5336262` read the eighth
   slice's 53 firings over the non-law suite. Two planner gaps held 7 of
   them:
   - **Rule B asked only where the graph relates nothing.** `_covered_keys`
     computed the rule-B proof (`proven_within`) only when the graph did not
     already put the two spellings within each other (`not within`). A
     `union join` member read off the column it joins is within it (q64's
     `agg_99.item_sk_99` over `ss.item.sk`), so the cover held, the veto
     refused it, and no proof was asked for. The proof is now asked for
     whenever the key is vetoed. q64's item lookup types RIGHT at plan time
     (the rowset covered) and the consumer's pushed WHERE makes it INNER; it
     was FULL, then LEFT under the guard rule, then INNER under this one.
   - **An EQUAL key both sides carry whole dedups its lefts.** `_join_onto`
     kept every left providing a `full_join_keys` key, EQUAL ones included,
     though `_full_keys` releases an EQUAL key both sides carry whole. The
     chain `merge ka into kb; merge kb into kc` joined `c` onto `a` and `b`,
     a two-left join `_narrow_covered` skips, so the joined sides lost their
     completeness and the aggregates above them re-joined FULL. Now `c` joins
     onto `a` alone, covered, and the chain types INNER end to end
     (`test_multiway_matrix` equal cells: 4 firings to 0).

   Census over the non-law suite: 53 to 46 (q64 and its join-form twin 12 to
   9, multiway equal 4 to 0). Over the whole suite, laws included, 90.
   `test_subset_join_plan_time_completeness.py` pins both with the optimizer
   rule stubbed (both red on `main`).

   Measured and not landed:
   - **An EQUAL key one side NULLs alone (6 firings).** `select id, label`
     under `merge first_org into code` keeps a launch with no agency at plan
     time; the optimizer's EQUAL arm drops it (`_upgrade_to_inner` makes the
     pair null-safe and narrows). Taught to the planner (both sides
     `complete_equal`, value NULLs on either), the rule also moved hackernews
     adhoc06/07 (every post with no GitHub URL dropped under `merge
     github.repo_url into any_github_url`) and q04-agent's fact-to-customer
     lookups (a sale with no customer dropped under the cross-channel
     customer merges), which no plan dropped before: the optimizer's arm is
     stopped there by the WHERE it pushed down first. The join matrix's
     `equal` oracle drops a one-sided NULL group, but a value NULL is no
     value the declaration speaks for, so this is a semantic decision, not an
     ownership move. Ruled in the tenth slice: it keeps its rows.
   - **q64's other three lookups (9 firings).** After the cover the planner
     types them RIGHT, and `ensure_content_preservation` widens them to FULL
     for the item lookup's padding. Only the consumer's WHERE (`cnt_00 <=
     cnt_99`, pushed down by the optimizer) proves the rowset present. Letting
     `narrow_join_types` read a forced covered side as forcing its partner
     (`matched_left`) is sound but never fires, because the merge holds no
     proof. `UpgradeJoinOnGuards` could read the planner's record, but only if
     no rule had filtered the covering side since, so it stays.
   - **Twins over a gate (8, `test_split_key_gates`-style).** The gate group
     the final merge reads is unfiltered at plan time, and the merge's own
     WHERE filters it, so the cover is a fact about where the filter sits. A
     multi-key "every right row is a left row" proof for a gate grouped back
     off its rows moved nothing.
   - **Multi-left `~` stamps (5, `test_bigquery_full_join_keys`).** The ON has
     two lefts until `PrunePreservedJoinKeys` drops one. INNER at plan time
     would AND both pairs and drop the rows the second left's padding NULLs.

   Gates, against `5336262`: suite SQL capture (`sqlcap`,
   `PYTHONHASHSEED=0`, laws included, 53,499 tests) moved no plan: of 941
   textual moves, 937 are worktree paths and tests that compile a varying
   number of statements, `test_cli_consistency` is known noise, and the
   other two compile byte-identical SQL when run alone (xdist CTE naming). The
   optimizer reached the same SQL on every shape; the slice moves who decides
   it. No test changed outcome. Corpus (205 statements, tpc-ds aggregates
   included): 0 moved. The suite, laws included, fails the same
   25 tests as `main`. Fuzzer 522/522.

   **Tenth slice (landed): a declared relation does not speak for a NULL.**
   The ninth slice left the EQUAL key one side NULLs alone open as a semantic
   question. Ruled: `merge a into b` declares one value space and says
   nothing of a NULL; a nullable binding (`?`, a null-propagating
   derivation such as `split(agency, '/')[1]`) or a guest keeps its rows.
   `_null_groups_differ` had the opposite branch for a declared relation (it
   "spoke for its keys' value NULLs", made the pair null-safe and narrowed
   to INNER, dropping a NULL group only one side held); it now treats a
   declared relation's nullable key like any other, so the optimizer refuses
   the upgrade unless both sides read the same filtered rows. The planner
   already kept those rows, so the ruling is the planner's answer and the
   optimizer stops overriding it.
   - The census on `4ede680` read 40 firings over the non-law suite; five
     went through the EQUAL-nullable branch (`first_org` twice,
     `web.billing_customer.sk` three times, both left-nullable). After: 35,
     those five gone, none new.
   - The test-sized gcat models declared `merge first_org into org.code`
     where the real model says `~org.code` (not every org code is a first
     org): `gcat2/launch.preql` and the two `first_org` discovery tests now
     declare the subset. hackernews `merge github.repo_url into
     any_github_url` stays EQUAL (neither side is a subset); its
     `any_github_url` is nullable, so the ruling keeps its posts.
   - Corpus: 1 moved, `_q04_agent_rowset_union_join` types its web-sales to
     customer lookup LEFT (`ws_bill_customer_sk` is `?`); the 8 rows on
     sf=1 are identical. `test_equal_merge_keeps_null_bindings.py` pins the
     ruling (red on `main`: the launch with no agency dropped). The join
     matrix holds a NULL row on both sides of every nullable cell and does
     not reach the branch.

   **Eleventh slice (landed): the rule's proofs the planner owns, ablated.**
   Ten slices moved proofs into the planner without deleting the optimizer's
   copies. This one asks, branch by branch, which of them still decide a
   firing. A census over the whole suite (non-law and law files, the
   `tests/grids.txt` grids included) puts the rule at 87 firings in 75
   tests across 28 files: 35 non-law, 52 law. Each of 24 proof branches got
   a temporary off switch. Every switch makes the rule narrow less, so it
   can move only a plan the rule fires in. Each was switched off alone
   against those tests under a SQL capture (`PYTHONHASHSEED=0`, two
   baseline runs to fix the noise floor: one sweep shape renames a CTE). The
   nine that moved nothing were then switched off together, and the deletion
   itself was measured against the same tests. 0 plans moved. About 170
   lines are gone from `value_set_join_upgrade.py`:
   - `_recursion_seed` and its exemption in `_accumulate_filter`. A
     recursion gate now counts as a filter on the twin test. The planner's
     `complete_key_domain` reads the walk (`_walk_carries`, eighth slice).
   - In `_complete_values`, everything past the direct evidence: the 1:1
     passthrough recursion, the rowset rename at matching grain arity, the
     BASIC/ROWSET derivation transfer, and `_joined_complete` /
     `_keeps_rows_of`. `complete_key_domain` and `_carries_every_value`
     prove each one on the datasource tree. What is left is the grain of a
     GROUP BY or an authoritative scan, and a non-partial binding on that
     scan.
   - The EQUAL nullable upgrade in `_upgrade_to_inner`. An EQUAL key that a
     side can NULL no longer becomes null-safe and INNER; it refuses, as
     every other nullable key does. The tenth slice's ruling already stops
     the case it served through `_null_groups_differ`.
   - `_anchors_coalesce` and `_paired_before` (join-axis audit, part 84: a
     second `subset join` member's ON coalesces its lefts). Since the
     plan-time subset slices, no firing reaches it: the
     `test_scoped_left_join_multi_partial_anchor` chain and its
     `shape_flipped` twin plan the same SQL without it. A walk-flip run (`WALK_FLIP=shape,name`) over the 121
     test files naming a `subset join` or `union join` fails the same tests
     before and after.
   - The declared-relation NULL-key partner in `_pair_fully_matches`,
     with `_unpaired_guest_padding` and `_guest_padded`. A subset side
     that can NULL the key no longer narrows. The planner types the guest
     case (`_unpaired_value_nulls`), and the unit row pinning the helper's
     negative case is dropped.

   **Kept, each with what moves without it:**
   - The equivalence upgrade and its `_equated_reads` witnesses: 6 plans,
     and `test_a_left_an_inner_join_equates_witnesses_the_key_set`.
   - Directional narrowing: 8 plans.
   - Each of its directions: FULL to LEFT (2 tests fail), FULL to RIGHT
     (4 plans), and directional to INNER (7 plans).
   - The graph path (7 plans), the stamp path and the genuine stamp (4
     each), rule B (3).
   - The rowset-definition boundary: 3 plans, and q44 and the store-anchor
     chain fail.
   - `_complete_values`' authoritative-scan arm (2 plans) and its grain
     arm. Off, the grain arm moves no suite plan, but it fails two
     hand-built rows of `test_value_set_join_upgrade` (rule B, the genuine
     stamp), and one law plan reorders its columns.
   - The EQUAL arm of `_pair_values_equal` (1 plan) and
     `_lefts_alone_before` (2).

   The vetoes (`_null_extended_before`, `_unshared_join_padding`,
   `_null_groups_differ`, `_row_limited`, `_extent_free`, the grouping-set
   guard) were not ablated. Turning a veto off makes the rule narrow more,
   which is a semantic change, not a harvest.

   Gates, against `5da38ee`:
   - Firing tests: 0 plans moved, non-law (14 files, 333 tests) or law (49
     tests).
   - Corpus (206 statements, tpc-ds aggregates included): 0 moved.
   - Fuzzer: 522/522.
   - Walk flip over the 121 subset/union-join files: the same failures as
     `main`.
   - Suite: see the PR.

3. **A rowset handle is a leaf (landed).** `row_lineage_arguments`
   returns nothing for a ROWSET handle, so no assembly row-lineage walk sees
   through one, and `concept_satisfiable` no longer judges a handle
   renderable off its content (it did in 46 tests). The one place a handle is
   computed off its content, its own boundary, says so:
   - `projection.renders_at`: a `RowsetNode` vouches for its rowset's
     handles, and for what is computed over them (`case agg.k ...`). They
     are seeded into the satisfiability cache, not added to the available
     addresses, so a `union join` mate never matches one as a pseudonym
     (added as addresses, q64 widened both `agg_*` boundaries with
     `ss.item.sk`, `ss.store.sk`, +2 CTEs).
   - `satisfiable_outputs(boundary=True)` for a ROWSET group's own build
     (the union TVF's handles off its arms' columns).
   - `widen_projection` on a `RowsetNode` takes a bounded handle's content
     as the input, never the handle.

   With it went `_synthesizes_handle` and its three call sites, and
   `_crosses_outer_aggregate`'s own handle stop (now `iter_row_lineage`).
   `literal_producible` reads a handle as it reads a ROOT: no row parent
   produces it. The first cut's failure,
   `test_tvf_union_order_by_grouped_away_column`, was a BASIC over a handle
   on the union's boundary; `renders_at` covers it.

   Measured and kept, because the leaf rule does not cover them:
   - the cross-rowset guard in `_carry_join_keys`: a handle carried onto
     another rowset's boundary under its `union join` mate's column, by
     pseudonym, not by content. Dropped, q64 re-exposes two mates as
     `agg_00` handles on the `ss` boundary.
   - `_raise_if_rowset_islanded`, `_mangled_rowset_content_addresses`:
     pseudonym connectivity, not row lineage.
   - `_carry_needs_own_node`'s handle stop and
     `concept_graph._lineage_leaves`: they walk existence arguments, which
     the row-lineage walk skips.

   Gates, against `5daf4ca`: corpus 0 moved (207 statements, tpc-ds
   aggregates included); fuzzer 522/522; every test file naming a rowset
   (3,261) green. `test_v4_rowset_handle_leaf.py` pins the leaf, the
   boundary and the ROWSET group's build.
4. **Rowset witness from the body plan (landed, recomputation half).**
   `_witness` built the body's concept graph and keyspace itself, and the
   body's plan built them again when `resolve_rowset` planned it, from
   inputs that had drifted apart: the witness asked for the body's outputs
   without its HAVING presence probes, and built the graph without the
   scan-filtered and coarse-rolled roots or the union datasources that
   pick the first materialized roots. Now both ask
   `concept_strategies_v4.plan_front` (concept graph plus keyspace, the
   part of a plan before it groups) with the same request
   (`nested_select.body_mandatory`, `first_materialized_roots`). The
   witness builds it and leaves it on `V4History.plan_fronts`, and the
   body's first `_build_from_graph` takes it, so the witness reads exactly
   the keyspace the body's plan groups over, and the body is never built
   twice. A front is kept under the same rule as its witness: one that read
   an enclosing computation's placeholder is dropped with it.

   Not landed: the recursion guard (`live_witnesses`, `witness_floor`).
   Planning the body first so it could hand its keyspace up does not work:
   the outer keyspace needs the witness, the outer span scope needs that
   keyspace, and the body is planned under the spans the outer plan owns.
   What the guard handles is a cycle (a body reading its own rowset), not a
   second computation, so it stays wherever the keyspace is built.
   The respelling in `keyspace.rowset_witness` stays too: the reader
   names the body's regions by its handles.

   Gates, against `a1bf436`: corpus 0 moved (207 statements, tpc-ds
   aggregates included). Over every test file naming a rowset (11,168
   tests, green), the body plan took the witness's front 4,879 times, out
   of 30,402 front builds, and each one matched a fresh rebuild of the
   graph, attributes, edges and keyspace. `test_v4_rowset_witness_front.py`
   pins that the body's plan groups over the keyspace its witness read; it
   fails on `main`.
5. **Provenance stamped by the producer (landed, ownership half).** A
   source owns its null provenance: `null_provenance.provenance(source)` is
   the source's stamp, each answer (`extent`, `guest`, `rollup`,
   `padded_by(spans)`, `values(c)`, `padders(k)`) computed once from its own
   joins and its parents' stamps, never by walking past a parent. A
   `QueryDatasource` keeps the stamp until one of its fields is written (the
   `__setattr__` that already drops its identifier cache), so an optimizer
   rewrite of a source's joins or parents restamps it.

   With it went the plan-wide memo: `ProvenanceMemo`, the `plan_scope`
   ContextVar around planning in `query_processor`, `active_memo`, the memo
   threaded through `get_modifiers`, `_span_padding_matrix`, `_side_facts`
   and `_Merge`, and the free walks (`extent_null_addresses`,
   `guest_padded_addresses`, `span_padded_addresses`). The planner and the
   optimizer now read one stamp: the optimizer's `value_set_join_upgrade`
   readers walked uncached before, because the memo's scope ended where the
   optimizer starts rewriting joins; a write now restamps the written source
   instead. `nulls_are_values` asks each carrying parent's stamp, so its
   cycle guard tracks only one source's argument chain, and
   `padding_sources` is a union of per-key stamps (its `canon` parameter
   always was the identity).

   Not landed: collapsing the five provenances into one per-address stamp
   (`value | extent`, as the corpus suggests). The audit's nullability
   register keeps each of `guest`, `rollup` and `span` pinned by engine
   rows, so that is a semantic move, not an ownership one. `padded_by` stays
   keyed by span set: an address counts only when EVERY provider of it was
   extended, so a set's answer is not the union of its members'.

   Gates, against `162f024`: corpus 0 moved (205 statements); the suite
   SQL capture (48,458 statements, every one any test compiles) 0 moved once
   `PYTHONHASHSEED` is pinned (unpinned, 46 move in both directions between
   two runs of the same commit: GROUP BY, SELECT and grain-check column
   order, and local server ports); fuzzer 522/522; the suite fails the same
   25 tests as `main` (credential-bound, and
   `the_look::test_wrapped_aggregate_with_dependent_by_grain`).
6. **Drop the `narrow_equal_domain_joins` knob (landed; the premise behind
   the rest was wrong).** The knob is gone, and
   `DomainGraph.full_relation_keys` (outer-relation keys minus the EQUAL
   ones) is the one owner of "no narrowing collapses this key", read by
   `MergeNode`'s coalescing veto and `UpgradeOuterFromKeySetEquivalence`,
   each of which had recomputed it.

   The seam's premise, "under model trust `merge a into b` makes one key",
   is wrong, and its planner half (type an EQUAL key like any other instead
   of FULL) is closed as not a seam. An EQUAL declaration (`merge a into b`,
   `merge equal a = b`, `equal join a = b`, one declaration in every
   spelling) says the two DOMAINS are one value space. It does not say that
   every side of a join carries the whole domain: a side is a filtered, an
   aggregated or an unnested read of it. So the join preserves both sides
   until each proves complete, and only then narrows; what the declaration
   buys is that completeness proven for `a` carries to `b` (`merge union`
   withholds even that, so it never narrows). The planner change assumed
   "not stamped `~`" meant complete, and the laws refuted it:
   `test_unnest_filter_laws.py::test_element_pairs_with_the_row_it_expands`
   pads a region only a treeless species lists (`(None, k)`), and
   `tree_reporting::test_native_range_build_matches_fixture` lost the
   species whose lists are empty. The laws now run every EQUAL cell under
   both `into` and `merge equal`, and the narrowing matrix under all three
   spellings, so the spellings stay one declaration.

   What remains is an ownership move, not a semantic one: the completeness
   proof runs in the optimizer after the planner emitted FULL, though many
   proofs (an unfiltered scan of a complete binding, a group over one) are
   knowable while planning. That is seam 2's plan-time evidence, and joins
   it. Two facts to carry there: the optimizer's EQUAL proof also narrows
   q04's agent-shape fact-to-customer LEFT joins (typed LEFT for a `?`
   value-NULL FK); and an UNNEST drops owner rows whose array is empty or
   NULL, yet `partial_binding_sources` names only the leaf `~` under it, so
   a plan-time proof must not read an aggregate over unnested rows as
   complete over the owner.

   Landed beside it, a bug on `main`: the `full_join_keys` rule fired on any
   connecting key, including a FULL key both sides' grains determine among
   the pair's other keys (`merge parent into direct_parent.id` beside `id`),
   which the pairing then drops. The join was typed FULL on a key it never
   rendered, padding every unfiltered row past the WHERE (hackernews adhoc02
   returned items outside `create_time.year = 2024`).
   `_independent_full_keys` asks for a FULL key that is an axis of its own;
   `tests/engine/test_duckdb_merged_recursion_filter.py` pins it (red on
   `main`). Corpus: adhoc02 alone moved (FULL to INNER). Fuzzer 522/522.
7. **One padding walk (landed; measured smaller than estimated).** The
   walks already shared `left_deep_joins` and `padded_by`; what each restated
   was the accumulation. `utility.padded_sources` (every source the joins
   pad) and `utility.padded_before` (each join with what the joins before it
   pad, read lazily so a caller that retypes the join pads by the new type)
   now serve `collect_applied_conditions`, `find_nullable_concepts`,
   `MergeNode._join_padded_addresses`, `prune_outer_join_pairs` and
   `restore_unpadded_joins`. The other walks each read a different fact, so
   no single walk subsumes them: `compute_outer_null_status` ranks a padded
   right past its lefts, `null_provenance._padded_addresses` pads only
   where a join extends, `_full_join_concepts` reads FULL joins' shared
   columns, and `narrow_join_types` visits UNNEST joins too. Corpus 0
   moved, fuzzer 522/522.
8. **`DomainKind` as a trace label (closed: already one).** No behavioural
   consumer reads `RegionDomain.kind`; `root_partition`'s trace is its only
   reader, and every decision keys on whether the domain has a bucket. The
   enum is the trace's vocabulary and the root-partition tests', so turning
   it into a string frees nothing.
9. **A boundary scope id** in place of `ConceptAttrs.rowset_name`: the one
   non-derivable fact it carries (a non-handle node that belongs to a
   boundary) is the same thing a union arm scope is. The three partition
   rules are already one scope-keyed rule; the tag itself remains.

10. **Grouping sets as a relational TVF (two slices landed: the rowset
    boundary, then the statement).** `by rollup|cube|grouping sets` is a select mutator: its
    aggregates carry the mode, its keys keep their own addresses, and about a
    hundred planner sites (`nulls_grouping_keys`, `rollup_padded`,
    `rollup_padded_keys`, `node_nulls_grouping_keys`, the `rollup_padding`
    join rule, `null_provenance.rollup`, `_fold_rollup_key_dims`) exist to
    remember that a NULL on one of those addresses may be grouping-set
    padding. The end state is the union TVF's: a rollup is a relation whose
    outputs are NEW concepts. A rolled key is a nullable key of that relation
    and its subtotal NULL a value of it, the grouping flags complete the
    identity, a measure is a property of both, and nothing outside the
    relation tracks padding.

    - **Landed.** `RollupSelectStatement` / `RollupSelectLineage` /
      `BuildRollupSelectLineage` sit on the union TVF's base with one arm, the
      select carrying the spec. `lower_grouping_select` (`tvf_rules.py`) mints
      one output per arm column: a key where the arm's grain holds it (its
      keys and the injected flags), nullable unless a flag, else a property
      of that identity. `gen_rollup_select` plans the arm as any nested
      select and re-exposes it through a rename-only `SelectNode`, with no
      `UnionNode`, so no arm pushdown reaches below the pass. A rowset body
      (`with r as select ... by rollup (...)`) and an inline `(select ...)`
      lower to it, so every rowset handle over a rollup reads a TVF output
      and the authoring surface is unchanged (the renderer prints the arm).
      A spec over no keys is one standard group and stays a select.
    - **What it changes.** A rollup rowset is now a rowset like any other:
      the passthrough exemption from the FINAL dedup
      (`node_nulls_grouping_keys` in `_final_dedup_targets`) no longer
      reaches it, so `select r.dept, r.total` over a handle set that omits a
      flag dedups as a plain rowset's does. Rows differ from before only
      where the data holds a NULL key whose leaf row equals its subtotal
      row; project the flag to keep both.
      Two render sites met that dedup for the first time.
      `BuildMultiSelectLineage.find_source` reads an arm column off the
      parent a folded consumer came from (`CollapseSingleParent` merges the
      rowset boundary into the rename CTE, which then projects neither). And
      `_get_aggregate_grouping` reads a handle rebound to its arm column
      (`rebind_rename_to_consumed`) as a passthrough when that column comes
      from a parent: the dedup group above a rollup rendered a second
      `GROUP BY ROLLUP` otherwise.
    - **Gate.** `tests/engine/test_duckdb_rollup_tvf_laws.py` runs 140 bodies
      as a statement and as a rowset read five ways (whole, without flags,
      filtered on a flag, filtered on a rolled key, re-aggregated per level);
      the suite held only about twenty rollup-in-rowset tests, and both
      render defects passed it. On main the laws fail 6 of 700 reads, all
      the duplicate rows the dedup exemption kept. Suite: the same 25
      failures as main (credential-bound). Fuzzer 522/522.
    - **Freed.** `rollup_padded_keys`' rowset branch and
      `rowset_to_concepts_v2`'s grouping-flag handle (a rowset's select never
      carries a spec with keys now, and a flag handle reads a TVF key): 0
      firings over the whole suite against 726 lowerings.
    - **Also fixed.** A union arm that only passes a rowset through
      (`union((select r.a, r.b), ...)`) did not plan: `widen_projection`
      handed the arm key, an inline constant, to the rowset boundary as an
      input no parent supplies. Not a rollup defect, but it is what a union
      of two rollups hits first.
    - **Second slice: the statement.** A top-level `select ... by rollup`
      is lowered at finalize (`lower_statement_grouping`, reached from every
      statement plan: select, persist, copy, show, call, chart). The select
      less its ORDER BY and LIMIT is the arm of a rollup TVF read through an
      anonymous rowset (`_rollup_<hash of the text and the statement's place
      in it>`, so a re-parse reuses its concepts and two statements never
      share them), and `SelectStatement.relational` is the select over that
      rowset's handles: every arm column, hidden ones hidden, with the ORDER
      BY re-pointed at the handles and the LIMIT. `process_query` plans
      `statement.planned` and keeps the authored statement for the contract:
      output columns, hidden set, local concepts and parameters are the
      authored ones, and `relational_names` (authored address to handle)
      rides `scoped_merge_map`, the map `sort_select_output_processed`
      already renders a column under its written name with. The shadowing
      step (a) asked for is not needed: no concept is redefined, the final
      projection re-labels. The authored statement, its renderer and a
      persist's datasource binding are untouched. The statement's joins now
      ride the arm, so `_collect_rowset_scoped_joins` reads them off the
      rollup's arm and skips every other statement's rollup relation.
    - **The SQL is the same.** Two optimizer facts keep the outer select from
      costing a query anything. `rename_reference` reads a rollup TVF output
      as the rename of its arm column it is, so the rename node and the
      boundary fold into the pass (`CollapseSingleParent`) with the outer
      select's ORDER BY and LIMIT. And `carry_root_contract` hides, on the CTE
      a fold makes the root, the TVF outputs the old root did not show: they
      are read through their handles alone, and left visible until the final
      projection they read as used, so the injected flags were carried
      through a window CTE above the pass for nothing. (Hiding every column
      the old root did not show is unsound: the final projection re-labels a
      target from a pseudonym twin the optimizer-time contract never named.)
      The seven TPC-DS rollup queries (18, 22, 27, 36, 67, 70, 86) render
      within four characters of their committed size.
    - **What the statement met.** Reading every rollup through the TVF sent
      shapes down it that only statements had: about 30 of the suite's
      17,300 rollup tests. Seven defects, the first four reproducing on main
      through a rowset:
      (1) the rename node inherited the arm's grain, so a read of the whole
      identity still regrouped; it now declares the relation's keys and a
      full read is a passthrough (a read that drops a flag still dedups).
      (2) A grouping key the arm does not project
      (`select label, sum(x) by rollup (dept)`) was listed as a rename input
      no parent supplied; the node now takes the arm's visible aligned
      columns only. (3) The align clause built the arm's columns in a
      factory without the arm's grouping spec, which recursed on a flag over
      a scoped-join key; a rollup's align now reads the arm's built outputs.
      (4) `find_source` did not find an arm column a FULL join had emitted
      under its partner's address once the consumer folded
      (`with a as select d, sum(x), grouping(d) as g union join d = e by
      rollup (d)` read with its flag). (5) `apply_child_merge` re-pointed a
      passed-through rename (`dept as d`) at itself when the rename node
      folded into the pass. (6) `find_source` and `_resolve_output_target`
      returned a same-address copy over the folded CTE's own column, whose
      lineage is the one the pass rewrote. (7) ORDER BY on a handle
      re-derived the expression where a visible output already carries it
      (`render_order_item`).
    - **Gate.** `tests/engine/test_duckdb_rollup_statement_laws.py` runs 100
      bodies as a statement and as hand-written DuckDB SQL, bare and under
      ORDER BY + LIMIT; the rowset laws keep the statement as their oracle.
    - **Found, not fixed.** A persist whose select hides a column writes a
      table one column wider than its INSERT (`to_datasource` lists hidden
      outputs), and a rollup always hides its injected flags, so
      `persist ... from select ... by rollup` fails at the warehouse, on
      main as here.
    - **Not yet.** The arm is still planned by the mutator, so every site
      above stays live for the arm's own pass, and a merge arm or union arm
      carrying a spec is still a mutated select. Next, in order: (a) move
      the post-aggregate scalars and HAVING out of the arm into the outer
      select, leaving a pure arm (keys, aggregates, flags) (landed, fifth
      slice below); (b) plan
      the arm as a STANDARD select and have the generator set the grouping
      sets on its root `GroupNode`, with a per-grouping-set union as the
      fallback where the arm is not one group; (c) delete the mode from
      `BuildAggregateWrapper` and the sites above with it.
    - **Third slice: the syntax.** `rollup((select ...), k1, k2)`,
      `cube(...)` and `grouping_sets((select ...), (k1, k2), (k1), ())` are
      the authoring forms, in both grammars. A call is read three ways:
      spread into a query (`select *rollup(...) having ... order by ...
      limit ...;`), named (`with r as rollup(...) [-> (names)]`), or inline
      (`from rollup(...) -> (names) select ...`). Each hydrates to the arm
      select carrying the spec and reaches `lower_grouping_select` /
      `lower_statement_grouping` unchanged, so the generated SQL did not move.
      An inline or signed call's handles live in a per-statement namespace;
      its signature names are bound only while the reading select hydrates
      (`SemanticState.name_bindings`) and shown through
      `SelectStatement.labels`. The `by rollup` select clause still parses, to
      raise a migration error naming the call; a merge and a persist can no
      longer carry a spec directly and read a named call instead.
      `local_scripts/migrate_by_rollup.py` rewrites the clause.
    - **`select *<call>` is the one statement form for every TVF.** `*`
      spreads a relation's outputs into the select, so a new TVF needs no
      statement keyword and `*union(...)` is never the `union(a, b)`
      expression. `-> (names)` is the positional signature only; `--name` in
      it binds a column without showing it. Decided, not built: `*@name(args)`
      for a user-defined TVF (beside the scalar `@name(args)`); more than one
      spread, as `*f(...) as x, *g(...) as y` with a declared join on their
      keys; a spread beside other select items; a call as another call's
      argument (`*f(*g(...))`); `scalar(*f(...))` for a one-row, one-column
      call.
    - **The statement is the select over the TVF.** With the clause gone, a
      spec reaches a statement only through `select *rollup(...)`, so the
      unsigned spread lowers where it is hydrated (`_spread_grouping`,
      `rowset_rules.py`) instead of at every statement plan's finalize: the
      arm (with any HAVING and the ORDER BY it sorts on) is finalized, lowered
      to the TVF, and the statement is the select over its handles, each
      labelled with its arm column through `SelectStatement.labels`, the
      contract a signed spread and `*union(...)` already use. Deleted:
      `lower_statement_grouping` and its hook in `finalize_select_tree`,
      `SelectStatement.relational` / `planned` / `relational_names`, and the
      authored-versus-planned pair in `_process_query`. A grain-less arm (no
      keys, nothing to infer) stays the statement, as before.
    - **Fourth slice: the group owns its grouping sets (step (b), ownership
      half).** Below the planner, nothing reads an aggregate's mode any more.
      `BuildGroupingSets` (mode, `by`, sets) is decided once, where a
      `GroupNode` resolves to a real GROUP over the rollup aggregates it
      computes itself (`computed_grouping_sets`), and rides
      `QueryDatasource.grouping_sets` (identity: a ROLLUP of a group is more
      rows) and `CTE.grouping_sets`. The renderer reads the CTE's spec, where
      it re-derived the mode from every locally computed output on every CTE
      (`_get_aggregate_grouping`, with its three mixed-mode raises, and
      `_grouping_has_empty_set` are gone). The group's rolled keys are marked
      nullable off the same spec. Five readers moved off aggregate lineage onto
      it: `CollapseSingleParent` (an AGGREGATE fold is refused for a CTE that
      runs the pass, and `renders_grain_matched_aggregate` excludes it;
      `has_nonstandard_aggregate_grouping` deleted), `SimplifyNullSafeJoins`
      (`_rollup_injects_null`), `UpgradeOuterFromKeySetEquivalence`
      (`_emits_grouping_set_rows`: the pass, or a parent's rows carried without
      a regroup), and `MergeNode`'s no-regroup-above-a-pass rule (reads its
      parents' datasources). A fold that moves the child's aggregates onto
      its parent moves the pass with them (`carry_child_state`): the fuzzer's
      `rollup_over_rowset_extra_dim` lost its ROLLUP under
      `MergeIrrelevantGroupBy` until it did, where the old mode rode along on
      the aggregates. Instrumented at render over the rollup-touching suite,
      `tests/optimization` and the fuzzer, the CTE's spec agrees with the old
      aggregate scan on every rendered CTE; the one reader that disagreed (a
      passthrough of the pass's rows, 8 tests) is the parent walk above.
      `test_rollup_grouping_sets_owner.py` pins the owner, the render (red on
      `main`: clearing the CTE's spec still rendered GROUPING SETS) and the
      fold. Suite SQL capture (`sqlcap`, `PYTHONHASHSEED=0`, laws included,
      53,500 tests), each moved test re-run serially on both sides: one plan
      moved, in 10 `test_duckdb_rollup_tvf_laws` reads (a flagless read of a
      body with a `grouping()` label). The AGGREGATE-fold refusal looked
      through a rowset handle at its content's mode, so the dedup group above
      the rename (no pass of its own) was refused the fold on `main`; read off
      the CTE it folds, and the label renders in the outer select instead of
      inside the ROLLUP CTE, same CTE count, rows held by the law oracle. The
      rest is xdist CTE naming, column order and temp paths. Corpus 0 moved;
      the suite fails the same 25 tests as `main`.
    - **Measured, not landed: the arm planned standard.** The other half of
      step (b) was prototyped: the arm's factory without the spec (its
      aggregates and flags standard), and `gen_rollup_select` stamping the
      spec on the first `GroupNode` of the arm's single-parent chain. Both law
      suites pass (240/240), and so do 5,276 of the 5,419 tests that touch a
      rollup. The 143 failures are the two things the planner's mode still
      buys, and both are step (a)'s, not (b)'s:
      - *Post-aggregate columns planned beside the pass* (windows, a scalar
        over two measures, a label over `grouping()`;
        `test_rollup_multi_window`, `test_duckdb_rollup_window_inline_aggregate`,
        `test_duckdb_rollup_rename_backjoin`). Standard planning computes two
        windows in sibling branches and joins them back on the visible keys,
        which a rollup repeats. Written over the TVF's handles instead, with
        the flags in the relation's identity, the same windows plan correctly
        today: `rank() over (partition by r.gc + r.gl ...)` over
        `with r as rollup((select category, cls, sum(amount) as total,
        grouping(category) as gc, grouping(cls) as gl), category, cls)`
        reproduces `test_rollup_multi_window`'s oracle row for row. So (a) is a
        parse-level rewrite (lift each post-aggregate column out of the arm,
        re-pointed at the handles, minting hidden arm columns for inline
        aggregates), not planner work.
      - *Several facts under one pass* (`test_derived_key_domain`'s
        two-facts and region cases, `test_duckdb_rollup_region_domain`).
        Standard planning groups each fact on its own and joins the groups;
        a rollup needs them in one pass over the joined stream, which is what
        `_partition_grouped_aggregates` does for a mode today. After (a) that
        rule is all the planner needs from the arm: a select-level "one pass"
        flag in place of the per-aggregate mode, which is what (c) deletes.
    - **Fifth slice: the arm is the pass (step (a), landed).** Every grouping
      call lowers through `lower_grouping_arm` (`rowset_rules.py`): named,
      inline-signed, spread and subquery alike. `rollup_lift.split_arm`
      keeps in the arm only the pass: keys, aggregates and grouping flags.
      A window, a BASIC column that reads an aggregate, the HAVING, the ORDER
      BY and the LIMIT move to a select over the pure arm's relation
      (`outer_select`), re-pointed at its handles. An aggregate written
      inline in a lifted column (`order by sum(w)`, `grouping(g1) = 1` inside
      a CASE) becomes a hidden arm column, and so does a named one the arm
      does not show. Each output is labelled with the arm column it shows
      as, so the statement, a named rowset's handles and a signature read
      the authored names. A column reading a row-level value no handle
      carries leaves the arm as it was. Three facts carry the arm's meaning
      across:
      - A rank of a tuple (`rank(g1, g2) over ...`) ranks the relation's
        rows: its tuple gains the grouping flags, since the visible keys do
        not tell a subtotal from a NULL key (`_ranked_by_identity`).
      - The outer select reads every handle, hidden ones included, so a read
        of the relation never dedups its identity.
      - HAVING stays the outer select's HAVING, which filters after the
        window as a plain select's does. On a plain select
        `rank() ... having total < 25` ranks first, and the arm did the
        same; a WHERE would have re-ranked.
      - **Fixed on the way (`main` bug):** a named rollup read through its
        handles (`with r as rollup((select g1, total, rank() over (order by
        total desc) as rk, total * 2 as dbl), g1) -> (a, t, k, d)`) planned
        the window beside the pass and joined it back on the visible `g1`.
        The subtotal NULL and the data NULL fanned out: 6 rows, ranks 1 and
        3 for the same row. It is now 4 rows, `test_rollup_arm_lift.py`
        (red on `main`, with the pure-arm pin).
      - **Render.** A labelled output that resolves to a handle renders
        through the handle when the parent also lists the arm column as its
        own (`_parent_has_own` in `sort_select_output_processed`); the
        parent never projected that name.
      - **Gates, against `c996f28`:**
        - Corpus (208 statements, tpc-ds aggregates included): 1 moved.
          q67 has the same 3 CTEs and joins under longer handle aliases
          (3,424 to 4,385 characters; its size budget is rebaselined), and
          its rows are checked by `test_sixty_seven`.
        - Rollup suites (both law files, 379 tests): green.
        - Full suite: the same credential-bound failures as `main`, plus
          `the_look`'s known one.
        - Fuzzer 522/522.
        - Two test edits. `test_grouping_membership_in_having_routes_to_having_not_where`
          no longer asserts that the text has no WHERE: the outer HAVING
          renders as a WHERE over the materialized flags, after
          aggregation, and DuckDB executing it is the check. q67's budget
          log is rebaselined.
      - **What it buys (b).** The arm planned standard failed 143 tests in
        the prototype, on two shapes. One was post-aggregate columns
        planned beside the pass; this slice moves every such column out of
        the arm. The other, several facts under one pass, still needs the
        select-level one-pass flag. Step (b) is now that flag plus the
        prototype.
    - **Sixth slice: the arm is planned standard, the pass is the select's
      (step (b), and the mode half of (c)).** No aggregate carries a
      grouping mode any more: `BuildAggregateWrapper.grouping` and
      `.grouping_sets` are deleted. The factory still keys a bare aggregate of
      a spec'd select on the spec's keys, in the spec's order, and skips the
      FD reduction of that `by` (a rolled key is a subtotal level), but it
      writes no mode. The pass is a fact of the arm's planning scope:
      `gen_rollup_select` sets `BuildEnvironment.grouping` (a
      `BuildGroupingSets`) on the arm's build environment, keyed by the `by`
      the factory gave a bare aggregate, and `BuildEnvironment.pass_of(c)` is
      the one question every reader asks: an aggregate at the pass's keys is
      one of its outputs (`BuildGroupingSets.computes`).
      - **The one-pass flag** is that environment field. The concept graph
        reads a node's grouping mode off it, so `_partition_grouped_aggregates`
        still co-sources every member in one pass (the flags beside the
        measures, two facts joined below the pass), and `GroupNode` takes the
        pass itself when it computes a member (`GroupNode.grouping_pass`,
        never skipped as a grain match). Nothing stamps a node after the
        plan.
      - **Readers moved off the aggregate's lineage**, each onto the
        environment or the datasource that owns the pass:
        `null_provenance.rollup_padded_addresses` (the datasource's
        `grouping_sets`), `rollup_padded_keys`, `reads_a_rollup`,
        `join_plan`'s pinned-grain walk, `region_reads`, the axis-widening
        `_grouping_pass_sibling_axis_members`, `gen_aggregate` and
        `_renders_nonstandard_grouping`, `datasource_nodes`' forced group,
        and the strategy builder's `node_nulls_grouping_keys`,
        `_groups_null_grouping_keys`, `_nonstandard_grouping_key_addresses`,
        `_aggregate_scalar_inputs`, `_parents_already_at_input_grain` and
        `_renders_grouping_rows`. Deleted: `nonstandard_grouping_spec`,
        `nonstandard_grouping_lineage`, `computed_grouping_sets` and the
        mode term of `_concept_is_relevant` (a `grouping()` flag only exists
        under a spec). `_standard_rollable` and the scope diagnostics' rollup
        label lost their mode term: the statement-level partial election
        never sees an arm aggregate behind the TVF boundary.
      - **One semantic change, pinned.** An aggregate pinned at exactly the
        pass's keys (`sum(v) by g1` in `rollup(..., g1)`) is now a member of
        the pass. On `main` it was its own standard group joined back
        null-safe on `g1`, so the grand-total row read the data-NULL group's
        value (7 where the pass says 42): the collision the TVF exists to
        prevent. An aggregate at another grain (`max(v) by id`) stays its own
        group, as before.
      - **Gates, against `c1647f9`:** corpus (204 statements) 0 moved. The
        rollup-touching files (109 test files, 12,450 tests, both law files
        included): the same 18 credential-bound failures as `main`. The two
        law files: green. Full suite, laws included (53,100 passed): the
        credential-bound failures and `the_look`'s known one, as on `main`.
        Fuzzer 522/522.
        `test_rollup_arm_standard.py` pins the mode-free wrapper and the
        pinned-at-keys rows (both red on `main`), two arms of one statement
        each running their own pass, and an aggregate at another grain.
      - **What is left of (c).** The planner-side readers above are still
        live: they now ask the select's pass instead of each aggregate's
        mode, so the "about a hundred sites" are about twenty-five, each a
        one-line `pass_of`. Whether each is still needed with the arm
        isolated behind its TVF is an ablation per site (the
        `rollup_padding` join rule, `rollup_padded_keys` in the demand pass,
        `node_nulls_grouping_keys`' FINAL exemption, `_fold_rollup_key_dims`,
        the `grp:` bucket segment), not a refactor. The author-side
        `AggregateWrapper.grouping` survives only as the factory's marker that
        a bare aggregate took the spec's keys.
    - **Seventh slice: the readers ablated (the rest of (c)).** Each of
      the 28 planner sites that asked the pass was switched off alone against
      the rollup-named test files (34 files, 4,110 tests). The survivors were
      then switched off together against every rollup-touching file (111
      files, 12,500 tests), the corpus, the fuzzer and a suite-wide SQL
      capture (serial re-runs on both sides, CTE names normalized). A site
      that moved a row or a plan stays. 14 are deleted (about 125 lines net):
      - The keyless-join guard's padded-axis exclusion, and its unit pin.
      - The spine skip over a pass parent (`group_graph`).
      - `node_nulls_grouping_keys` in the row-stream dedup.
      - `_aggregate_scalar_inputs`' pass term.
      - In `condition_placement`: the uncovered-contributor skip, and the
        pass exemption in `accumulated_atoms_above`. A unit row of
        `test_v4_final_extra_sides` built that graph by hand and is dropped.
      - `rows_above_a_rollup` and the pass term of `region_reads`' `one_pass`.
      - `_grouping_pass_sibling_axis_members`: a flag now takes no axis
        widening at all.
      - The pass skip in `join_plan`'s pinned-grain walk.
      - `_renders_nonstandard_grouping` and the forced group in
        `gen_aggregate` and in `datasource_nodes`. `GroupNode.grouping_pass`
        already refuses the grain-match skip.
      - The `grp:` bucket-id segment: `partition_aggregates` splits a pass's
        members from standard ones before any bucket exists.

      Suite SQL: the 14 together move no plan. Corpus 0 moved, fuzzer
      522/522, and the rollup files fail only the credential-bound tests
      `main` fails.

      **Kept for rows**, each with the test that fails without it:
      - `_null_alike`'s rollup term:
        `test_grouping_set_join_not_narrowed::test_rollup_grand_total_survives_a_subset_join_back`.
      - The `rollup_padding` join rule and `_one_read`'s pad check:
        `test_duckdb_fuzzer_regressions::test_rollup_label_over_union_joined_rowsets`.
      - `_groups_null_grouping_keys`:
        `test_rollup_extra_leaf_dim_dedups_pairs` and `..._with_grouping_sets`.
      - `_nonstandard_grouping_key_addresses`:
        `test_duckdb::test_grouping_over_aliased_rollup_key_builds`.
      - `region_domains`' `one_pass`:
        `test_derived_key_domain::test_rollup_by_an_absent_key_counts_the_region`
        and `test_scalar_folded_into_pass_reads_its_rewritten_aggregate`.
      - `_renders_grouping_rows`: 7 `test_duckdb_rollup_rename_backjoin` tests.

      **Kept for plan size.** Rows hold without each of these; the plans
      grow.
      - `node_nulls_grouping_keys`' FINAL exemption: q36, q70 and q86 gain a
        regroup above the pass that re-projects every flag (corpus).
      - The demand pass's padded keys (`rollup_padded_keys`), including the
        rollup-key PADDED region domain: 18 suite plans larger, none
        smaller.
      - `condition_placement`'s pass host term: 4 plans larger.
      - `_parents_already_at_input_grain`'s pass term: 2 plans gain a CTE.
        The rest only reorder.
      - `_fold_rollup_key_dims`: 4 plans gain a CTE and a join
        (`test_rollup_sibling_column_join_preserves_subtotals`, two
        subset-join rollups, a ranked grouping partition). 10 rollup law
        reads get 32 characters shorter, with a scalar over a key computed
        above the pass. Narrowing the fold to dims that a later read
        cannot compute above the pass is the open half.

      **Not ablated:** the one-pass flag itself (the concept graph's mode,
      `partition_aggregates`, `GroupNode.grouping_pass`), and
      `null_provenance.rollup_padded_addresses`, which feeds the kept join
      readers. They own the pass. `reads_a_rollup` was not measured.

11. **One owner for a filter's rendered form (handoff 2.3(b), first
    slice, landed).** A filter virtual renders three ways: a per-row CASE,
    bare content when the CASE is redundant, or MAX(...) when a grouping
    CTE's grain holds its keys but not its predicate (the collapse). The
    redundant-CASE test was written three times: the renderer
    (`_render_concept_sql`, own WHERE or `_filter_guaranteed_by_parents`),
    the collapse and `_has_local_aggregate` (own WHERE only). It is one
    function now, `execute.filter_case_elided`, and the dialect's copy is
    gone.
    - **The disagreement was live.** A CTE whose parent's HAVING guarantees
      a filtered key's predicate rendered it bare but still collapsed it:
      `max("p"."city") ... GROUP BY "p"."city"`. Now it stays a group key.
      Suite capture (`PYTHONHASHSEED=0`, 53,734 tests, re-run serially
      against base to clear xdist noise): one plan moved,
      `test_oracle_windows_laws[255]` base and summary, 18 characters
      smaller, rows checked by the oracle.
      `test_filter_guaranteed_by_parent_stays_group_key.py` pins it (red on
      `main`).
    - **The predicate arm of `_has_local_aggregate` is deleted.** It counted
      a filter as aggregate-bearing when its un-elided CASE read an
      aggregate computed in the CTE. A census over the whole suite (53,321
      tests) recorded no firing: the planner materializes a filter's
      predicate inputs in a parent, so the aggregate is never computed
      where the CASE renders.
    - **The rest is pinned.** Each site got an off switch, run alone under a
      SQL capture against the tests it fires in (357 for the collapse, 3,524
      for the elision; noise floor from a second baseline run, residual moves
      re-run serially). Every switch fails tests:

      | site | tests failing with it off |
      |---|---|
      | keyless one-row collapse | 2, `test_outer_where_pushes_into_global_agg` |
      | collapse of a parent-computed filter | 2, `test_rule_soundness_laws::test_filtered_value_collapses_to_its_key` |
      | collapse of a filter computed here | 10, q16, q95, `test_filter_concept_is_a_value` |
      | an atom on a collapsed filter is HAVING | 19, filters oracle laws and sweep |
      | aggregate over a collapse is its one-row formula | 16, q78, q95, `test_filtered_count_at_regroup_grain` |
      | a parent guarantees the predicate | 5, q16, q94, q95, `test_aggregate_filter_uses_having` |

    - **Measured, not landed: the collapse stamped at planning.** The
      collapse is a fact about a (concept, CTE) pair, read most often for a
      concept that is only an aggregate's argument in a CTE that does not
      output it, and often only after `CollapseSingleParent` folds the
      per-row CTE into the group. A birth-time stamp of each CTE's outputs
      missed every firing in the sampled test files. The planner-owned form
      remains the handoff's lineage substitution.

    Gates, against `7935105`: corpus (204 statements) 0 moved; fuzzer
    522/522; suite, laws included, fails the same 36 environment tests
    (BigQuery and Snowflake credentials) as `main`.

12. **One owner for a zero-filled COUNT (landed; the handoff's "wrong
    stage" COUNT to `coalesce(..., 0)`).** A COUNT renders `coalesce(n, 0)`
    where a join pads it, since a count over no rows is 0. The merge stamped
    the region-padded half (`MergeNode._zero_filled_counts`), but the CTE
    re-derived the rest at render from its own nullable list, grouping flag
    and rollup list, and the renderer, predicate pushdown and join upgrade
    each asked it. `execute.zero_filled_counts` is now the one rule:
    `QueryDatasource` applies it at construction, and `CTE.zero_fills_count`
    / `zero_filled_counts` only read the result. The merge's stamp is kept
    apart as `region_zero_filled`.
    - **The CTE's half was most of it.** A census over the suite (53,700
      tests): 934 decisions in 42 test functions came from the merge's
      stamp, 1,825 in 47 from a padded non-grouping CTE, and 468 in 4 from a
      re-summed partial in a grouping CTE.
    - **A plan-time stamp drifts unless the grouping flips re-decide it.**
      Four optimizer sites change `group_to_grain` after planning:
      `CollapseSingleParent`'s aggregate merge, an inline with `force_group`,
      `DropIdentityGroup`, and the existence-HAVING fold. Stamping without
      them moved 71 plans (57 gained a redundant coalesce; 14 lost a pushdown
      that reads the stamp). They now call `CTE.regroup`, which applies the
      same rule to the new shape. A flip to grouping keeps only the region
      stamp, because its counts are read from a padded parent. Dropping that
      too failed 33 region row pins (`test_derived_key_domain`,
      `test_unmodelled_regions`, `test_duckdb_rowset_null_group_rejoin`,
      `test_outer_padding_null_not_a_value`).
    - **Either spelling may be the COUNT.** A nullable entry and an output
      can share an address with only one of them a COUNT: a rowset handle
      over the count, or a count only the WHERE reads. Asking the output
      alone failed the same 33 pins. The rule accepts either.
    - **One plan moved.** In `test_materialization_invariance` (statement-wide
      gate over `*rollup(...)`), `n` is now coalesced in the final select.
      The arm's count is nullable only because the group marks values derived
      from a rollup key nullable. Before, the renderer asked about the
      statement's spelling, which is not a COUNT. The answer is the same: a
      count is never NULL there, and the twin-row assertion passes.
    - `tests/test_zero_filled_count_owner.py` pins the ownership (red on
      `main`: there the CTE re-derived the answer from its own nullable list).

    Gates, against `7ddf99c`: corpus (206 statements, tpc_ds aggregates
    included) 0 moved; suite SQL capture under `PYTHONHASHSEED=0`, with the
    moved tests re-run serially on both sides, 1 statement moved, 0 outcomes
    changed; fuzzer 522/522; suite fails the same 36 environment tests as
    `main`. `sqldiff.py` now normalizes `MATERIALIZED` CTE names and the
    checkout path; before, both read as moves.

13. **One owner for a coalesced key read (landed; the handoff's last three
    "wrong stage" COALESCEs).** A column reads one parent column, or a
    coalesce of several: every member of an outer-join key class (any side
    may be the padded NULL), or every in-scope source of a multi-source
    entry unless an INNER join equates them on the key. That rule was
    written four times, and only the renderer's copy was whole:
    `safe_get_cte_value` (key class over in-scope sources, then the
    FROM-scope filter and the INNER collapse), `CTE.render_binding` (key
    class and raw sources, which decides GROUP BY dedup),
    `filtered_aggregate._reads_source` (key class and raw sources) and
    `join_upgrade._renders_exclusively_from` (raw sources only). It is one
    method now, `CTE.column_reads`, and the four read it. The join ON clause
    had a second grouping rule of its own: an outer join coalesced every
    left side of one right key, and anything else went through
    `coalesced_key_groups`, which the optimizer's join proofs already read.
    - **The ON clause's outer branch was redundant.** A census over the
      suite (53,600 tests): it fired 884 times in 86 test functions, and
      `coalesced_key_groups` put the same pairs in one group every time.
      It is deleted; the renderer, `inner_join_key_sources` and the join
      upgrade's proofs read the one grouping.
    - **The disagreement was a soundness hole, not a plan move.**
      `_renders_exclusively_from` let a consumer's rejection of NULL on an
      outer-join key class member prove its producer non-null, though the
      consumer renders the key `coalesce(a.k, b.r)`, which masks a padded
      NULL. No suite shape reaches it;
      `test_execute_models.py::test_a_coalesced_key_carries_no_rejection_to_one_producer`
      pins it (red on `main`).
    - **Not moved: the `left_local` pin in `_render_left_concept`.** It
      pins an ON key to the left base's own column when the generic render
      might read another source. Asking `column_reads` instead of the raw
      source count failed 15 law tests: the INNER collapse picks the
      lexically first source, which can be the join's own right side, and
      the key renders `"c"."k" = "c"."k"`. It keeps the raw count.

    Gates, against `0450ee9`: corpus (204 statements) 0 moved; fuzzer
    522/522; suite SQL
    capture under `PYTHONHASHSEED=0`, the 1,285 statements that moved under
    xdist re-run serially on both sides: 40 moved against a base-to-base
    floor of 37, the 5 left over being assertion column order and an
    environment skip, so 0 plans moved; suite outcomes match `main` but for
    `test_rank_beside_partial_merge::test_a_rank_and_its_total_need_their_own_node`,
    an xdist ordering failure that passes serially on both sides.

14. **The planner narrows on its own WHERE (landed; the planned half of
    handoff 2.1 step 4).** `UpgradeJoinOnGuards` flips outer joins on three
    proof channels: the CTE's own condition as the planner built it, a
    condition an optimizer phase pushed there, and a consumer's rejection
    (`_external_forced_map`). `local_scripts/sql_ab/guard_census_plugin.py`
    decides every flip again without the consumer proofs and on the CTE's
    planned condition, and names the weakest channel that still flips it.
    Over the suite (53,600 tests) the base made 1,539 flips in 760 tests:
    475 on the planned condition alone (335 in the early pass before
    `UnionDimPushdown`), 851 on pushed conditions, 204 on consumers. The
    planner had every input of the 475. It missed them three ways:
    - **A FULL ignored `IS NOT NULL`.** Its proofs are the conservative
      harvest, since a merged key renders as a cross-side coalesce. A
      column no other dataset of the merge holds renders off its side
      alone, so the full harvest proves that side through it
      (`_exclusive_addresses`; a column a later join's side shares is
      excluded, since it renders coalesced).
    - **The padded left side was the ON clause's sources.** A RIGHT/FULL
      pads the whole left-deep stream, so a WHERE on a dimension joined
      INNER before it (`date.year` beside q75's FULL to returns) proves the
      left present. `_stream_sources` reads the stream, through exclusive
      columns only.
    - **A later INNER equality was not a proof.** A NULL key matches nothing,
      so a padded side whose key a later plain INNER reads keeps no padded
      row (q91: customer LEFT, then an INNER to the address on the
      customer's `address_sk`). `narrow_joins_read_by_inner`, to a fixpoint;
      a coalesced left, a null-safe pair and an opaque binding prove
      nothing, as in the optimizer's `_add_inner_join_key_proofs`.
    - **Ordering matters.** The INNER rule may move a FULL to LEFT or RIGHT
      only in the first narrowing step. After `compute_outer_null_status`
      orders the sources, a FULL that changed which side it preserves left
      a `union join` key reading the padded side
      (`test_union_join_anchor_where_precedes_the_rowset_pairing`, two rows
      lost). The second step narrows to INNER only, and those FULLs stay the
      optimizer's.

    After: 1,080 flips in 535 tests, 12 on the planned condition (5 of them
    the `union join` FULLs above). The optimizer rule stays: the pushed and
    consumer channels are the open half, and they need the pushed conditions
    visible to the planner.
    `tests/modeling/tpc_ds_duckdb/test_planner_owned_join_narrowing.py` pins
    the FULL and the INNER chain at plan time (both red on `main`).

    Gates, against `c7e5727`: corpus (206 statements) 5 moved, rows checked
    by their TPC-DS tests. q30, q30-alt and q78 shrink (an INNER at plan
    time leaves no stale null-safe pair). q75 and q80 push an item filter
    into each sales arm, because the item join is INNER before
    `UnionDimPushdown` runs. On sf=1 the rows are identical, q75 runs in
    77ms against 98ms, and q80 is unchanged; q75's log is rebaselined
    (7,043 to 7,498 characters). Suite SQL capture under `PYTHONHASHSEED=0`
    and xdist: 1,074 statements moved, 1,050 of them the same length
    (column and GROUP BY order); 3,582 characters fewer in all, 7 more
    joins. Of the 8 that grew, q75, q80 and two `test_union_dim_pushdown`
    tests are the arm pushdown above, two scoped window joins are a FULL
    narrowed to RIGHT, and two merge-bridge reads name the merged key's
    twin; each asserts rows. Fuzzer 522/522. Suite: the same
    credential-bound failures as `main`, and the xdist-order
    `test_rank_beside_partial_merge` failure that passes serially.

15. **The planner narrows on its consumers' filters (landed; the pushed
    and consumer channels of handoff 2.1 step 4, first slice).** Seam 14
    left 1,059 `UpgradeJoinOnGuards` flips over the suite: 863 on a
    condition an optimizer phase pushed into the CTE, 175 on a consumer's
    rejection (`_external_forced_map`), 12 planned and 9 in CTEs the
    optimizer built. The pushed and consumer channels are one fact the
    planner already holds: a WHERE or HAVING on a node above the merge (the
    HAVING wrap, a staged filter's select, a parent merge's own WHERE).
    `consumer_proofs.narrow_for_consumers` runs once `_plan_query_node` has
    the statement's tree. It walks it consumers first and hands each
    outer-joining merge what all of its consumers reject, on columns read off
    that merge alone. The merge reads that into the same harvests as its own
    WHERE (`MergeNode.consumer_proofs`), and the plan above it is resolved
    again.
    - **What passes a node.** Rows pass a plain select, a merge and a group
      that is not a grouping-set pass unchanged; a window, unnest, limit,
      union or rowset boundary stops the proof. Through a group only a key
      passes: a rejected key's group holds only rows with that key NULL,
      but dropping a padded row changes the aggregates of the group it
      joined. A count the node zero-fills is 0 on a padded row, and a
      partial aggregate a regroup rolls up is not a row value, so neither
      passes, and a merge with a LIMIT takes nothing from above it.
    - **One relation, every consumer.** Nodes are keyed by the relation
      they resolve to (`QueryDatasource.identifier`), which does not
      include join types: two nodes that render as one CTE take only what
      all of their consumers reject. In `test_relation_rollup_laws`
      (332 of the flips left) the narrowed join is in a CTE also read under
      an unnest, so it stays the optimizer's.
    - **Two stamps the narrowing must not move.** A merge's partial
      stamp: the rows it drops are rows every consumer drops, so an
      address it bound completely before stays complete
      (`MergeNode.unfiltered_partials`). Without that, the unnest filter
      laws re-joined the tree table above the narrowed merge (one more CTE
      and join each). Nullability: a node keeps what its first resolve
      found nullable (`StrategyNode.resolve`), so the narrowed merge and
      each reader drop the padding their parents no longer report before
      they are resolved again. Without that, a reader kept an
      `IS NOT NULL` and a null-safe key the optimizer used to strip
      (`test_window_expression_join`).

    After: 608 flips in 298 tests (pushed 433, consumer 157, planned 12,
    new CTE 6). q46's FULL to the bought rowset is INNER at plan time, q77's
    three FULLs to returns are LEFT and q44's FULL is RIGHT
    (`tests/modeling/tpc_ds_duckdb/test_planner_owned_join_narrowing.py`,
    red on `main`). Open: the consumer channel's flips carry no condition
    on the flipped CTE, so their proof is a consumer's join (likely an
    INNER equality on the merge's key, unmeasured), and a relation shared
    with a stopped reader.

    Gates, against `649ebd0`: corpus (206 statements) 0 moved; fuzzer
    522/522; suite SQL capture under `PYTHONHASHSEED=0` and xdist: 991
    statements moved, 966 of them the same length, 2,694 characters fewer
    in all, joins and CTEs unchanged. The 13 that grew do so by 1 to 6
    characters: coalesce operand order, a merged key read off its twin
    column once the join is INNER, and one FULL rendered LEFT
    (`test_membership_set_is_no_side_of_the_merge_hosting_it`); each
    asserts rows. Suite: the same credential-bound failures as `main`, and
    the xdist-order `test_rank_beside_partial_merge` failure that passes
    serially.

16. **The planner narrows on its consumers' joins (landed; the consumer
    channel of handoff 2.1 step 4, second slice).** Seam 15 left 157
    consumer-channel flips that carry no condition on the flipped CTE: the
    proof is a consumer's join equality (`_inner_pair_rejections`). A NULL
    never matches `=`, so a parent row NULL on a column a consumer joins on,
    on a side the join does not preserve (both of an INNER, the right of a
    LEFT, the left of a RIGHT), reaches nothing above it.
    `consumer_proofs.join_rejections` reads that off each consumer's
    resolved joins (null-safe pairs, a coalesced left and opaque bindings
    excluded) into `ConsumerProofs.keys`, which passes up a node as the
    filter proofs do.
    - **A partial binding still proves its side.** The merge's own WHERE
      cannot force a side through a column bound partially there
      (`_unprovable_addresses`). A consumer's join can: a column only the
      padded side holds is NULL on every padded row, whatever values the
      side lacks. `JoinProofs.key_proofs` forces a side through a column
      it alone renders (`grain_utility._keys_force`); without that, the
      flights' `~carrier_code` in `test_dimension_chain_oracle_laws` kept
      every one of these flips the optimizer's. A join key, or a pseudonym
      another dataset holds, renders coalesced with the other side and is
      not NULL on a padded row: a `subset join cust = members.mid` merge
      narrowed on its reader's INNER on `cust` lost the member with no sale
      (`test_explicit_is_not_null_matches_directional`).
    - **A narrowed merge rejects more.** A merge's own joins are read after
      it takes its consumers' proofs and is re-resolved, so a RIGHT it
      narrows to INNER hands its parent the rejection the INNER makes.
    - **A consumer's join type is not fixed.** Re-resolved over a narrowed
      parent, a merge may preserve the side it dropped before: in tpc_h
      adhoc07 the order lookup's narrowing left the summed side alone
      padded, and the root merge it was keyed on went from INNER to RIGHT,
      keeping the NULL order row its INNER had dropped. So the plan the
      join channel built is checked (`_keys_hold`): when any narrowed
      merge's consumers no longer reject its keys, the plan is restored and
      narrowed on the filter channel alone. Over the 600 dimension-chain
      laws the check fails in 6 of 583 compiles.
    - **A regroup by its own key folds.** A narrowed merge can leave a
      `group(x) by k` over rows already at `k` as a projection, its own
      CTE. `rename_reference` now reads any GROUP's content as the rename
      (the bare render is still confirmed by `consumed_parent_column`), so
      `CollapseSingleParent` folds it into the merge it reads.

    The unnest shape seam 15 left (332 flips in `test_relation_rollup_laws`)
    is not a row proof the planner can make. Its flips come from
    `PredicatePushdown` pushing the root's `ecoregion_id = 1` into the
    unnest CTE as a filter on the element, then from there into the parent
    CTE's own `ecoregion_id` column: under `merge ... into` the element and
    the parent's column are one address. No consumer rejects the parent's
    padded rows as rows: the merge above reads the key coalesced, and the
    unnest's consumer reads it as its padded side.

    After: 529 flips in 263 tests (pushed 431, consumer 77, planned 12, new
    CTE 9), against 597 on `b85e285` measured the same way (seam 15's 608
    was another machine's run). `test_cluster_key_lookup` has none left and
    `test_dimension_chain_oracle_laws` 46 of 78; the consumer flips left are
    shapes the check above rejects, a key that renders coalesced, and
    `test_semi_join_pushdown`'s CTEs the optimizer builds.
    `tests/discovery/test_consumer_join_narrowing.py` pins the partial
    column's narrowing and the regroup's fold (both red on `main`).

    Gates, against `b85e285`: corpus (204 statements) 1 moved, q18 (4 CTEs
    to 3, the regroup folded; rows checked, log rebaselined). Fuzzer
    522/522. Suite SQL capture under `PYTHONHASHSEED=0` and xdist, leaving
    out `tests/scripts` (statement counts vary run to run) and the tests
    that errored in the base run: 2,330 statements moved, 1,557 the same
    length, 23,901 characters, 122 CTEs and 9 joins fewer. Of the 43 that
    grew, 34 are `test_dashboard_queries` batch compiles the base run
    captured twice (each statement is a character shorter); the others are
    oracle laws and `test_cluster_key_lookup` reading a column off a
    narrowed side (1 to 40 characters), and
    `test_group_to_always_splits`, where `SemiJoinPushdown` now restricts
    the population twin to the keys its INNER reader keeps (whole groups
    only). Each asserts rows. Suite: the same credential-bound failures as
    `main`.

17. **The planner narrows on a consumer's join through a key it reads off
    one side (landed; the consumer channel of handoff 2.1 step 4, third
    slice).** Seam 16 left 77 consumer flips, 46 of them in
    `test_dimension_chain_oracle_laws`. `_keys_force` refused every join
    key, on the ground that a key renders coalesced with the other side.
    It does only where the merge reads it off more than one source: with
    the flights binding `~tail_num` and the aircraft binding it complete,
    `resolve_concept_map` never reads the partial binding, so the merge
    renders `tail_num` off the aircraft alone. A padded aircraft row is
    NULL there, and the aircraft lookup above, which joins on `tail_num`,
    drops it.
    - **What the merge read, when it took the proofs.**
      `consumer_proofs.single_source_reads` records, as a merge takes its
      consumers' proofs, the columns its resolve read off one source
      (`MergeNode.key_reads`). An outer key paired with another address is
      left out, since `CTE.outer_join_key_class` coalesces the pair. The
      record is of the resolve the consumers' joins were read against, so
      the rows it names are the rows they reject. `JoinProofs.key_sources`
      carries it, and `_keys_force` forces the source a rejected key was
      read off, join key or not.
    - **Measured, not landed: plans whose nodes share a relation.**
      `_consumers_first` gives up when a node reads another node of its
      own relation (a joinless merge over the regroup it passes through):
      521 of 38,877 compiles over discovery, TPC and the join matrix.
      Skipping those edges lets the walk run. In dimension-chain seed 258
      the root merge, re-resolved over the narrowed seats merge, then
      widened its INNER on `id` to a RIGHT; it still rejects the narrowed
      side, so `_keys_hold` passed it. The narrowed merge's extent-free
      partial stamp also moved, which made a passthrough a domain join (one
      more CTE) until it was masked like the joined partials. A check that
      no source of any node becomes preserved catches the widening, but
      also rejects the FULL+LEFT to FULL+RIGHT side flip seam 16 shipped in
      `test_cluster_key_lookup` (its 50 flips went back to the optimizer).
      All three were backed out; taking these plans needs a test that tells
      a widening from a side flip.

    After: 504 flips in 253 tests (pushed 434, consumer 49, planned 12, new
    CTE 9) against 515 in 258 on `a154c09` on the same machine (pushed 426,
    consumer 77, planned 11, new CTE 1). The pushed and new-CTE rises are
    not plan changes: tpc_ds `test_queries` compiles some queries a varying
    number of times (its SQL is unchanged), and `test_join_upgrade` runs
    the rule on hand-built CTEs whose channel depends on the test before
    it on the worker. Counted once per (test, CTE, join), flips fell from
    304 to 284 and consumer flips from 72 to 44. Dimension chain is at 22
    (46 before) and tpc_h adhoc07 at 0 (6).
    `tests/discovery/test_consumer_join_narrowing.py` pins the key read off
    one side (red on `main`).

    Gates, against `a154c09`: corpus (208 statements) 0 moved; fuzzer
    522/522. Suite SQL capture under `PYTHONHASHSEED=0` and xdist moved 848
    tests, almost all column order; recaptured serially in both trees, 6
    move. Four dimension-chain laws shrink by 5 to 19 characters: a
    reader's null-safe pair on a key the narrowed merge no longer pads
    becomes `=`, or a LEFT the optimizer kept is INNER. Filters seed 133525, base and summary, gains a CTE (8,637 to
    9,704 characters): once a merge's LEFT to a `_wscope` regroup is INNER,
    a HAVING-born `max <= sum` condition stays in a CTE of its own instead
    of being pushed below the regroup. Both assert rows. Suite: the same
    credential-bound failures as `main`.

18. **A WHERE on a grouped merged key is planned on the aggregate's own
    rows (landed; the unnest shape of handoff 2.1 step 4).** Seams 15 and
    16 left 332 pushed flips in `test_relation_rollup_laws`, all one plan:
    `select ecoregion_id, count(tree_id) where ecoregion_id = 1` under
    `merge tree_eco_region into ecoregion_id`. The concept graph's
    `_constrain_aggregates_by_merged_expansions` gives every D0 aggregate
    the unnest as a lineage parent when the WHERE reads the key it is
    merged into, so the WHERE filters the rows the unnest expands. An
    aggregate grouped by that key already reads it per row, and its row
    stream reaches it through the same unnest (trees, the lists, LEFT the
    unnest INNER `eco`). The extra edge planned a second unnest of that
    stream, joined it back LEFT on (key, `tree_id`), read the key as a
    coalesce of both and put the WHERE above. No proof passes that: the
    stream has a consumer under an unnest, and the coalesce reads two
    sources. `PredicatePushdown` then pushed `ecoregion_id = 1` into the
    stream and `UpgradeJoinOnGuards` made its LEFT an INNER.
    - **The fix is the edge.** An aggregate with a lineage edge from the
      merged key takes no edge from its unnest. The WHERE is placed on the
      row stream, the merge narrows its LEFT on its own condition, and the
      self-join is never planned.
    - Every other D0 aggregate keeps the edge: one that does not read the
      key (`select city, count(tree_id) where ecoregion_id = 1`) has no
      other relation between the key and its rows.

    After: 157 flips in 111 tests (pushed 87, consumer 49, planned 12, new
    CTE 9) against 486 in 253 on `5dad39d` on the same machine (pushed
    416). `test_relation_rollup_laws` has none left (332); tpc_ds
    `test_queries` moved 12 to 15, its varying compile count. Distinct
    flips 284 to 142. `test_a_where_on_the_grouped_merged_key_is_planned_on_its_rows`
    pins the plan (24 cases, all red on `main`): no outer join at plan
    time, one unnest.

    Gates, against `5dad39d`: corpus (206 statements, with
    `tpc_ds_duckdb/aggregates`) 0 moved; fuzzer 522/522. Suite SQL capture
    under `PYTHONHASHSEED=0` and xdist, the moved tests recaptured serially
    in both trees: only `test_relation_rollup_laws` moves, 152 statements,
    none larger (628 CTEs to 456, 608 joins to 304, 195,064 characters to
    122,318). Each asserts rows against the summary-free plan. Suite: the
    same credential-bound failures as `main`.

19. **The consumer walk reaches a fixpoint, through filter nodes and a
    stream-kept FULL (landed; handoff 2.1 step 4, consumer and pushed
    channels).** Seam 18 left 157 flips. Three gaps in
    `consumer_proofs.narrow_for_consumers` and the INNER-chain rule held
    53 of them:
    - **A FilterNode passes rows.** `_passes_rows` let a proof through a
      plain select, a merge and a non-grouping-set group, never a
      FilterNode, though a FilterNode's condition is a WHERE over its
      parent's rows exactly like a select's. A HAVING the planner turns
      into a filter virtual (`having ra.n > 0` on a column the select does
      not project, the filter laws' `_wscope` comparisons) sits on one over
      the merge that pads the column, so the optimizer pushed it down and
      flipped the join. The merge now takes it at plan time; the SQL is
      the optimizer's, byte for byte, in the shapes sampled.
    - **Narrowing repeats to a fixpoint.** Consumers are read before the
      merges under them narrow, once. In dimension-chain seed 49 the
      airports' merge narrows on the carrier lookup's join, which leaves
      its `id` unpadded; re-resolved, its INNER on the per-flight seats
      regroup pairs `id` with plain `=` instead of null-safe, which rejects
      the seats merge's FULL padding. The first pass had read that pair
      null-safe. `_narrow` now re-propagates until no merge takes more. A
      later round only adds to what a merge took (each proof held of the
      plan it was read off), keeps the first round's `unfiltered_partials`,
      and `_keys_hold` checks the plan the last round built.
    - **A FULL a later INNER reads may narrow to a LEFT after source
      ordering.** Seam 14 barred `narrow_joins_read_by_inner` from any
      narrowing but INNER once the merge has ordered its sources
      (`inner_only`), since changing which side a FULL preserves left a
      union join key reading the padded side
      (`test_union_join_anchor_where_precedes_the_rowset_pairing`). That
      failure is the RIGHT, which pads the stream the merge put first
      (re-checked: allowing RIGHT fails it again, allowing LEFT does not).
      A LEFT keeps the stream preserved: in dimension-chain seed 362 the
      flights' INNER on the aircraft's `tail_num`, itself narrowed from a
      LEFT by the carrier lookup, drops the models no aircraft has.

    After: 104 flips in 63 tests (pushed 55, consumer 30, planned 10, new
    CTE 9) against 157 in 111 on `af20c84` on the same machine (pushed 87,
    consumer 49, planned 12, new CTE 9). Distinct flips 142 to 80. By
    file: filters laws 14 to 2, windows laws 9 to 3, dimension chain 22 to
    12, `test_merge_union_parity` 8 to 0, `test_existence_feeder_pushdown`
    5 to 0, spellings 6 to 2, tpc_ds `test_queries` 12 distinct to 9 (q10,
    q21 and q22 now planned; its per-run count rose 15 to 24 only as q73
    and q58 were compiled more times). Three law seeds gained a flip
    (compositions 1481, expressions 1371 and 187859, one each): the same
    SQL, a join the census now credits to a pushed condition.
    `tests/discovery/test_consumer_join_narrowing.py` pins both join
    shapes at plan time (red on `main`): the FULL a stream INNER narrows,
    and the second round.

    Gates, against `af20c84`: corpus (206 statements, with
    `tpc_ds_duckdb/aggregates`) 0 moved; fuzzer 522/522. Suite SQL
    capture under `PYTHONHASHSEED=0` and xdist moved 937 tests, almost all
    column order; the 927 planner ones recaptured serially in both trees
    leave 14, all law or join-matrix tests asserting rows. Twelve shrink:
    filters seed 133525 (base and summary) loses the CTE seam 17 cost it
    (9,704 to 8,637 characters), the rest are 0 to 103 characters shorter
    as a null-safe pair becomes `=` or a LEFT the optimizer kept is planned
    INNER. Two grow by a read off the other side of an INNER at the same
    CTE and join count: windows seed 744 (18 characters, the `tree_form`
    key read off the row side) and `test_membership_feeders_do_not_chain`
    (1, each `cid` read off its own scan instead of one copy). Suite: the
    same 25 credential-bound failures as `main`.

20. **The consumer walk reads through computed flags and shared
    relations (landed; handoff 2.1 step 4, pushed and consumer channels).**
    Seam 19 left 101 flips on this run's base. Two places still stopped
    `consumer_proofs.narrow_for_consumers`:
    - **A proof through a computed flag.** tpc_ds q16, q94 and q95 filter
      `order_number ? is_returned`, with `is_returned <- _returned_order_number
      is not null`. The FilterNode's `is_returned = True` proves
      `_returned_order_number` non-null (`_forced_true_proofs`), but the
      select under it computes the flag and outputs only `is_returned`, so
      `row_reads` dropped the proof one edge above the merge that LEFT joins
      the returns. `computed_inputs` adds the columns a parent reads to
      compute a BASIC value the consumer reads off it, and a proof on them
      passes. An aggregate's inputs never pass: a group's value says nothing
      of each row's.
    - **A node reading another of its own relation.** A joinless merge over
      the select or regroup it passes through resolves to that node's
      identifier, so `_consumers_first` found the relation consuming itself
      and gave up on the whole plan: q58, q83, the return-date pins,
      filters seed 5331, the union-key HAVING semijoin and the subset join
      key sets (376 of 8,654 compiles over the flip files). The self edge
      is skipped in the ordering and in `_propagate`: both nodes render as
      one relation and take what its consumers reject.
    - **A re-resolved node that widens backs the join channel out.** The
      shared-relation walk was backed out before because of dimension-chain
      seeds 258 and 280. Narrowed on its consumer's INNER on `id`, the seats
      merge stamps `tail_num` partial (`_extent_free_partials`: the aircraft
      it preserved are gone), and the joinless merge above re-resolves into
      a LEFT from `aircraft_by_tail_num`, which returns an aircraft no
      flight names (pinned with a dangling aircraft,
      `test_a_narrowing_that_widens_a_consumer_is_backed_out`). `_widened`
      compares each node's parents kept by join type before and after: a
      parent newly kept that did not already keep every row
      (`preserved_sources`, a join whose rows all find a partner) sends
      the plan to the round without join keys, as a failed `_keys_hold`
      does. Only that round is checked: the WHERE proofs of the second
      round reject the padded rows a widened consumer keeps (filters seed
      133525 widens an INNER to a LEFT there, as on `main`).
    - **Measured, not landed: masking the extent-free stamp.** Intersecting
      `_extent_free_partials` with `unfiltered_partials`, as the joined
      partials are, keeps 258's passthrough joinless and narrows it. It
      also moves dimension-chain seed 582: with the stamp masked, the
      airports merge re-resolves its INNER into a RIGHT whose pair no
      longer rejects `tail_num`, `_keys_hold` fails and the whole join
      channel falls back, handing two joins back to the optimizer.

    After, over the 28 files holding a flip: 82 flips (pushed 36, consumer
    28, planned 10, new CTE 8) against 101 on `81966a7` (pushed 52,
    consumer 30, planned 10, new CTE 9); distinct 80 to 64. tpc_ds
    `test_queries` q16, q58, q83, q94 and q95 have none left;
    `test_partial_key_assembly_shapes`, `test_union_key_having_semijoin`,
    `test_duckdb_union_subset_join_key_set`, `test_rank_beside_partial_merge`
    and filters seed 5331 none. `tests/discovery/test_consumer_join_narrowing.py`
    pins the computed flag and the shared relation at plan time (both red
    on `main`) and the widening by rows (red without `_widened`).

    Gates, against `81966a7`: corpus (204 statements) 0 moved; fuzzer
    522/522. Suite: the same 25 credential-bound failures as `main`.

21. **A filtered aggregate's filter is planned as a WHERE; the key channel
    falls back per merge (landed; handoff 2.1 step 4, pushed and consumer
    channels).**
    - **The filtered-aggregate rewrite, planned.** `PushFilteredAggregateInput`
      turns `count(x ? p) >= 1` into a row WHERE `p` after planning, and
      `UpgradeJoinOnGuards` then flips the joins `p` rejects (tpc_ds q73).
      `consumer_proofs.group_filter` hands a group's parent `p`'s proofs
      when every aggregate the group computes is NULL-ignoring over the same
      filter and its consumers reject a group none of whose rows pass:
      a NULL proof for SUM/AVG/MIN/MAX, and for a COUNT, which is 0 there,
      a comparison 0 fails (`condition_utility.zero_rejections`; `>= 0`
      and `is not null` keep the empty groups and the outer join). An
      unfiltered aggregate beside it keeps every row. `filtered_aggregate`
      moved beside it; the optimizer rule imports it.
    - **A grouped merge reads an aggregate as a key.** `row_reads` dropped
      every aggregate a grouped node reads, so a HAVING on the merge above
      the group (the q73 shape) never reached it. A merge's group only
      dedupes, and a partial it rolls up is still excluded (`rolled`).
      q64 moves in the corpus: `cs_ui_sale > 2 * cs_ui_refund`, both
      `sum(... ? cs.is_returned)`, plans the catalog returns INNER, as the
      reference's own join is.
    - **The key channel bars a merge, not the plan.** A merge whose
      consumers stop rejecting its keys (`_failing_keys`), or every keyed
      merge under a node that `_widened`, is barred from taking keys, and
      the plan narrows again from the saved state; only when nothing keyed
      is to blame does the whole channel fall back. With the extent-free
      mask (`MergeNode` intersects `_extent_free_partials` with
      `unfiltered_partials`, as the joined partials are), dimension-chain
      seeds 258, 280 and 595 narrow; seed 582's airports merge is barred
      alone (its two flips go back to the optimizer, as seam 20 measured),
      and `test_cluster_key_lookup` falls from 50 flips to 26. Filters seed
      133525 now keeps the keyed round with three merges barred, and one
      consumer LEFT is left to the optimizer (2 flips; SQL byte-identical).

    After, full suite under `-n 4` (`guard_census_plugin`): 92 flips
    (external 51, pushed 22, planned 10, new CTE 9) against 125 on
    `7120370` (external 80, pushed 26, planned 10, new CTE 9); distinct
    116 to 83. Under xdist q73 records 3 flips (12 when the file runs
    alone); it has none left either way. `tests/discovery/test_consumer_join_narrowing.py`
    pins the narrowing at plan time (red on `main`), the zero-kept COUNT
    and the unfiltered aggregate beside it by rows.

    Gates, against `7120370`: corpus (204 statements) 1 moved (q64,
    above); fuzzer 522/522. Suite: the same 25 credential-bound failures
    as `main`, plus `test_rank_beside_partial_merge::test_a_rank_and_its_total_need_their_own_node`
    under xdist, which fails alone on `main` too (it reads a window concept
    an earlier test's query materializes).

22. **A directional join narrows on a forced side, as a FULL does
    (landed; handoff 2.1 step 4, external and planned channels).**
    - **The gap.** `narrow_join_types` narrows each join twice with
      `downgrade_join_for_proofs`: first on the merge's own and its
      consumers' proofs (WHERE and key channel), then on its branches'
      proofs (`branch_proofs`). Both acted on a FULL only. In
      `test_cluster_key_lookup` the snap-reason merge FULL joins two tree
      branches and FULL joins the cluster lookup. Without the key channel
      the branch proof (the city filter) forces the stream: LEFT. With it,
      the consumers' `cluster_id` forces the lookup first, FULL to RIGHT,
      and the branch call skipped the RIGHT. The RIGHT keeps lookup rows
      the plan dropped before, `_widened` barred the merge, and the
      optimizer flipped the LEFT on the consumers' INNER on `cluster_id`.
    - **The fix.** `downgrade_join_for_proofs` narrows any outer join;
      `unpadded_join_type` already maps a LEFT/RIGHT whose padded side is
      forced to INNER and leaves the kept side's proof inert. A side's
      forcing is a fact about the surviving rows, not about the join's
      current type, so it composes whichever call ran first.
    - **Also moved.** The five `union join` rowset flips
      (`test_duckdb_rowset`, FULL to RIGHT on `year = 2001`, the planned
      channel) are now made by the planner; their SQL is byte-identical.
      Dimension-chain seed 250 narrows.

    After, full suite under `-n 4` (`guard_census_plugin`), both trees on
    one machine: 59 flips (external 24, pushed 21, new CTE 9, planned 5)
    against 92 on `3908a40` (external 51, pushed 22, new CTE 9, planned
    10); distinct 44 to 38. `test_cluster_key_lookup` 26 to 0,
    `test_duckdb_rowset` 6 to 0, dimension chain 7 to 6. Pinned at plan
    time by `test_cluster_key_lookup::test_the_cluster_lookup_is_planned_inner`
    and by the two `downgrade_join_for_proofs` unit tests in
    `tests/core/processing/test_grain_utility.py` (red on `main`).

    Measured, not landed: dimension-chain seed 541. The key channel
    narrows the airport/carrier merge under the count, and the top merge
    pairing the count with `max(seats)` is generated RIGHT OUTER
    (`generate_joins`, before any narrowing), so `_widened` bars all three
    keyed merges under it. That is join generation reading the narrowed
    parent's partial marks, the extent-free family of seams 20 and 21.

    Gates, against `3908a40`: corpus (204 statements) 7 moved, all tpc_ds
    (q01, q05, q24, q54, q59, q62, q80), same CTE and join counts and
    types: a key once read off the fact side of a join the optimizer
    flipped is now read off the dimension side of one planned INNER
    (`S_STORE_SK` for `SR_STORE_SK`, `D_DATE_SK` for `WS_SHIP_DATE_SK`),
    equal under the INNER equality; their row tests pass. Fuzzer 522/522.
    Suite: the same 25 credential-bound failures as `main`.

23. **A re-typed reader keeps its planned joins over a narrowed side
    (landed; handoff 2.1 step 4, external channel).**
    - **The gap.** Dimension-chain seed 541 (`select carrier_name,
      airport_name, count(id) as n, max(seats) as mx`). The key channel
      narrows the `max(seats)` side to INNER throughout, so its
      `carrier_name` and `airport_name` are no longer padded. The root merge
      pairing it with the count was not narrowed, but it re-resolves over
      the narrowed parent: the key is now nullable on the count's side
      alone, and `_nullable_join` preserves that side (INNER to RIGHT). The
      RIGHT stops rejecting `airport_name`, so `_failing_keys` names the
      count's merge and `_widened` all three keyed merges under the root;
      all are barred and the optimizer flips their LEFTs. Seed 582 is the
      same one level down, where the reader's rows all matched before:
      `_widened` passed it and `_failing_keys` barred the merge under it.
      Seam 22 read this as join generation reading partial marks; the
      trace shows the reader's nullable marks.
    - **The fix.** When a round would bar, each merge whose join types now
      keep a side they dropped before is held to the types it planned
      before any narrowing (`MergeNode.planned_join_types`, applied by
      `utility.keep_planned_join_types` ahead of `narrow_join_types`), as
      long as it pairs the same parents in the same order, and the plan is
      narrowed again. A pin never widens a type, and narrowing still runs
      on top of it. Only when no merge can be held does the channel bar
      as before. The narrowing dropped only rows the planned types reject,
      so they return the rows they did. With a key no longer nullable on
      either side the null-safe pair renders plain `=`, equal there.

    After, full suite under `-n 4` (`guard_census_plugin`), both trees on
    one machine: 53 flips (external 18, pushed 21, new CTE 9, planned 5)
    against 59 on `5f24c2a` (external 24, pushed 21, new CTE 9, planned
    5); distinct 46 to 40. `test_dimension_chain_oracle_laws` 6 to 0
    (seeds 343, 541, 582). Pinned at plan time by
    `test_consumer_join_narrowing::test_a_reader_keeps_its_planned_join_over_a_narrowed_side`
    (red on `main`: LEFT OUTERs, same rows) and the `keep_planned_join_types`
    unit tests in `tests/core/processing/test_grain_utility.py`.

    Gates, against `5f24c2a`: corpus (204 statements) 0 moved;
    `tpc_ds_duckdb/aggregates` identical; fuzzer 522/522. Suite: the same
    26 failures as `main` (25 credential-bound, plus
    `test_rank_beside_partial_merge` under xdist).

24. **The consumer walk reads what the optimizer read: complete keys,
    per-row WHEREs, lineage, aligns (landed; handoff 2.1 step 4, external
    and pushed channels).**
    - **A consumer's key forces the side complete on it.** The key channel
      (`_keys_force`) forced only the source a key renders off. A WHERE
      proof on the key tuple also forces the other side, complete on it,
      since a non-null value has a match there (`_side_forced`), and the
      optimizer's `proves_key` does the same for a consumer's equality.
      `_paired_key_reads` feeds the tuple arm the consumer-rejected keys
      the merge reads off a source this join equates them on, per side,
      only where the pair equates a concept with itself and the side holds
      it completely. The first cut took any paired key and broke 24 suite
      tests: a subset join (`b.r_key = a.l_key`) holds `b` complete on
      `r_key`, not on `l_key`'s values (join matrix subset cells, derived
      key domains, unmodelled regions). Two-namespace import 2 and
      `test_semi_join_pushdown` 10 are now planned.
    - **A narrowed merge re-reads its keys.** `_propagate` re-took a merge
      only when its proofs changed, so `key_reads` stayed as first read:
      gcat's launch-year merge narrowed FULL to LEFT on its consumer's key,
      after which `launch_tag` renders off the launches alone, and nothing
      read it again. `_reads_changed` re-takes when a rejected key comes to
      render off one source (gcat 1, spellings seed 900 1).
    - **A filter's own WHERE reads its parent's aggregate per row.**
      `row_reads` drops every aggregate a grouped node reads, right for a
      consumer's rejection of a regrouped value. A FilterNode folding into
      its group still applies its WHERE before grouping: `min(height) by
      tree_form > 3` rejects each parent row whose minimum the merge
      padded. `where_reads` passes the node's own conditions on those
      columns (windows seed 20).
    - **A proof reaches a computed value's NULL-strict inputs.** `v0 < 2`
      over `v0 <- coalesce(count, 0) / nullif(max - 5, 0)` rejects a NULL
      maximum; the walk stopped at `v0`, which no parent supplies.
      `lineage_proofs` follows the BASIC lineages a node computes, as the
      optimizer's `_lineage_proofs` does inline. Not through `GROUP` or
      `GRAIN_PIN`: a pinned value is read at its anchors' grain, and
      `grain_pin(case ..., species)` proved `species` and dropped the
      padded `ash` (`test_pinned_filter_on_padded_species`). Windows seed
      92, expressions 3, tpc_ds q72 1. It also plans three joins the
      optimizer never flipped, since their WHERE sits a CTE above the join
      where no external proof reached: tpc_h adhoc04 (`return_ratio >
      0.5`), tpc_ds q75 (`curr_cnt / prev_cnt < 0.9`) and the q04 agent
      candidate (its HAVING rejects all four arms through their aliases).
      Rows identical on the tpc_ds data for both tpc_ds statements.
    - **An align's merge takes its HAVING.** `_passes_rows` refused a merge
      with explicit `node_joins`, so a MULTISELECT HAVING never reached the
      align's FULL (demo merge 2). A HAVING reads the aligned rows; arm-local
      evidence still does not narrow it (`MergeNode._join_proofs`).
    - **Measured, not landed: sibling nodes the optimizer fuses.** Root
      partition `attribute_filter` (three regroups of one FULL merge, one
      read by the WHERE) and compositions seed 1481 (two keyless merges over
      the same parents, only the `v2` one rejected) are flipped only after
      the optimizer merges the siblings into one CTE. The walk ANDs over
      the siblings, which is right while they are apart.

    After, full suite under `-n 6` (`guard_census_plugin`), both trees on
    one machine: 30 flips (pushed 12, new CTE 9, planned 5, external 4)
    against 53 on `5728405` (pushed 21, external 18, new CTE 9, planned
    5); distinct 44 to 26. Pinned at plan time (each red on `main`):
    `test_consumer_join_narrowing::test_a_grouped_filter_reads_its_parents_aggregate_per_row`,
    `::test_a_having_on_a_computed_value_narrows_through_its_lineage`,
    `::test_a_having_over_an_align_narrows_it`,
    `test_two_namespace_import_merge_hidden_output::test_a_consumer_key_read_off_one_side_plans_the_lookup_inner`,
    `test_gcat::test_a_narrowed_merge_rereads_its_keys`, and the two
    consumer-key `downgrade_join_for_proofs` unit tests in
    `tests/core/processing/test_grain_utility.py`.

    Gates, against `5728405`: corpus (204 statements) 3 moved, the three
    joins above, each a LEFT or FULL to INNER under a WHERE or HAVING that
    rejects the padding; `tpc_ds_duckdb/aggregates` identical; fuzzer
    522/522. Suite: the same 25 credential-bound failures as `main`.

25. **The planner's own WHERE reads what the optimizer read: a later
    INNER after ordering, an exclusive partial, a passed-through flag
    (landed; handoff 2.1 step 4, planned and pushed channels).**
    - **Most of the census is the rule's own tests.** All 9 new-CTE flips
      and 2 of the 4 external ones are `tests/optimization/test_join_upgrade.py`,
      which builds CTEs by hand to test `UpgradeJoinOnGuards` itself; no
      planner produces them. Of 30 flips on `67f6e02`, 19 were plans.
    - **A later INNER narrows a FULL to RIGHT after the merge orders its
      sources.** `narrow_joins_read_by_inner` ran a second time with
      `inner_only`, which refused FULL to RIGHT: `compute_outer_null_status`
      had already ordered the sources while both sides of the FULL were
      padded, so a key shared with the left would be read off the left the
      RIGHT now pads (`test_union_join_anchor_where_precedes_the_rowset_pairing`,
      seam 14). The status is now taken after every narrowing, so the order
      sees the final types and `inner_only` is deleted
      (`test_multihop_nullable_fk_no_null_leak`: an INNER to the address on
      the customer's `address_sk` keeps no padded customer row).
    - **A partial column one side alone holds proves that side to a WHERE
      the merge renders.** `_unprovable_addresses` refused every partial
      binding, for the region-domain case: a value the domain carries is
      partial on the solid stream, and a WHERE over it keeps the domain's
      rows. That case is a column two datasets hold. One only the padded
      side holds renders off it alone and is NULL on every row it pads,
      partial or not, as the optimizer's `_blocked_partials` already read
      it (`ret_plus <- ret + 1`, stamped partial beside the `~` keys of the
      returns; the presence probes). `_opaque_addresses` applies to
      `side_proofs` only. `branch_proofs`, a branch's own filter taken as
      intent for the merge, keep the guard: a first cut without it planned
      INNER under `item_name != 'B'` and dropped the unsold item's 0 count
      (`test_unprojected_property_where_keeps_the_region_on_a_guest_model`,
      caught by the full suite).
    - **A flag proof reaches the node computing the flag through the merge
      passing it.** `computed_inputs` looked one node down. `where
      is_returned` sits above a merge that reads `is_returned` off a select
      computing it from `ret_qty`; the proof drawn through the flag names
      `ret_qty`, which the merge does not output, so the walk dropped it
      there. It now follows a computed value a row-passing node reads off
      one parent alone down to where it is computed (unmodelled regions,
      both twins).
    - **Measured, not landed: a merge's own WHERE through a value it
      computes.** Handing the merge's own WHERE `lineage_proofs`' walk (the
      optimizer's `_lineage_proofs`) fired 0 times over the full suite:
      every proven value a merge reads is a column of one of its sources,
      already computed below it.
    - **Measured, not landed.** Keyed-class trees 4 and scoped join
      expression keys 2: the optimizer pushes a merged-key or sibling
      condition into one side and flips there (a FULL on `tree_id` under
      `coalesce(tree_id) is not null`; the WHERE reaches the inlined
      customers scan of a sibling merge). The fixpoint ungated-pushdown
      flip is a test that turns pushdown's gate off on purpose.

    After, full suite under `-n 4` (`guard_census_plugin`), both trees on
    one machine: 24 flips (pushed 10, new CTE 9, external 4, planned 1)
    against 30 on `67f6e02` (pushed 12, new CTE 9, planned 5, external 4);
    of plans, not `test_join_upgrade`'s hand-built CTEs, 19 to 13.
    `test_multihop_not_null_preserved`, `test_null_rejection_laws` seed
    16, the two presence probes and unmodelled regions 2 are now planned. Pinned at plan time (each red on `main`) by
    `tests/discovery/test_planned_join_narrowing.py`.

    Gates, against `67f6e02`: corpus (204 statements) 1 moved, tpc_ds q35:
    `customer_current_demographics_sk` read off the demographics side of
    the INNER join on it instead of the customer side, equal there (the
    source order now sees the narrowed types); its row test passes.
    `tpc_ds_duckdb/aggregates` covered by `test_agg_queries` in the suite.
    Fuzzer 522/522. Suite: the same 25 credential-bound failures as
    `main`.

26. **A relation's sibling nodes are one row set to the consumer walk
    (landed; handoff 2.1 step 4, pushed channel).**
    - **Siblings the optimizer fuses were ANDed apart.** Nodes sharing a
      relation identifier render as one CTE: the identifier fixes their
      datasources, grain, group keys and condition, and `CTE.__add__`
      fuses only copies of one grain and condition. A consumer rejecting a
      column any of them reads rejects the relation's row, and the parent
      rows under it. `_propagate` ANDed what each sibling passed a shared
      parent, so a WHERE read off one sibling alone never reached the
      merge: root partition `attribute_filter` (three regroups of one FULL
      merge, `state = 'ca'` read off one) and compositions seed 1481 (two
      keyless merges over the same parents). The optimizer flipped those
      only once it had fused the siblings into one CTE. Siblings that all
      pass rows now OR their reads of a parent, then AND with the other
      relations reading it. Seam 24 measured this as needing the siblings
      planned as one node; the walk only had to read them as one.
    - **Measured, not landed: the grain-pin flip.** `bucket = 'Same'` over
      `bucket <- grain_pin(case ..., tree_id)` proves `tree_id` to the
      optimizer, since `GRAIN_PIN` is in `NULL_STRICT_FUNCTIONS` ("a value
      pinned to its anchors exists on their rows only"), and the FULL to
      the enrichment flips to LEFT. The pin renders as `(case ...)` and
      never reads its anchor, so on a row whose anchor is NULL the value
      is computed anyway; the planner refuses the proof (`lineage_proofs`,
      seam 24). Which is right is a semantics question (is a pinned value
      NULL off its anchor's rows?), not a narrowing one. The same proof is
      the only early (`left_only`) flip over the fuzzer (2, the merged-axis
      pin cases); over the suite the early pass flips only
      `test_join_upgrade`'s hand-built CTEs.
    - **Measured, not landed.** Keyed-class trees 4: the planner hands the
      final FULL `tree_id` as a side proof only (a FULL key renders
      coalesced), and the optimizer flips it after collapsing the WHERE's
      select into the merge and narrowing the merge under it first.
      Scoped join expression keys 2: the consumer's
      `customers.region is not null` reaches a sibling merge only through
      the customers scan the optimizer inlines into it. Both need the
      pushdown's placement planned.

    After, full suite under `-n 4` (`guard_census_plugin`), both trees on
    one machine: 21 flips (pushed 7, new CTE 9, external 4, planned 1)
    against 24 on `95e7548` (pushed 10). Pinned at plan time (red on
    `main`) by `test_planned_join_narrowing::test_a_where_read_off_one_of_a_relations_sibling_nodes_narrows_its_parent`.

    Gates, against `95e7548`: corpus, every file under the four globs
    (383 files, 208 statements), 0 moved; fuzzer 522/522. Suite: the same
    25 credential-bound failures as `main`.

27. **A merged key's IS NOT NULL proves each side complete on it (landed;
    handoff 2.1 step 4, pushed channel).**
    - **The FULL arm read the key tuple off half the harvest.** A HAVING
      above the final merge (`select tree_id, predicted, --is_imagery,
      --height_m having tree_id is not null and ...`, keyed-class trees)
      hands the merge `tree_id is not null` on the FULL's coalesced key.
      `non_null_proofs` skips `IS NOT NULL` so that a shared key is never
      claimed non-null per side, and the key-tuple arm of
      `downgrade_join_for_proofs` read only that set. The arm claims less
      than that: a side complete on the key has a match for every non-null
      value, and `k is not null` proves the key non-null as `k = 5` does.
      `downgrade_directional_join_for_proofs` already read its key tuple
      off the full harvest, and the optimizer's `proves_key` reads `direct`
      the same way. The FULL arm now does too (`_key_tuple_forced` over
      `side_proofs`), with the same `_partial_lookup_keys` guard. The final
      FULL plans INNER, and the walk then hands `tree_id` to the merge under
      it, now read off one side, which narrows FULL to RIGHT.
    - **Measured, not landed: scoped join expression keys.** The 2 flips in
      `test_window_expression_join` are not a NULL rejection. The final
      `uneven RIGHT JOIN abundant on customers_rnk` keeps an `uneven` row
      only where its rank is a customer's, and a row of `uneven` the
      customers side padded carries a rank no customer has. The optimizer
      gets there by pushing `customers.region is not null` onto the
      customers scan it inlines into the sibling merge. Planning it needs
      a value-domain proof (a consumer's join matches only values its
      complete side holds), which the consumer walk does not carry.

    After, full suite under `-n 4` (`guard_census_plugin`), both trees on
    one machine: 15 flips (new CTE 9, pushed 3, external 2, planned 1)
    against 19 on `cee3bb2` (pushed 7). Filters seed 133525 flipped on
    neither tree in this run or in isolation (21 on `cee3bb2` in seam 26's
    run). Pinned at plan time (red on `main`) by
    `test_planned_join_narrowing::test_a_merged_key_is_not_null_proves_each_side_complete_on_it`.

    Gates, against `cee3bb2`: corpus (204 statements) 0 moved; fuzzer
    522/522. Suite capture (`sqlcap`, about 54,300 statements): the keyed
    class trees, and 3 statements (filters seed 10457 summary, spellings
    seed 9938 base and summary) whose INNER join to the `max(height) by
    tree_form` CTE reads `tree_form` off the regroup by (city, species,
    tree_form) instead of the row scan it is INNER-joined to on (city,
    species): one value on every row, join types unchanged, row oracles
    green. The other moves (column order) recur between runs of one tree
    and are not in a serial A/B of the same files. Suite: the same 25
    credential-bound failures as `main`.

28. **A pinned value is NULL-strict on its anchors; a key proof forces no
    partial side (landed; handoff 2.1 step 4, pushed channel).**
    - **The semantics, settled.** A pinned value has no value off its
      anchors' rows, as a property has none without its key: grain
      pinning defines a value keyed on the select's keys. The optimizer
      read `GRAIN_PIN` as NULL-strict all along (`NULL_STRICT_FUNCTIONS`);
      the planner now does too (`_evaluated_at_a_grain` stops only at
      `GROUP`). The merged-species `bucket = 'Same'` FULL is planned LEFT.
    - **The wrong-results bug it had hit.** Seam 24 refused the proof
      because it dropped `ash` (`test_pinned_filter_on_padded_species`).
      The pin was not the cause. `downgrade_join_for_proofs` and its
      directional twin force a side through the key tuple unless
      `_partial_lookup_keys` names the key, and that named only a side
      whose grain the keys cover (notes by customer). An unnest binds its
      parent's key partially at a wider grain: a species with an empty
      array has no row. So `species = 'ash'`, or `species != 'oak'`,
      forced the unnested side and the LEFT became INNER: on `main`,
      `select city, ecoregion_id, count(tree_id) where species != 'oak'`
      drops the city-B tree of `ash` that the unfiltered query returns as
      `('B', NULL, 1)`. `Rewrite.key_proof_keeps_lookup_gap` already
      states the rule for any side lacking key values; `_partial_keys`
      now applies it to every partial key binding. Nothing in the suite
      pinned the wider forcing (full suite green with it removed).
    - **Not done: the render.** A pin still renders as `(expr)`, never
      reading its anchors, so a row whose anchor is NULL shows the
      computed value where the semantics say NULL. Guarding it with its
      anchors (`case when anchor is not null then expr end`) rewrites
      every pinned value's SQL and needs its own A/B.

    After, full suite under `-n 4` (`guard_census_plugin`), both trees on
    one machine: 14 flips (new CTE 9, external 2, pushed 2, planned 1)
    against 15 on `73b3199` (pushed 3). Pinned (each red on `73b3199`):
    `test_key_proof_partial_side::test_a_key_where_keeps_the_rows_an_unnest_lacks`
    (both merge spellings), the `species = 'ash'` case of
    `test_pinned_filter_on_padded_species`, and
    `test_planned_join_narrowing::test_a_pinned_value_proves_its_anchor`.

    Gates, against `73b3199`: corpus (204 statements) 0 moved; fuzzer
    522/522. Suite capture (`sqlcap`): about 260 statements moved, almost
    all `test_relation_rollup_laws` and `test_summary_cover_laws`, plus
    `test_merge_bridge_filter_only_axis` (2) and the merged-species pin.
    Each is an INNER becoming LEFT onto an unnest side partial on the
    key: the bug's shape. Their row oracles pass either way because
    their data never holds an empty array; extending the relation laws'
    data to one would pin it there too. Suite: the same 25
    credential-bound failures as `main`.

## Gate used

```
python local_scripts/sql_ab/corpus_sql.py <out.json> "tests/modeling/**/*.preql" "tests/engine/**/*.preql" "tests/complex/**/*.preql" "tests/discovery/**/*.preql"
python local_scripts/sql_ab/sqldiff.py base.json new.json --show 40
python -m local_scripts.fuzzer
python -m pytest tests -m "not adventureworks_execution and not clickhouse_server and not bigquery_execution and not cloud_live"
```

The corpus misses `tests/modeling/tpc_ds_duckdb/aggregates/` and every shape
only a unit test builds; the register above is the list of sites that
difference hid.
