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
  `test_duckdb_rollup_rowset_binding.py`.
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
sixth slice: 3, 4, 6 and 7 landed (6 smaller than estimated, its planner half
folded into 2), 5 landed its ownership half, 2 landed its plan-time EQUAL,
`~`-subset, graph-subset and twin proofs and decided NULL membership (the
optimizer rule's firings over the non-law suite: 912 before the first slice,
796 after it, 522 after the second, 407 after the third, 377 after the
fourth, 299 after the fifth, most of that drop refusals of unsound
narrowings, and 256 after the sixth, measured from 324 once the null-group
matrix joined the suite; over the whole suite, laws included, its CTE
rewrites fell from 1,045 to 496 in the fourth) and lost its per-side origin premise, 8 closed,
9 half done; 1 and the rest of 2 remain, and hold nearly all of the
estimated lines. Seam 1 was re-measured: its blocker is the ranked owner
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
   - **An unnest of complete lists is not complete at plan time.** The
     optimizer reads the elements as complete (`_unnested_parent`), and 153
     of the remaining EQUAL firings rest on it. Typed INNER while planning,
     an owner-NULL row appears downstream
     (`test_merged_unnest_lookup_no_padding.py`, `tree_reporting` native
     range build), so it stays in the optimizer.
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
   an `OPEN` wrong-rows lead, now plans right (two aggregates over one
   stream typed INNER) and joins `FOUND_SEEDS`. Corpus (205 statements, tpc-ds
   aggregates included): 0 moved. The first cut of the guest refusal moved
   hackernews adhoc05 (twin groups over one stream INNER to LEFT): the
   optimizer reads the two CTEs' sources, and one predates the WHERE the
   optimizer pushed into their shared parent, so it judges the rows by the
   CTE filter chain too (`_same_filtered_rows`). The suite fails the same 25
   tests as `main` (credential-bound and
   `the_look::test_wrapped_aggregate_with_dependent_by_grain`). Fuzzer
   522/522.
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
