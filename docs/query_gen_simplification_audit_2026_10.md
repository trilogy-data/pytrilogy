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
- `_group_final_grain_contribution` returning `merge_grain` for a ROWSET:
  q44, q46 and q68 gain a redundant `GROUP BY` without it.

Rowset assembly:
- Two rowset boundaries flowing straight to FINAL defer their WHERE to FINAL
  (`rowset_final_groups`): a derived-key union join leaks next-year-only rows
  (`test_independent_rowset_matrix.py::test_union_join_derived_key_filtered_on_derived_side`).
- `_synthesizes_handle` (a handle is computable only on its own boundary):
  3 row pins; this is the generic rule the deleted sites restated.
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

## Design seams (not built)

Ranked by lines a seam would free, across the axes:

1. **Extension rows as a first-class row source** (about 2,100 lines:
   `region_domains.py`, `extent_ownership.py`, `region_reads.py`, the
   `SpanScope` carriers, `_extent_free_*` stamps and joins). The keyspace
   already names each region; a region with rows of its own as a ROOT-like
   leaf would collapse the owner election, the extent-free suppression and
   the held-span join contract into "a leaf joins on its key, preserving the
   leaf". Blocked by the padded-plan fallback every region row pin protects.
2. **Per-side origin nodes in the planner** (about 520 lines of
   `value_set_join_upgrade.py` plus `scoped_partial_derived` and the
   `JoinScope` registries). The narrowing rules act on evidence the planner
   had at `get_join_type` time in 24 of 34 corpus firings; the 10 it could
   not decide are same-address origin arbitrations that need a per-side
   node. This is the seam `docs/domain_graph_design.md` step 5 names.
3. **A rowset handle as a leaf in `projection.row_lineage_arguments`**
   (about 170 lines: `_raise_if_rowset_islanded` and the bridge diagnosis,
   `_mangled_rowset_content_addresses`, `_synthesizes_handle`, the
   grain-change walk stop, `_crosses_outer_aggregate`). The concept graph
   already treats a ROWSET concept as a leaf, but `BuildRowsetItem.
   concept_arguments` is `[content]`, so every assembly row-lineage walk sees
   through the handle and each site re-imposes leaf-ness after the fact. A
   handle was judged satisfiable through its content in 46 tests; those are
   the gate. A first step (`row_lineage_arguments` returns `[]` for a
   ROWSET) is corpus byte-identical but disconnects
   `test_duckdb_rowset.py::test_tvf_union_order_by_grouped_away_column`: a
   union's hidden ORDER BY key reaches its arms only through handle content.
4. **Rowset witness from the body plan** (about 335 lines). `_witness`
   rebuilds the body's concept graph separately from `resolve_rowset`; a body
   plan that handed back its keyspace would delete the recomputation and the
   recursion guard.
5. **Provenance stamped by the producer** (about 500 lines across
   `null_provenance.py`, `join_resolution.py` side-fact assembly and the
   `value_set_join_upgrade` padding walks). Corpus evidence says the stamp
   could start as `value | extent`.
6. **Drop the `narrow_equal_domain_joins` knob** (about 230 lines). Under
   model trust `merge a into b` makes one key; the planner's FULL-for-every-
   EQUAL-key rule plus the optimizer's `_upgrade_to_inner` exist to make that
   trust switchable.
7. **One padding walk.** Six left-deep "who gets padded" walks over the same
   `left_deep_joins` primitive (about 120 lines).
8. **`DomainKind` as a trace label.** No behavioural consumer reads
   `RegionDomain.kind`; every reader keys on whether a bucket exists. The
   BOUNDARY decision is load-bearing, the label is not.
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
