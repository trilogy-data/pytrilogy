# Handoff: extension-row NULL semantics, open follow-ups

What PR #702 (`extension-row-null-semantics`) left open. The design is in
`docs/grain_pin.md` and `docs/modeling_concepts.md`; every fix the branch made
is in git history. Delete an item when it lands or moves to an issue.

## Measured against main (merge base 44d8513a8)

From the committed `zquery<N>.log` files:

| corpus | chars | CTEs | JOINs | GROUP BYs |
|---|---|---|---|---|
| TPC-DS (104 queries) | 389,314 -> 382,477 (-1.8%) | 356 -> 348 | 457 -> 457 | 189 -> 189 |
| TPC-H | 27,513 -> 27,046 (-1.7%) | 38 -> 38 | 65 -> 65 | 26 -> 26 |
| thelook | 12,432 -> 11,296 (-9.1%) | 15 -> 15 | 38 -> 35 | 20 -> 21 |

The 33 changed TPC-DS queries (sf=1, DuckDB, interleaved, min of 3): 3.91s ->
3.65s (-6.6%). The committed timing logs came from a loaded machine and are
not comparable across the branch. Every growth was read by hand; the largest,
q11, is a correctness fix (main dropped the row-level WHERE).

Regression gates: `local_scripts/sql_ab/region_battery.py` reruns the
two-region (`tests/engine/test_padded_null_pairing.py::TWO_REGIONS`) and
`LINE_ITEMS` batteries and diffs them against any tree;
`python -m local_scripts.fuzzer` must stay 260/260.

## Open: plan cost (no wrong rows)

- **q04 filters year after the union.** `FoldExistenceIntoAggregate`
  (`optimizations/existence_having_fold.py`) moves q04's implied year WHERE
  onto the aggregate, but the year lives on `date_dim`, joined above the
  union, so pushdown cannot sink it into the arms: 0.054s -> 0.076s vs
  830387ff9. The old plan filtered each arm through its own INNER date join,
  sound only because every aggregate's filter implied the WHERE. q11 (0.122s
  -> 0.040s) and q74 (unchanged) fold fully.
- **A `union join` axis kept under a WHERE that already makes it one-sided.**
  `where year = 2001 select ticket, r_filtered.return_quantity $J` over
  `_ANCHOR_WHERE_FIXTURE` (`tests/engine/test_duckdb_rowset.py`) plans the FULL
  axis although `year` is sale-side; a directional pairing onto the anchor
  gives the same rows with 3 joins instead of 5. `null_rejected(conditions)`
  and the 5(a) typing rules already reason about this.
- **`_merges_coalesced_sides` is broad** (`strategy_builder.py`): it vetoes
  folding any 2+ parent merge that outputs a coalescing-relation member. No
  current cost (a suite A/B moved only its two target tests). Narrow it to
  "parents carry different sides" if it shows up in a regression.
- **Two rowset merge-site patches are still load-bearing**:
  `group_graph._rowset_relation_keys` (composite three-key union joins) and
  the ROWSET branch of `_group_final_grain_contribution`
  (`test_union_join_single_key_with_measures`).

## Open: design

- **Spelling classes keep the smallest spelling as the name.** Source planning
  reads network roots back as concepts and plans depend on the name.
  Authored-first lost the merge variant a scan renders (the titanic
  merge-rowset demo cross-joined); canonical-first grew five TPC-DS plans.
  Nothing asserts that a map keyed by one spelling is read by the same one.
- **Deciding the grain pin after `build_keyspace`** would make the anchor
  heuristics (`_always_beside`, `_held_beside`, `_co_held_only_beside`,
  `_may_pad`) exact. Not a move: the keyspace, persisted-column matching and
  predicate pushdown all read the pin back from the lineage. No bug traced.
- **`RowsetDefinition`.** `retire_rowset` reconstructs a rowset by prefix and
  lineage walk. One registered definition needs hooks at three parse stages,
  because the outputs are pending when `add_rowset` runs.
- **Condition placement**: an UPSTREAM_MOST atom is still copied to every
  uncovered aggregate rather than asking `_atom_decided_for`.
- **Two meanings of "coalescing".** `concept_graph.coalescing_relation()` is
  statement-scoped `union join` only; `domain_graph.coalescing_relation_members()`
  is any declared INCOMPARABLE edge. No known bug, but a trap.
- **`_read_partials` uses ROOT derivation as its "must read" signal**
  (`partial_bridging.py`). A `~` sibling holding a ROOT concept also reachable
  by another path is treated as not read. Probe with a `~` rollup binding a
  root attribute.
- **A guarded FULL join cannot lower on MySQL**; it is refused with a reason
  (`test_guarded_full_join_is_refused_not_dropped`). No reachable case needs
  it. Lowering means each spine arm emits `(k, g)` with `g` the guard.
- **`group_id` FK-path `keys`.** The build environment gives a key bound
  beside another grain FK-path keys (`{visit_id}`, main too). Region-domain
  re-sourcing no longer reads them, but they are the deeper oddity.
- **`subset join` hint for an FK output** (`preql-demo/docs/examples`):
  `orders.customer_id` and `customers.*` relate only by reading the same
  physical tables, so `rowset_relation_hints` has no merge to follow. A hint
  needs a "same datasource address" heuristic; that is a product call.
- **Anchor heal assumes the declared model.** A complete anchor plus a fact row
  with no anchor row (data violating the model) is dropped by the INNER merge.
  Correct under the contract, but worth a note in the user modeling guide.

## Left by design

- **IS [NOT] NULL is not NULL-absorbing**: it feeds presence probes and
  null-rejection proofs.
- **`join_resolution._padding_witness`'s skip** fires in 123 battery queries
  and every row is right. If a wrong row turns up there, first check why
  `_pads_beside` does not exempt the pairing.
- **`SideFacts.span_padding` beside `held_spans`**: `span_padding` alone decides
  exactly one corpus call (TPC-DS q81). Retiring it means explaining q81.
- **Not duplicates (checked):** `value_null_spans` vs `value_nullables`; the
  three ROLLUP readings; `binding_is_complete`, `scan_partial_addresses` and
  `complete_key_domain`; `Region.filtered` vs `_PartnerFacts`; the builder's
  `applied_atoms` vs `MergeNode._join_proofs`.
- **Do NOT "simplify":** `group_rules.merge_terminal_siblings`'s
  `output_addresses` (empty grains would start merging);
  `group_graph._regraft_candidate(environment=None)` (one caller relies on the
  exact-grain fallback).
- **Long functions left:** `_split_root_dimension_clusters` (182 lines, no
  clear seam) and `_place_atom` (~375, could split by placement reason).
