# Handoff: final audit of `extension-row-null-semantics` (2026-10-09)

The pre-merge audit asked three questions: did generated SQL get simpler, does the
keyspace now overlap other nullability machinery, and is the code DRY. Everything
it fixed is in git history (02a4f07c2..0c0ccc15f). This doc keeps what the branch
measured against main, the decisions to leave things alone, and what is still open.

## 1. Generated SQL vs main (merge base 44d8513a8)

Compared using the committed `zquery<N>.log` files, `generated_sql` from
`origin/main` against the branch.

| corpus | chars | CTEs | JOINs | GROUP BYs |
|---|---|---|---|---|
| TPC-DS (104 queries) | 389,314 -> 382,477 (-1.8%) | 356 -> 348 | 457 -> 457 | 189 -> 189 |
| TPC-H | 27,513 -> 27,046 (-1.7%) | 38 -> 38 | 65 -> 65 | 26 -> 26 |
| thelook | 12,432 -> 11,296 (-9.1%) | 15 -> 15 | 38 -> 35 | 20 -> 21 |

The 33 changed TPC-DS queries (sf=1, DuckDB) ran interleaved, min of 3: **3.91s
-> 3.65s (-6.6%)**. The committed timing logs are not comparable across the
branch: they came from a loaded machine.

Every growth was read by hand and is intended. The largest, TPC-DS q11 (+1009
chars, 0.08s -> 0.16s), is a correctness fix: main dropped the row-level WHERE
and matched only because the HAVING implies it.

**Open opportunity (q11):** when the existence rows and the per-key aggregate
read the same stream, the existence test could be one more HAVING term on the
aggregate (`count(case when <row atoms> then 1 end) > 0`), dropping the second
scan. Not attempted.

## 2. Left by design

- **The in-source padding path (`join_resolution._padding_witness`).** Its skip
  fires in 123 battery queries and every affected row is right (the TWO_REGIONS
  ones are pinned by tests). In `customer_id, count(customer_id) by bucket` the
  pairing the skip allows is the intended answer: cat's padding unites with the
  NULL bucket group. A presence marker there would deny those pairings. If a
  wrong row turns up on this path, first check why `_pads_beside` does not
  exempt the pairing.
- **`SideFacts.span_padding` beside `held_spans`.** `_pads_beside` ORs them.
  Over the corpus, `span_padding` alone decides exactly one call (TPC-DS q81,
  `cs.return_customer.sk` padded for `cs.item.sk`/`cs.order_number`); in the
  batteries `held_spans` decides every case. Retiring `span_padding` means
  explaining q81 first.
- **Not duplicates (checked):** `value_null_spans` vs `value_nullables`; the
  three ROLLUP readings; `binding_is_complete`, `scan_partial_addresses` and
  `complete_key_domain`; `Region.filtered` vs `_PartnerFacts`;
  `null_on_padding`; a `_same_scope` helper; a `domain_regions` helper (two
  `region_of` sites filter, two assert).
- **Do NOT "simplify":** `group_rules.merge_terminal_siblings`'s
  `output_addresses` (empty grains would start merging);
  `group_graph._regraft_candidate(environment=None)` (one caller relies on the
  exact-grain fallback).
- **Long functions left:** `_split_root_dimension_clusters` (182 lines, no clear
  seam) and `_place_atom` (~375, could split by placement reason).

## 3. Architectural items

Worked 2026-10-09 (9ab1a4ea4, a5c25edec, 0c0ccc15f). Each was A/B'd on the
corpus, the four region batteries and the suite SQL capture, and no plans moved.

- **Four spellings of one address.** DONE (the cheap part, 0c0ccc15f):
  `BuildConcept.spellings` (authored + canonical) and `all_spellings` (+
  pseudonyms), plus `BuildDatasource.bound_spellings`. They replace the
  hand-rolled sets in scan_partials, partial_bridging, condition_routing,
  network_coalescing, network_build, value_set_join_upgrade and the
  nullable-proof filters. Keep each site at its width: widening a `spellings`
  site to `all_spellings` changes which partials, null tests and connector
  keys match.

  OPEN: a per-environment `AddressClass`. The three canonical maps differ in
  their root choice as well as in scope: `keyspace._canonical_addresses` takes
  the smallest member, `network_build._equivalence_map` the union-find root, and
  `join_resolution.build_canonical_address_map` uses `min` with alias origins
  ranked last. The roots reach join-graph node names and sort orders, so
  merging them would move plans. Design the scopes first. Nothing asserts that
  a canonical-keyed map is read by a canonical.
- **`RowsetDefinition`.** The cheap part is DONE (a5c25edec):
  `core.constants.rowset_alias_prefix` is the one spelling of the `_{rowset}_`
  scheme, which was hand-written in seven places. The definition object
  itself stays out. The outputs are still pending when `add_rowset` runs, so
  it would need hooks at three parse stages.
- **Anchor heuristics** (`grain_pin`). The cheap part is DONE (9ab1a4ea4):
  `select_anchors` returns None when nothing in scope pads (`_may_pad`), which
  skips every pin walk. `_held_beside` and `_co_held_only_beside` now share one
  bound-by-datasource map per expression. OPEN: deciding the pin after
  `build_keyspace` would make it exact. It is not a move, though: the
  keyspace, persisted-column matching and predicate pushdown all read the pin
  back from the lineage, so it would mean building twice or making the pin a
  lazy annotation. No bug traced. `_co_held_only_beside` has no focused test;
  only the TPC-DS q51 plan budget guards it.
- **"The filter-population claim is computed twice".** CLOSED, not a
  duplicate. The builder's `applied_atoms` chooses which request atoms a merge
  may claim. `MergeNode._join_proofs` finds which resolved datasets carry
  them, by walking the resolved tree. That walk is needed for two reasons:
  parents are rewritten and folded between the builder and `_resolve`, and
  non-builder callers (`select_merge_node`, `passthrough_if_materialized`,
  `basic`) feed the same path. A builder-supplied parent set would be a
  second mechanism. `_reads_partner` is a one-level check over resolved
  structure and is already minimal.
