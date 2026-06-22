# Handoff: consolidate scoped joins onto the identity path (remove substitution)

**Status:** Steps 1-2 LANDED (2026-06-22), full suite green (4178 passed, 102
skipped, 2 xfailed). Step 3 (delete substitution) SCOPED OUT here — it is a real
project, not a deletion. Spikes below quantify exactly what blocks it.

## Background: two mechanisms relate scoped-join keys

A query-scoped `JOIN a = b` (and a global `MERGE`) relate two key concepts. There
are two ways the build does this:

1. **Substitution** (`Factory._build_concept`, build.py ~2720): the source address
   is *swapped* for the canonical target everywhere — concept refs, grain
   components, datasource column bindings. So `orders.customer_id` builds AS
   `customers.customer_id`; a dependent property keyed on the merged key inherits
   the canonical grain. Driven by `scoped_merge_map` (built from BOTH query JOINs
   and `environment.merges`). The FULL path also keys off `scoped_full_join_keys`
   / `scoped_key_merge_map`, which are derived from the substituted canonical.

2. **Identity + pseudonym + coalesce** (the "new shape"): the collapsed-away key
   keeps its OWN address, gets a pseudonym back to the canonical (so discovery
   connects the subgraphs), and the merge node coalesces the distinct physical
   columns. This is what makes OUTER joins correct when the two sides have
   different row populations (e.g. different per-rowset WHEREs) — substitution
   would render one side's column and NULL the rows present only on the other.

INNER hides the difference (matched rows have `a == b`, no unmatched rows). LEFT/
FULL expose it.

## What landed (steps 1-2)

The goal was: make the identity path TOTAL for OUTER joins so the rowset-only
special-case (`_is_rowset_pair`/`_distinct_base_rowsets`) could be deleted.

1. **Equivalence-class key coalescing** (`merge_node.py`, `_key_equivalence_classes`
   + the loop after `resolve_concept_map`). Pairwise routing left an N-way class
   (`a.k=b.k`, `c.k=b.k`) only partly coalesced. Now union-find groups every
   OUTER concept-pair into a class and points every member's `source_map` at the
   class-wide source union → the output key renders `coalesce(all members)`.

2. **Removed the build-side gate** (`build.py`, the `scoped_joins` loop). Deleted
   `_is_rowset_pair`; every binding-keyed (ROOT/ROWSET) OUTER key now gets the
   identity treatment (`scoped_merge_sources` + pseudonym), tracked in
   `scoped_outer_identity_sources` (renamed from `scoped_rowset_identity_sources`)
   and subtracted from `scoped_partial_derived` (partiality stays with the
   binding/rowset machinery). `_is_binding_keyed` (derived-vs-bound) is the only
   remaining structural branch — derived keys still need `scoped_partial_derived`
   (no binding to carry `Modifier.PARTIAL`).

3. **FULL ON-clause coalesce across all left sources** (`join_resolution.py`,
   `resolve_join_order_v2` dedup ~line 317). The dedup dropped a redundant left
   source; for a FULL-join key every left source must survive so `_build_joinkeys`
   emits `coalesce(l1.k, l2.k) = r.k`. Gate: `is_full_key = all_connecting_keys &
   full_join_keys`. Without this, a 3-way FULL split rows present on only one side.

Tests: `test_scoped_join_rowset_outer_blend.py` (row-level INNER/LEFT/FULL incl.
projecting the optional-side key), `test_full_join_three_sources` now exercises the
identity path. No regressions in the full unit + tpc_ds modeling suite.

## Why step 3 (delete substitution) is blocked — measured

Spike A — disable the swap in `_build_concept` entirely → **13 failures**:
`test_buildenv_*` (assert the address swap + dependent grain collapse),
`test_multi_way_inner_join_merge_parity[3way/4way]`,
`test_chained_equality_join_matches_pairwise`, `test_chained_full_join_all_buckets`,
`test_disjoint_inner_and_full_groups`, several FULL key tests. (q44 PASSES without
it — its window-remap fear from older handoffs is stale.)

Spike B — disable the swap ONLY for `scoped_outer_identity_sources` → still
**4 failures**, all FULL: `test_full_join_registers_canonical_key`,
`test_full_join_two_keys_single_join`, `test_two_fact_full_join_key_only_is_union_of_facts`.
The FULL **canonical-key registry** (`scoped_full_join_keys`, `scoped_key_merge_map`)
is derived from the substituted canonical, so removing the swap for outer keys
breaks FULL even though identity also runs.

Conclusion: substitution is still load-bearing — and *correct* — for the cases
where the two keys are genuinely the same value (INNER, `merge`, FULL canonical
registry) plus dependent-grain collapse. The principled end state we now have:

- **substitution** = "these keys are genuinely identical" (INNER / merge / FULL
  registry / dependent grain collapse)
- **identity + coalesce** = "relate but keep distinct" (LEFT / RIGHT / FULL across
  distinct columns)

## What a real step 3 requires (if pursued)

1. **Re-derive the FULL canonical-key registry from the pseudonym/identity classes**
   instead of from substitution. `scoped_full_join_keys` / `scoped_key_merge_map`
   currently assume the canonical address is the single substituted one; they'd
   need to recognize a pseudonym equivalence class (build_canonical_address_map
   already computes these at join-resolution time — lift that earlier).
2. **Move dependent-grain collapse onto identity.** A property keyed on a merged
   key must still resolve across the joined datasources without rewriting its
   grain to the canonical address — i.e. grain satisfaction has to follow
   pseudonyms. This is the riskiest piece (the `test_buildenv_multiway` grain
   assertion is the canary).
3. **Decide global `merge` separately.** `scoped_merge_map` mixes query JOINs and
   `environment.merges` (query_processor.py ~648). Global merge means "a IS b
   everywhere" and its contract (`test_buildenv_*`) asserts the swap — it may want
   to KEEP substitution even if joins move off it. If so, thread join-derived vs
   merge-derived entries separately into the Factory (currently indistinguishable).
4. **Rewrite the `test_buildenv_*` contracts** to assert identity+pseudonym rather
   than address-swap, for whatever subset moves off substitution.

Recommended order: (1) FULL registry off substitution → re-run; (2) grain collapse
off substitution → re-run (watch `test_buildenv_multiway`); (3) only then flip the
`_build_concept` swap off for joins, keeping it for `environment.merges` if its
contract is kept. Each step is independently testable against the spike failure
lists above.

## Key files
- `trilogy/core/models/build.py`: `Factory.__init__` scoped-joins loop
  (`scoped_merge_sources`, `scoped_outer_identity_sources`, pseudonym augmentation),
  `_build_concept` substitution (~2720), `scoped_full_join_keys`/`scoped_key_merge_map`
  (~2297-2316), grain normalization (~3359), alias_origin population (~3639).
- `trilogy/core/processing/nodes/merge_node.py`: `_key_equivalence_classes` + the
  source_map class-coalesce loop.
- `trilogy/core/processing/join_resolution.py`: `resolve_join_order_v2` FULL-key
  dedup, `get_node_joins`, `build_canonical_address_map`, `reduce_concept_pairs`.
- `trilogy/dialect/common.py`: `_build_joinkeys` (renders coalesce when multiple
  left pairs share a right concept).
- Tests: `tests/test_scoped_join.py` (buildenv contracts + N-way), 
  `tests/test_join_merge_parity.py`, `tests/test_scoped_join_rowset_outer_blend.py`.
