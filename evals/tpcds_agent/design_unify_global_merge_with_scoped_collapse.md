# Design note: unify global `merge` onto the build-time scoped collapse

**Status:** Phase 1 IMPLEMENTED (INNER merges) 2026-06-12. `~` (LEFT/enrichment)
merges remain on the pseudonym path — that's Phase 2. Prereq: the scoped-join
bridge bug is FIXED (`source_scoring.py` bridge guard — see
`bug_scoped_join_operand_order_noncommutative.md`).

## The bug (recap)

Two facts reaching one shared transitive attribute (`week_seq`) through two
different date dims. Scoped `join` was correct; the equivalent global `merge`
fanned out (3 rows where 2 is correct), because the two forms resolved through
DIFFERENT machinery:
- **Scoped join** → `build.py` `Factory` collapses source→canonical via
  `scoped_merge_map` *before* `generate_graph` → one graph node → normal
  `resolve_subgraphs` (with the bridge guard).
- **Global merge** → `merge_concept` set up *pseudonyms* (two nodes) →
  `resolve_weak_components`/`extract_ds_components` split the canonical concept
  into its own component, sourced standalone + LEFT-joined → no constraint → fanout.

## Key semantic distinction (`~`)

`merge X into ~Y` vs `merge X into Y` are NOT the same:
- `merge X into Y` (no `~`) → **INNER**, symmetric equivalence. (This is the
  bridge-bug shape.)
- `merge X into ~Y` (`~` = `Modifier.PARTIAL`) → **LEFT / enrichment**: `Y` is
  the preserved base (e.g. a `date_spine(...)`), `X` maps onto it (missing rows
  become gaps). Collapsing this as INNER is wrong and breaks resolution
  (`tests/modeling/gcat/test_gcat.py` `decom_spine`). The first attempt did
  exactly that and was reverted.

## Phase 1 (implemented): migrate INNER merges only

1. **`Environment.merges: list[(src, tgt, JoinType)]`** — `merge_concept` appends
   `(source, target, INNER)` ONLY when `Modifier.PARTIAL not in modifiers` (i.e.
   no `~`). `~` merges are skipped → stay on the pseudonym path. `add_import`
   copies `merges` (namespaced) so imported merges carry through.
2. **`get_query_node`** unions `environment.merges` into `caches.scoped_joins`
   (idempotent; nested sub-selects inherit) → the existing `scoped_merge_map`
   collapse folds the merged concepts into one node, same path scoped joins use.
3. **`build.py` `_build_environment`** (~L3540): build the scoped-merge source as
   ITSELF via `base.alias_origin_lookup.get(source_addr)` first, falling back to
   `concepts.data`. Required because a global merge rewrote
   `concepts[source]=target`, so `concepts.data[source]` returns the target and
   the source identity would never be built (graph-gen `invalid pseudonym`). A
   scoped join has no such alias entry → falls back → unchanged.

Result: INNER-merge bridge shape now matches the scoped join (regression test
`test_global_inner_merge_bridge_matches_scoped_join`); date-spine `~` merges and
all merge/modeling/parity suites stay green.

## Phase 2 (future): migrate `~` (LEFT) merges too

Map `~` merges to the LEFT path of the same collapse and retire the
merge-specific handling in `resolve_weak_components`/`extract_ds_components`.
Sketch: for `merge X into ~Y`, emit `(Y, X, LEFT_OUTER)` so the index makes `Y`
the preserved canonical and `X` partial (mirrors scoped `LEFT JOIN a=b`: source
preserved, target partial). Validate against gcat date-spines and confirm the
weak-component path is unreachable for merges (instrument before deleting — per
the "validate dead code by instrumentation" rule). Higher risk; its own PR.

## Regression surface

`tests/test_join_merge_parity.py`, `tests/modeling/gcat` (date spines),
`tests/modeling/{mobs,usa_names}`, full `tpc_ds`, `tests/generators/test_merge_*`,
`tests/optimization/test_merge_basic.py`, `tests/modeling/test_merge_discovery.py`.

## Known limitation

`Environment.merges` is a runtime field, not serialized in `to_dict`/`from_dict`.
Envs round-tripped through serialization (rather than re-parsed) would lose the
INNER-collapse pairs and revert to the pseudonym path for those merges. Add to
serialization if/when env caching needs it.
