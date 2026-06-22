# Handoff: finish moving ROOT OUTER join keys onto the identity path

**Status:** SCHEDULED. The permutation suite + in-code distinction landed
(2026-06-22); this bug is captured as a strict xfail that the fix must flip.
Companion to `handoff_scoped_join_substitution_removal.md` (which concluded the
two mechanisms — substitution vs identity+coalesce — should both stay, split by
join semantics). This handoff is the concrete next slice that decision implies.

## The bug (one sentence)

Projecting the AUTHORED SOURCE key of a FULL scoped join on a ROOT key drops the
coalesce: `FULL JOIN a.kid = b.kid; SELECT a.kid` renders the raw `a_at.k`
column, so b-only rows get a NULL key — even though the `ORDER BY` on the same
concept coalesces. SELECT and ORDER BY disagree about what `a.kid` is.

Driver test (strict xfail today, must pass after the fix):
`tests/test_scoped_join_permutations.py::test_full_source_side_projection_coalesces`

## Root cause (traced through the merge node)

For a ROOT OUTER key both mechanisms run, inconsistently:
- The output concept stays projectable via IDENTITY (`alias_origin_lookup` entry
  built in `build.py` `_build_environment` ~3639, gated by `scoped_merge_sources`
  / `scoped_outer_identity_sources`).
- But its DATASOURCE BINDING is still SUBSTITUTED to the canonical (the key
  column is built through `_build_concept`'s `scoped_merge_map` swap, which is
  NOT skipped for datasource columns — `_source_identity_addresses` is empty
  during the normal datasource build).

So the resolved join keypair is `(b.kid, b.kid)` (same address). The merge node's
`_key_equivalence_classes` coalesce only fires for DISTINCT-address pairs, so it
never runs; only the canonical `b.kid` gets the FULL coalesce via
`full_join_concepts` in `resolve_concept_map`. The authored `a.kid`, projected
via its `alias_origin` single source, misses it.

Contrast — ROWSET OUTER keys work on either side: their bindings stay distinct
(`a.reg = b.reg`), so the equivalence-class loop coalesces BOTH. This is exactly
why `test_scoped_join_rowset_outer_blend.py` passes projecting the source side
but root keys don't. Verified by inspecting `joinkey_pairs` + `source_map`:
- root FULL: keypairs `[(b.kid, b.kid)]`, only `b.kid` source_map = {a_at, b_bt}
- rowset FULL: keypairs `[(a.reg, b.reg)]`, BOTH source_maps = {both sources}

## What the fix requires

Make a ROOT OUTER key's datasource binding keep its OWN address + a pseudonym to
the canonical (mirror the rowset/derived path), so:
1. The join renders distinct addresses (`a.kid = b.kid`) and the merge-node
   equivalence-class coalesce fires for both members.
2. `build_canonical_address_map` (join_resolution.py) already collapses
   pseudonym-equivalent addresses to one graph node, so join resolution still
   relates them — confirm it still emits ONE join, not two.

Touch points + expected fallout (spike-A measured ~13 failures when substitution
is disturbed — budget for contract rewrites):
- `build.py` `_build_datasource` / `_build_concept`: stop substituting the key
  column for an address in `scoped_outer_identity_sources` (add it to
  `_source_identity_addresses` during the datasource build, the way
  `_build_environment` already does for the alias_origin build).
- Render-side alias resolution (`dialect/common.py`): a projected concept with an
  `alias_origin` must resolve via the merged `source_map` (coalesce) when the key
  is a FULL member, not pin to its single origin source.
- FULL registry: `scoped_full_join_keys` is consumed through `canon_node`
  (pseudonym-collapsed) in `get_node_joins`, so it should survive distinct
  bindings — but re-derive it from the equivalence classes rather than
  `scoped_merge_map` (the handoff's "step 1") so it doesn't depend on the swap.
- Contracts to revisit: `test_scoped_join.py` buildenv asserts
  (`test_buildenv_left_join_marks_datasource_binding_partial` asserts the binding
  is the canonical address — that assertion changes if bindings keep identity),
  plus the FULL-key tests spike B flagged.

## Validate

`tests/test_scoped_join_permutations.py` (the oracle — the xfail must flip to
pass, nothing else regress), then the four scoped-join files, then the full unit
suite + tpc_ds modeling sweep (`-m "not adventureworks_execution"`).
