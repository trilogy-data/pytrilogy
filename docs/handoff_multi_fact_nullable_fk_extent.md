# Handoff: a nullable fact FK silently changes the dimension extent of a two-fact select

## RESOLVED 2026-08-21 — contract settled and implemented

Selecting one dimension property beside an aggregate from each of two facts
used to return a different, asymmetric set of dimension members depending on
whether one fact's FK was declared nullable. The contract is now written down
and enforced by `tests/engine/test_multi_fact_nullable_fk_extent.py` and the
fuzzer case `padding_provenance/padded_and_unpadded_sides`.

## The settled contract

A non-partial FK binding is an EQUAL-domain claim (the fact covers the key's
full domain, docs/subset_union_join_design.md). A `?` on the FK weakens that
claim to "some subset, plus a NULL group" — NULL is a value the join equality
drops, so the fact's coverage of the dimension is no longer total.

For `select gname, sum(vamt) as v, sum(eamt) as e` over two facts:

| FK declarations | extent |
| --- | --- |
| both required | INNER — members both facts cover (each fact claims the full domain; data violating a claim loses the violating rows) |
| any `?` | the UNION of both facts' members, each side's exclusive members padded with NULL in the other aggregate — symmetric in the two facts |

Padding NULLs are join manufacture, not values, so a padded member never
null-pairs with anything on the other side; a genuine NULL-key group (rows
whose FK is NULL) survives as its own padded row.

## The fix (two changes)

1. `get_join_type` (`trilogy/core/processing/join_resolution.py`): when
   exactly one side of a non-partial join is nullable, the join used to
   preserve the nullable side directionally — silently dropping the other
   side's exclusive members. At a *grain-aligned merge* (both sides' grains
   sit within the connecting keys, i.e. both are complete group-sets at the
   merge grain) the join is now FULL, preserving both sides padded. The same
   applies to the both-nullable branch after its host-asymmetry guard. A
   fact-to-lookup join (grains not aligned) keeps the directional behavior.

   Three boundaries keep the FULL from over-firing (`extent_null_addresses`
   plus the solid-key test in `_is_nullable_grain_aligned_merge`):

   - only `?`-rooted nullability counts — a `?` leaf binding, or padding
     from an outer join whose ON keys carry value NULLs. The
     `find_nullable_concepts` widening that marks a whole side merely
     JOINED on a nullable condition is bookkeeping, not extent (an INNER
     join introduces no NULLs), and amplifying it flipped q98's solid item
     rejoin to LEFT;
   - padding from partial-driven (`~`) preserving joins never counts —
     extension families ride the host machinery, and claiming their padding
     re-preserved rows that machinery already keeps exactly once
     (duplicated extension rows in tests/engine/test_duckdb_partial_key_assembly.py);
   - when the connecting keys free of extent nullability still cover one
     side's grain, that side's intact EQUAL claim makes the pairing total
     and the ordinary typing stands (a value-nullable attribute riding a
     solid key weakens nothing).

2. `UpgradeOuterFromKeySetEquivalence`
   (`trilogy/core/optimizations/value_set_join_upgrade.py`): a null-safe pair
   used to be treated as unconditionally safe to upgrade to INNER once both
   sides passed `_complete_distinct`. Null-safety pairs the NULL groups but
   says nothing about the non-null values: a join-padded side carries the
   IMAGE of the key — a subset of the value space. The upgrade is now vetoed
   when a side's key nullability is join padding (`nulls_are_values` is
   False) and the two sides' padding does not share provenance
   (`_padding_sources`). An EQUAL declaration (`merge a into b`) overrides
   the veto — narrowing trusts the declaration by documented contract.

## Adjacent decision: the field-report stitch tripwire (2026-08-21)

`test_field_report_select` used to forbid `is not distinct from` anywhere in
the rendered SQL. That collided with the no-host-basis gate behavior the
forked partial-key-assembly row tests require: when the planner manufactures
the same `~` extension member in two aggregate branches, the mid-plan reunion
of its halves must pair null-safely on the padded keys or the member's row
splits/duplicates. Source-identity provenance cannot separate the two shapes
(re-manufactured families have disjoint sources); FAMILY identity can, and is
visible in the join itself:

- a reunion join also anchors on a licensed `~` family key, so the pairing is
  member-to-member (`family_anchored` in `_gate_nullable_by_host`);
- a bare null-safe pair with neither a family anchor nor shared provenance
  pairs "missing" with "missing" across unrelated trees and now STRIPS at
  plan time (this killed the field report's unjustified
  `INNER ... item_id is not distinct from` stitch).

One anchored stitch initially remained mid-plan in the field report (the
product-family reunion in the `cheerful` join). It is inert in that query:
the final assembly re-anchors extension rows from the dimension span and
consumes the metric branch through a plain-equality LEFT join, so every
padded row in that subtree is output-invisible. The optimizer now proves
that invisibility (directional pair-rejection harvest in
`UpgradeJoinOnGuards`) and removes the dead join outright
(`PruneInvisibleOuterJoins`), restoring the whole-statement tripwire; see
docs/handoff_field_report_residual_stitch.md for the resolution.

## Corpus footprint

12 grain-aligned-FULL firings and 1 veto firing across the tpc-ds/tpc-h/
thelook corpora; 4 tpc-ds queries render different SQL (q04, q51, q59, q64),
all row-validated against their references. q51's merge of the two
cumulative-window branches now renders the reference query's own
`FULL OUTER JOIN` shape. The plan-shape boundary is pinned by
`test_value_nullable_attribute_does_not_degrade_solid_key_joins` (q98 must
render outer-join-free — row results cannot see that regression). The full
`padding_provenance` fuzz family passes, including the re-added
`padded_and_unpadded_sides` case with a union-of-members oracle.
