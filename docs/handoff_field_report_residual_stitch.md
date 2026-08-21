# Handoff: output-invisible null-safe stitches survive in the field-report plan

## RESOLVED 2026-08-21: whole-statement tripwire restored

`tests/engine/test_duckdb_partial_fk_field_report.py::test_field_report_select`
again asserts `"is not distinct from" not in sql` over the WHOLE rendered
statement. The surviving `cheerful` stitch (and the `highfalutin` subtree
feeding it) no longer renders at all: the metric branch is consumed through a
plain-equality LEFT join on `item_id` from the full-extent anchor, so its
padded contributor is output-invisible and the optimizer removes the join.

## Why the fix is consumer-side (optimizer), not plan-side

The gate decision stands as shipped: `_gate_nullable_by_host` keeps
family-anchored no-basis pairs, because the forked reunions
(`tests/engine/test_duckdb_partial_key_assembly.py`) render the byte-same
merge shape and their padded rows DO feed the output (consumed via a
null-safe INNER on `item_id`). The two cases separate only at the
consumption edge two CTEs downstream, which is post-multi-node state. A
plan-time drop of the axis contributor was prototyped and breaks the forked
twin; the necessity of the contributor is decided by the final assembly's
consumption mode, elected after the branch builds.

## The fix (three steps, all in the optimizer layer)

1. `_inner_pair_rejections` (`trilogy/core/optimizations/join_upgrade.py`)
   now harvests directional joins: a rendered plain equality forces non-null
   only on a side whose unmatched rows the join discards: both sides of an
   INNER, the RIGHT of a LEFT_OUTER, the LEFT of a RIGHT_OUTER. Null-safe
   pairs still prove nothing. `_external_forced_map` propagates unchanged;
   the final assembly's `concerned LEFT yummy ON item_id` lands forced
   `item_id` on the metric branch.
2. The existing `_downgrade` narrowing then turns the stitch's FULL into
   LEFT_OUTER (`left_only` holds the forced key), and narrows the fact
   merges upstream (`orders FULL items` becomes RIGHT OUTER, and the
   `UpgradeOuterFromKeySetEquivalence` declared-subset match upgrades
   `items LEFT products` to INNER once the FULL is out of its way).
3. `PruneInvisibleOuterJoins`
   (`trilogy/core/optimizations/prune_invisible_join.py`, flag
   `prune_invisible_outer_joins`): a LEFT_OUTER join whose right side has no
   rendered reference in the consumer (no visible output column, condition,
   ORDER BY, existence, semi-join feeder, or other-join endpoint) and whose
   right grain sits within the join's right key addresses is a row-identical
   no-op (left rows are preserved either way and multiplicity is at most
   one), so the join is removed and the driver's irrelevant-CTE filter
   sweeps the orphaned producer. Null-safety on the removed pairs dies with
   the join. Modifier STRIPPING was a dead end: the consumer proofs reach
   `item_id` only, never the stitch keys `order_id`/`product_id`, so no
   sound non-null proof exists for `SimplifyNullSafeJoins` to consume.

## Validation (2026-08-21)

- Pinned suites green in one process: field report (restored assert),
  partial_key_assembly, multi_fact_nullable_fk_extent,
  join_padding_provenance, join_matrix, generators/test_utility (226).
- Fuzzer 228/228 including the `padding_provenance` family.
- Corpus A/B (three legs, one process, no-op control clean): 1 of 154
  queries changes. thelook q19 upgrades `products LEFT order_items` to INNER
  (directional proof) and drops a dead unique-key LEFT join to an order
  group; row-validated. All three modeling suites pass.
- Trap found on the way: source_map tokens for an INLINED datasource are its
  render alias, not the CTE name. Matching on name alone made the reference
  check vacuous and the rule fired on 37 corpus queries, several with the
  right side still referenced (binder errors caught by the modeling run,
  never by generation). `_right_source_keys` now includes
  `cte.source_key_for(right)` and skips on token collisions.

## Follow-up direction (not this fix)

Graph-time extent ownership: elect the licensed span's owner on the flow
graph before branches build and propagate it downward, so extent is
manufactured exactly once and the reunion machinery (family-anchored
null-safe stitches) becomes unnecessary rather than gated. Constraint
discovered while scoping: ownership is per delivered output, not per key:
`test_forked_with_status` pins `order_status = 'LATER'` on extension rows, a
fact-grain CASE that evaluates only in the row-bearing branch, so extent
must flow through branches computing such attributes. See the discussion in
docs/handoff_multi_fact_nullable_fk_extent.md.
