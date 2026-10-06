# Handoff: release follow-ups (2026-10-05)

Pick-up list from the pre-release audit of PR #702. Delete this file before merge
once each item is closed or moved to an issue.

## Landed this session

- e60efd42c: tidy of the two union join fixes (one pass for an aggregate's axis
  members, coalescing-only handle mates, hoisted fold veto). Generated SQL unchanged.
- f51c4aca4: a pin never heals a binding beside a `~` sibling the statement reads
  whose rows carry every killer (`_read_partials`). Fixes the dropped saleless
  return on a model where sales and returns are both `~`.
- bb176f77e: partial key fixtures honour their own bindings (`_ANCHORED` had a
  saleless return under a COMPLETE sales binding).
- (this commit) the `_anchors_dispensable` check (b) is removed: reading a complete
  anchor beside the heal is fine, since every fact row has its anchor row. TPC-DS
  q17, q24, q25, q29, q49, q50, q84 and the q83 sales-measure variant plan smaller.
  q29 and the q83 variant lose their FULL JOIN. Rows were compared at sf=1 against
  the old plans and are identical for all eight. The MySQL FULL-lowering tests now
  use `adhoc02.preql`. The q83 variant is `_q83_with_sales_measure.preql`.

## Open

1. **CI on the check (b) removal.** Confirm green across all dialect jobs. The
   local run covered engine, join_matrix, core and modeling (duckdb) only.
2. **`_merges_coalesced_sides` is broad** (`strategy_builder.py`). It vetoes
   folding any 2+ parent merge that outputs a coalescing-relation member. A suite
   SQL A/B showed it moves only its two target tests, so the breadth has no
   current cost. Narrow it to "parents carry different sides" if it ever shows up
   in a plan regression.
3. **Two meanings of "coalescing".** `concept_graph.coalescing_relation()` is
   STATEMENT-scoped `union join` only. `domain_graph.coalescing_relation_members()`
   is any declared INCOMPARABLE edge. Both are used side by side in the union join
   fixes. This predates the branch and has no known bug, but it is a trap.
4. **`_read_partials` uses ROOT derivation as its "must read" signal.** That is
   what keeps thelook q16's pair-grain rollup (derived `revenue`) from blocking
   the heal. A `~` sibling that holds a ROOT concept also reachable from `ds` by
   another path is treated as not read. Worth a probe with a `~` rollup that binds
   a root attribute.
5. **Anchor heal now assumes the declared model.** With check (b) gone, a
   complete anchor plus a fact row that has no anchor row (data violating the
   model) is dropped by the INNER merge. That is correct under the contract
   ("undeclared violations may be dropped or kept by plan shape"), but users with
   dirty `~`/complete declarations will see rows vanish under a pin. Consider a
   docs note in the user-facing modeling guide.
6. **Benchmark artifacts.** The modeling runs regenerate the timing logs and
   charts. Commit the regenerated set with the next artifact refresh.
