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

## Must fix (found 2026-10-06, older than this branch unless noted)

Repro fixture, data and SQL for all of these: `docs/handoff_union_join_anchor_where.md`.
`J = "union join ticket = r_filtered.r_ticket"`; ticket 100 has two sale lines,
103 is a 2001 sale with no return, 104 a return with no sale.

7. **Duplicate rows when both sides of a union join are projected under a
   WHERE.** `where year = 2001 select ticket, r_filtered.r_ticket $J` returns
   (100, 100) twice; it should return it once. Both relation members are
   outputs, so `axis_only_projection` (group_graph.py) sets
   `deduplicate_to_grain=False` and FINAL is a `whole_grain` MergeNode that
   never groups. The `year` column it carries for the WHERE is at sale-line
   grain, so ticket 100's two lines both survive. The "keep the fan-out" rule
   is meant for the sides' own rows: a column carried only for a WHERE must not
   add rows. Fix: dedup FINAL to outputs plus each side's row identity
   (excluding condition-only columns), or drop the condition column before the
   whole-grain projection.
8. **A count over an anchor-only key is NULL, not 0, under a WHERE.**
   `where year = 2001 select ticket, count(r_filtered.return_quantity) as c $J`
   gives (103, NULL). Without the WHERE it is (103, 0), and main returns a
   single row. The count is computed on the rowset side and left-joined back,
   so the 0-fill (`CTE.zero_fills_count`) is lost once the WHERE moves the
   merge. Check where the zero-fill is decided relative to a FINAL-hosted WHERE.
9. **DuckDB binder error: membership beside an anchor output.**
   `where r_filtered.r_ticket in (ticket ? year = 2001) select r_filtered.r_ticket, ticket $J`
   -> `Values list "juicy" does not have a column named "ticket"`. A CTE is
   asked for `ticket` that its projection never carried. That suggests the set
   feeder and the axis merge disagree on which side renders the coalesced key.
   Start from the CTE that reads `"juicy"."ticket"`.
10. **Two `~` facts binding one key return plan-dependent rows.** Model: sales
    and returns both `~order_id, ~item_id` and both binding `?date_id`; a
    complete `orders` table. `where week = 1 select order_id, customer` returns
    sale orders (1, 4) under one plan and return orders (1, 9) under another.
    The answer of the union of both facts' rows is (1, 4, 9). Which plan you get
    depends on whether the heal fires and which fact is chosen to source
    `date_id`. Decide whether a non-measure select over a key shared by two `~`
    facts reads the union of their rows; then make the row stream match.
    Probe harness:
    scratchpad `probe_read_partials.py`, model `sales_customer+orders_plain`.
