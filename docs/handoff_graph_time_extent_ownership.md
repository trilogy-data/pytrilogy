# Handoff: graph-time extent ownership (RESOLVED 2026-08-21)

Implemented. The design and the shipped mechanism are documented in
`docs/extent_ownership.md`; this file keeps the record of what the
investigation established, since several of the findings are traps a future
change could walk back into.

## What shipped

`trilogy/core/processing/v4_helper/extent_ownership.py` elects one owner per
demanded `~` span on the group graph, before any node is built. The owner and
its ancestors may pad; every other group is extent-free and told so through
`environment.extent_free_spans`, which each `MergeNode` captures at
construction. Join typing consumes it three ways (a suppressed `~` grants no
row intent; inherited extension padding is absence rather than nullability; a
suppressed span is not a licensed key for hosting or the family-anchored
exemption). `_cover_groups_for_mandatory` reads the election instead of
re-deriving it.

`QueryDatasource.extent_free_spans` had to become part of the identifier: two
scans over the same sources that differ on extent routing are different
relations, and without the distinction a d1 (WHERE-phase) scan and its d0 twin
collided under one CTE name and concatenated their join lists (tpc-h q17,
caught mid-implementation).

Three refinements the fuzzer forced out, all in the "declining to extend
narrows the branch" direction:

- Two sides that both bind a suppressed span `~` are peers unless
  `partial_binding_sources` shows the same leaf binding behind both. Treating
  them uniformly dropped `(4, 13, None)` from the chasm family: a group with
  sales but no returns is a fact row, not an extension member.
- An extent-free merge marks its suppressed spans PARTIAL, which is what makes
  the FINAL assembly preserve the owner rather than INNER-join it away.
- `_tighten_joins_for_filtered_branches` must not treat such a branch as the
  population: a row missing from it is a member its facts never bound, not a
  row the request WHERE rejected. Scope that carve-out to spans actually
  suppressed (`deep_extent_free_spans`); keyed on plain partiality it flipped
  tpc-ds q44's rank join to LEFT for unrelated reasons.

## Measured

- Corpus (TPC-DS 109 + TPC-H 23, rendered against a PYTHONPATH-shadowed HEAD
  copy): **132/132 byte-identical**. The election is nonetheless live,
  active on 69 of those queries and suppressing 128 groups, because TPC's `~`
  keys are join axes nobody projects, so ownership changes no join there.
- `PruneInvisibleOuterJoins` firings across the corpus + the field report:
  **1 → 0**. Its only firing was the field report's dead subtree, which is now
  never manufactured.
- Family-anchored null-safe keeps: **61 → 52** (field report 6 → 0, q29
  18 → 15, q64 unchanged at 28). The machinery is still load-bearing for plans
  that genuinely split a span across owners; it is no longer what makes the
  common case correct.

## Findings worth not re-deriving

- The forked twin's padded rows are **not** load-bearing at the FINAL merge, as
  the earlier scoping believed. In `test_forked_with_state` the assembly reads
  the metric branch through a plain equality on `(order_id, product_id)`, and in
  `test_forked_with_status` through a plain equality on `item_id`; the branch's
  NULL-keyed rows die either way. What IS load-bearing is that `order_status`
  (a fact-grain CASE with an ELSE) evaluates on the padded rows of the branch
  that computes it, which is why the election lands on that branch. Ownership
  is per delivered output, and the "most downstream group exposing the span"
  ranking gets there without a separate value analysis.
- A naive plan-time parent drop (remove an aggregate parent whose outputs a
  sibling address-covers) still breaks the forked twin: in
  `test_forked_with_status` no single sibling covers the dim bucket's
  `(item_id, order_id, product_id)`, so dropping it leaves the aggregate joining
  on `product_id` alone and fanning out. The ownership fix does not need the
  drop: suppressing extent is enough to keep the redundant scan from padding,
  and the field report's `cheerful`/`highfalutin` subtree is never planned.
- Demand for a span must be read from the FINAL **output** addresses (plus FD
  closure), not from `merge_grain`. Merge grain includes join axes, and electing
  an owner for a key nobody projects is how tpc-h q17 acquired a routing split
  it had no use for.
