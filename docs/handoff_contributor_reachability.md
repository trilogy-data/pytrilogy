# One reachability answer for election and materialization

## RESOLVED 2026-08-22. Closes the thelook half of
## docs/handoff_invisible_contributor_joins.md, and retires
## `PruneInvisibleOuterJoins` with its flag.

## Summary

The redundant contributor was NOT an election error. Election is correct on the
inputs it has; the contributor nodes are then reconstructed, and the replacement
gains the concept as a join key, which is what strands the contributor. The fix
is `_fold_covered_contributors` in `v4_helper/strategy_builder.py`, called
immediately after `_widen_merge_join_keys` at the FINAL merge and before the
`MergeNode` exists. The join is never built.

## Why not earlier

Two stages ask a similar question and get different answers:

- **Election** (`_cover_groups_for_mandatory`) asks *which built group EXPOSES
  this concept*: it reads `output_concepts`. A projection-boundary question.
- **Materialization** (`_widen_merge_join_keys` -> `_widen_passthrough_group` ->
  `_widen_scan_chain`) asks *which parent can be WIDENED to carry this concept*
  and descends **past** that boundary into the subtree. A subtree-reachability
  question.

Reconciling them at election time does not work, and four measurements say why.
`tests/modeling/thelook_duckdb/adhoc04.preql` is the live case; election picks:

    grp:group_to:d0:order.id      -> ['order.id']                      <- highfalutin
    grp:aggregate:d0:local.id     -> ['id','revenue','margin','total_order_revenue']
    grp:root:root:*:dim:local.id  -> ['user.id','product.id']

1. **"`juicy` gains `order.id` late, so election just needs to wait."** False.
   The elected `grp:root:root:*:dim:local.id` is a MergeNode whose
   `renderable_addresses` is exactly `{product.id, user.id}` at election time
   **and still at merge time**. It never becomes able to render `order.id`.
2. **"The rendered `juicy` CTE is the elected node."** False. The node that
   gains `order.id` is UNREGISTERED: a wrapper built around the elected node
   after election, by `_wrap_for_grain`/`_fresh_final_root_projection`. That
   wrapper is what renders as `juicy`, and its different parent chain is what
   lets `_widen_scan_chain` reach `order_id` in the `order_items` scan.
3. **"Election needs a reachability predicate."** Measured and rejected: asked
   directly at election time, every widening path off the elected node refuses,
   because `order_id` lives under a `force_group` arm neither widener crosses.
   A pure mirror of `_widen_scan_chain` was checked against the real function on
   every (node, concept, depth) triple across the corpus (12/12 agreement), so
   purity is achievable; it simply answers False here, correctly.
4. **"Detect it while SOURCING the root."** Measured 2026-08-22 and rejected,
   though this is the near miss worth knowing. `_relevant_root_preserve_keys`
   IS handed `{local.id, order.id}` and drops `order.id` (not in the root's
   outputs, no FD to them, not a bucket member, no authored relation). Forcing
   `order.id` back into `_fresh_final_root_projection` costs nothing (the root
   scan already reads `order_items` and the `juicy` CTE renders byte-identical
   ) but it does NOT remove the invisible join: with the fold disabled the plan
   still joins `highfalutin`. Election already committed it, so a fold is
   needed regardless; sourcing the key earlier only moves when the sibling gains
   it. Widening also has a property re-sourcing lacks: it adds the key to an
   already-built node, so it cannot drag a new table in the way the sibling
   `own_join_keys` narrowing exists to prevent.

A peer-fold BEFORE materialization was also prototyped: **0 changed across the
corpus**; it cannot see the capability yet.

## The rule

`_fold_covered_contributors` drops a merge parent when all of:

- it descends from a group elected to cover a mandatory concept (an axis-only
  contributor from `_add_relation_axis_contributors` /
  `_add_partial_completion_contributors`, or an elected extent owner, is
  deliberately column-invisible and is never folded);
- every needed address it carries is rendered by a surviving sibling, no less
  completely (a sibling holding it PARTIAL cannot stand in for a complete one);
- dropping it does not split the survivors into more join components: it may
  be the only parent bridging two siblings that share no axis with each other;
- the addresses it shares with the survivors are **exactly its own grain**.
  Fewer and the join fans out; more and the join CONSTRAINS, because the contributor
  is pairing columns (`item_id`->`order_id`) the survivors would otherwise pair
  freely, and dropping it changes rows even though every column still renders.
  This one is not optional: without it
  `tests/engine/test_duckdb_partial_key_assembly.py` returns fanned-out rows,
  and its weaker form (grain merely a subset of the shared addresses) trips the
  keyless-join guard on the same file;
- it restricts no rows the survivors don't already restrict (`conditions`
  compared with `condition_implies`; an existence subselect or a row limit is
  not comparable, so it refuses).

## Evidence

- Fold firings across the 203-statement corpus (tpc_ds_duckdb incl.
  `aggregates/`, tpc_h, tpc_ds, thelook_duckdb, hackernews, ncaa, gcat,
  the_look, faa): **exactly 1**, adhoc04's `highfalutin`.
- Corpus render with the fold on vs off, one process: **0 differing**, with a
  no-op control leg proving the harness reports 0.
- The prune ablation (same corpus, rule on vs off) went **1 -> 0**; the same
  harness reported 1 before the fold, which is what makes the 0 meaningful.
- Fuzzer 228/228. Full suite green.

`PruneInvisibleOuterJoins`, its flag, and its test are gone.
`tests/optimization/test_no_invisible_contributor_joins.py` replaces them with
a structural assertion: every CTE the final statement joins must appear in its
projection, over the field report and the thelook adhocs.

## Reproducing the traces

Monkeypatch, do not edit: wrap `_cover_groups_for_mandatory` to snapshot
`built` (id -> gid), then wrap `_widen_merge_join_keys` to diff each parent's
outputs before/after and report the gid. Parents absent from the snapshot are
post-election wrappers, and that asymmetry is the whole story and is invisible if
you only log addresses.
