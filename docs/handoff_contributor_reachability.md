# Design: one reachability answer for election and materialization

## OPEN 2026-08-22. Supersedes the thelook half of
## docs/handoff_invisible_contributor_joins.md

## Summary

The redundant contributor is NOT an election error. Election is correct on the
inputs it has; the contributor nodes are then reconstructed, and the replacement
gains the concept as a join key, which is what strands the contributor. The fix
belongs immediately after join-key materialization. Sections below record the
four hypotheses that were measured and rejected getting there — check them before
re-deriving one.

## The framing that motivated this (partly wrong, kept for the trace)

Two stages ask the same question and get different answers:

- **Election** (`_cover_groups_for_mandatory`, `v4_helper/strategy_builder.py`)
  asks *which built group EXPOSES this concept* — it reads `output_concepts`.
  That is a **projection-boundary** question.
- **Materialization** (`_widen_merge_join_keys` -> `_widen_passthrough_group` ->
  `_widen_scan_chain`, same file) asks *which parent can be WIDENED to carry
  this concept* — and descends **past** that boundary into the subtree, up to
  `_JOIN_KEY_CHAIN_LIMIT`. That is a **subtree-reachability** question.

Nothing reconciles them. Election therefore recruits a contributor for a concept
that a already-chosen contributor's subtree could have supplied, and the extra
contributor joins in to render nothing. `PruneInvisibleOuterJoins` then deletes
the join, which is why the rule cannot be retired.

This is the second cause behind the invisible-contributor joins. The first (a
property's grain key promoted to a hard search terminal) is fixed; see
`_concepts_with_grain_keys` and the sibling handoff.

## The live case, fully traced

`tests/modeling/thelook_duckdb/adhoc04.preql`. Election picks three:

    grp:group_to:d0:order.id      -> ['order.id']                      <- highfalutin
    grp:aggregate:d0:local.id     -> ['id','revenue','margin','total_order_revenue']
    grp:root:root:*:dim:local.id  -> ['user.id','product.id']

`highfalutin` wins `order.id` on the most-downstream sort. It contributes
nothing else, and the FINAL merge joins it on `order_id` while rendering no
column from it.

### Three things that are NOT true (each cost a probe; do not re-assume)

1. **"`juicy` gains `order.id` late, so election just needs to wait."** False.
   The elected node `grp:root:root:*:dim:local.id` is a MergeNode whose
   `renderable_addresses` is exactly `{product.id, user.id}` at election time
   **and still at merge time**. It never becomes able to render `order.id`.

2. **"The rendered `juicy` CTE is the elected node."** False. The FINAL merge
   receives FOUR parents where election chose three. The node that gains
   `order.id` is UNREGISTERED — a dedup GroupNode wrapper constructed around the
   elected MergeNode after election. That wrapper is what renders as `juicy`,
   and it reaches `order_id` by descending into the `order_items` scan, past the
   elected node's narrowed projection.

3. **"Move join-key materialization earlier."** Cannot work as stated: the node
   that carries the key does not exist at election time, and the elected node
   cannot be widened to the key by any path (see the measurement below).

4. **"Election needs a reachability predicate."** Measured and rejected — the
   contributor it would point at cannot carry the concept either.

A peer-fold pass (drop a contributor whose whole coverage a peer can carry once
widened) was prototyped and measured: **0 changed across 195 statements**.
Reverted, not committed.

## The election is NOT wrong. Measured, 2026-08-22.

The obvious fix — give election a reachability predicate so it sees that the
already-chosen `juicy` could carry `order.id` — does not apply, because **juicy
cannot carry it**. Asked directly at election time, every path refuses:

    BEFORE outputs=['product.id', 'user.id']
      _widen_scan_chain(GroupNode force_group=True)  -> False
      _widen_scan_chain(SelectNode products)         -> False
      _widen_scan_chain(SelectNode users)            -> False
    AFTER _widen_passthrough_group  outputs=['product.id', 'user.id']   (unchanged)

`order_id` lives under the force_group GroupNode arm, and neither widener will
cross it. So a third contributor for `order.id` is genuinely REQUIRED on the
inputs election has. Set cover does not help either: `user.id` is exposed only by
`juicy`, the three measures only by `uneven`, and neither exposes `order.id`.

A pure mirror of `_widen_scan_chain` was also checked against the real function
on every (node, concept, depth) triple it is called with across the corpus:
**12/12 agreement, no drift**. Purity is achievable and the mirror is sound —
it simply answers False here, correctly.

## Where the redundancy actually comes from

Between election and the merge, the contributor nodes are **reconstructed**. The
FINAL merge receives four parents where election chose three, and the extra one
is a dedup wrapper around the elected MergeNode. That wrapper has a different
parent chain, and `_widen_merge_join_keys` successfully widens it with the merge's
join keys — `keys=['local.id','order.id']`, gaining `order.id`.

That is the whole mechanism, and the rendered SQL confirms it: the FINAL
projection reads `"juicy"."order_id"` and `"juicy"."id"`, i.e. BOTH from the
wrapper, not from the contributors elected to supply them. `uneven` survives
because it still owns revenue/margin/total; `highfalutin` was elected for
`order.id` alone, so once the wrapper carries `order.id` it renders nothing.

**The redundancy is created by node reconstruction after election, not by a bad
election.** No predicate available at election time can prevent it, because the
node that gains the capability does not exist yet and does not inherit the
elected node's parents.

## The change

Fold contributors AFTER join-key materialization, before the merge node is
constructed: once `_widen_merge_join_keys` has widened the parents, a parent
whose entire elected coverage is now present on another parent is redundant and
should be dropped there. That is the first moment the information exists, and it
is still the planner — the join is never built, rather than built and deleted.

Do NOT attempt it earlier. Two earlier placements were prototyped and measured:

- A peer-fold before materialization, keyed on projection boundaries:
  **0 changed across 195 statements** (it cannot see the capability yet).
- Giving election a reachability predicate: inapplicable, per the section above.

Note this makes the planner fold and `PruneInvisibleOuterJoins` structurally the
same test at two different times. That is expected — the point is to move it to
the moment the plan can still avoid building the join.

## Bar for the change

- `prune_ablation.py` (195 statements, see the sibling handoff — it MUST include
  `tpc_ds_duckdb/aggregates/`) reports **0** statements whose render depends on
  `prune_invisible_outer_joins`, with a no-op control leg proving the harness.
- tpc corpus render 132/132 byte-identical; fuzzer 228/228.
- Full suite green. Note it is a weak backstop here: it passed 8533 with a
  3.8x plan regression in place, caught only by a committed size artifact.

At that point `PruneInvisibleOuterJoins` is provably dead and retires with its
flag — which is the actual goal, since the rule is expensive code that can
introduce bugs that should not need to exist.

## Reproducing the traces

Monkeypatch, do not edit: wrap `_cover_groups_for_mandatory` to snapshot
`built` (id -> gid), then wrap `_widen_merge_join_keys` to diff each parent's
outputs before/after and report the gid. Parents absent from the snapshot are
post-election wrappers — that asymmetry is the whole story and is invisible if
you only log addresses.
