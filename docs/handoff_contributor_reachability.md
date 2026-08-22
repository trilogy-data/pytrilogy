# Design: one reachability answer for election and materialization

## OPEN 2026-08-22. Supersedes the thelook half of
## docs/handoff_invisible_contributor_joins.md

## The defect class

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
   that carries the key does not exist at election time. The gap is a missing
   PREDICATE, not a mis-ordered step.

A peer-fold pass (drop a contributor whose whole coverage a peer can carry once
widened) was prototyped and measured: **0 changed across 195 statements**,
because the precheck was written against projection boundaries — the very thing
that is wrong. Reverted, not committed.

## The change

Make reachability ONE pure, tested query, and have both stages call it.

    def carryable(node_or_group, concept) -> bool

Requirements, in priority order:

1. **Pure.** Today the only way to learn the answer is to call
   `_widen_scan_chain`, which mutates on success and partially mutates on
   failure. A predicate that cannot be asked without changing the plan cannot be
   consulted by election. This is the load-bearing piece.
2. **Agrees with the widener by construction.** A hand-written mirror of
   `_widen_scan_chain` was tried and disagreed with it (it returned False for
   cases the real function answers True). Do not mirror it — refactor
   `_widen_scan_chain` into `can_widen` + `apply_widen` over one shared walk, so
   drift is impossible. A test that asserts agreement on a corpus of nodes is
   the guard.
3. **Answerable from the GRAPH where possible.** Reachability is fundamentally
   about what the scans underneath bind, which the source graph knows before any
   projection is narrowed. Deriving it from built-node state is what created the
   boundary/subtree split. Graph-derived is the goal; node-derived agreement is
   the fallback.

Then election's candidate test becomes "exposes it OR can carry it", with an
explicit preference for a contributor already chosen for something else — a
contributor covering nothing another cannot carry should never be recruited.

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
