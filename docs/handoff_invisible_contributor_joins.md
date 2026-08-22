# Handoff: joins to contributors that render no column

## OPEN 2026-08-22, for an agent with clean context

`PruneInvisibleOuterJoins` was expected to decay to dead code once extent
ownership landed (docs/extent_ownership.md). It did not. An ablation over every
modeling statement shows it still fires 3 times and changes 2 statements, and
neither is an extent problem. This is the remaining defect class: the planner
joins in a contributor whose columns nothing renders, and an optimizer pass
deletes the join afterwards.

## Reproduce

`prune_ablation.py` (recreate it; ~90 lines) renders every `query*.preql` and
`adhoc*.preql` under tpc_ds_duckdb, tpc_h, tpc_ds, thelook_duckdb, hackernews
and ncaa, plus gcat's inline aggregate query and the field report, once with
`CONFIG.optimizations.prune_invisible_outer_joins = True` and once with False,
in ONE process, and diffs the two result sets. Current result:

    statements rendered: 194
    rule firings with flag ON: 3
    statements that DIFFER: 2
      gcat:aggregate                              1016 -> 1249
      tests/modeling/thelook_duckdb/adhoc04.preql 2195 -> 2278

Note the coverage: the standard corpus render is tpc_ds + tpc_h only (132
queries) and reports 0 firings. Both live cases are outside it.

## The two cases

**gcat, `test_gcat.py::test_aggregate_optimization`.** With the rule off the
plan carries

    LEFT OUTER JOIN "launch_info" AS "launch_info"
      ON "fuel_aggregates"."launch_tag" = "launch_info"."Launch_Tag"

and reads no column from `launch_info`. The test's inline `fuel_aggregates`
binds `launch_tag` itself, so the join exists only to complete that key against
what the planner treats as its authoritative source (the model declares
`MERGE payload.launch.launch_tag into ~launch_tag`). `launch_tag` is NOT one of
this statement's licensed `~` spans - those are `vehicle.name`,
`vehicle.variant` and their `payload.launch.*` twins - so extent ownership is
inert here and always will be. Row-identical: verified against a worktree at
the pre-prune commit, same 10 rows.

**thelook `adhoc04`.** The rule removes

    LEFT OUTER JOIN "highfalutin" ON "juicy"."order_id" = "highfalutin"."order_id"

`order_id` is not a `~` span in that model either. Extent ownership IS active in
this statement (both `product.id` and `user.id` elected to the dim bucket, 6
groups suppressed) and the dead join survives beside it, which is the clearest
evidence the two concerns are orthogonal.

## Disproven, do not repeat

Suppressing extent for licensed spans the statement does not demand (adding an
`undemanded` set to `ExtentOwnership` that `suppressed_for` returns for every
group). Prototyped 2026-08-22:

- It does not fix either case. gcat's dead join is keyed on `launch_tag`, which
  is not licensed in that statement, so no amount of span suppression reaches
  it.
- It breaks tpc-ds q29 with `Could not find CTE for datasource ...`. Extent
  routing is part of `QueryDatasource._compute_identifier`, and once nearly
  every node carries a non-empty suppression set, any scan re-planned under a
  different routing than the same scan got during the build loop lands a second
  identity the CTE lookup cannot resolve. Clearing the set for FINAL assembly
  and re-planning there is one such path; setting it to the undemanded set
  there does not fix it either.

The identifier's dependence on a routing decision is load-bearing (it is what
keeps a d1 WHERE-phase scan distinct from its d0 twin, tpc-h q17) but it is also
the fragile part of the design. Any change that widens which nodes carry a
non-empty `extent_free_spans` needs the q29 shape checked first.

## Where to look

The join is chosen well before the optimizer sees it. Candidates, in the order
worth reading:

- `_cover_groups_for_mandatory` (`v4_helper/strategy_builder.py`) elects a
  contributor per mandatory concept. A contributor that ends up covering
  nothing rendered should not be in `per_group` at all.
- `_parent_nodes_for`'s `covered_by_descendant` dedup drops a parent whose whole
  contribution a sibling already provides, but only when the sibling is a
  lineage descendant or a grouping sibling. Two peers that both expose the key
  do not qualify.
- The authoritative-datasource completion path in `v4_helper/source_planning.py`
  (`_complete_partial_requested`) is what pulls `launch_info` in for gcat.

The bar for a fix: the ablation above reports 0 changed statements, the tpc
corpus render stays 132/132 byte-identical, the fuzzer stays 228/228, and
`test_gcat.py::test_aggregate_optimization` (which now asserts a single-scan
plan) passes with the prune rule DISABLED. At that point the rule is provably
dead and can be retired with its flag.
