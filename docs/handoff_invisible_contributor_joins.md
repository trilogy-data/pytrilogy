# Handoff: joins to contributors that render no column

## Status 2026-08-22: CLOSED. Both causes fixed at the planner and
## `PruneInvisibleOuterJoins` is retired.

`PruneInvisibleOuterJoins` was expected to decay to dead code once extent
ownership landed (docs/extent_ownership.md). It did not, and neither surviving
case was an extent problem. The two turned out to have **separate root causes**,
not one. Both are now fixed in the planner and the rule has been deleted; see
docs/handoff_contributor_reachability.md for the thelook half.

## Reproduce

The prune rule is gone, so its ablation no longer exists. The corpus sweep it
used is still the gate for any planner change here (recreate it; ~80 lines):
render every `query*.preql` and `adhoc*.preql` under tpc_ds_duckdb, tpc_h,
tpc_ds, thelook_duckdb, hackernews, ncaa, gcat, the_look and faa, **plus
`tpc_ds_duckdb/aggregates/query*.preql`**, twice in ONE process with the
candidate change toggled, and diff the two result sets per statement. 203
statements; a no-op control leg must report 0.

The `aggregates/` cases are NOT optional. They import from the parent directory
(`import aggregates.opt_three`, env working_path = `tpc_ds_duckdb`) so a
per-directory glob skips them, and they are the `partial ... complete where`
summary-table shapes a source-selection change re-routes first. The earlier
version of this sweep missed them and a candidate fix regressed q03 from 923 to
3492 chars unnoticed — the full pytest suite passed, because
`test_agg_queries.py::test_three` bounded the plan at 7000. That bound is now
1500.

## gcat — FIXED

`test_gcat.py::test_aggregate_optimization`. The inline `fuel_aggregates` binds
`org.state_code`/`org.hex` but not their grain key `org.code`.
`_concepts_with_grain_keys` expanded every requested concept into its grain
components, making `org.code` (canonically the `~org.code` merge key from
`launch_base.preql:100`) a hard search terminal. `fuel_aggregates` could not
bind it, so the search added `organizations` to bind it and `launch_info` to
bridge to it, then sourced both properties off `fuel_aggregates` anyway.

The fix treats a grain key as a join AFFORDANCE rather than a demand when one
datasource covers the whole request completely: no join exists, so no spine is
needed. Two boundaries are load-bearing and each is backed by a failing probe —
see the comment at `_concepts_with_grain_keys`.

Note the shipped `fuel_dashboard.preql` never had this problem; it binds
`org_code:?org.code`. Only the test's inline redefinition drops the key.

## thelook `adhoc04` — STILL OPEN, different site

The rule removes

    LEFT OUTER JOIN "highfalutin" ON "juicy"."order_id" = "highfalutin"."order_id"

This is not source planning. `_cover_groups_for_mandatory`
(`v4_helper/strategy_builder.py`) elects a winner per mandatory concept
independently, sorting candidates by how far downstream they are:

    grp:group_to:d0:order.id      -> ['order.id']                       <- highfalutin
    grp:aggregate:d0:local.id     -> ['id','revenue','margin','total_order_revenue']
    grp:root:root:*:dim:local.id  -> ['user.id','product.id']           <- juicy

`highfalutin` legitimately wins `order.id` at election time — `juicy` does not
expose `order.id` yet. It only becomes redundant afterwards, when join-key
materialization adds `order_id` to `juicy`. So the election is not wrong on its
own inputs; the redundancy is created downstream of it, and any fix has to
account for that ordering rather than just re-ranking candidates.

## Disproven, do not repeat

- **Suppressing extent for licensed spans the statement does not demand** (an
  `undemanded` set on `ExtentOwnership`). Prototyped 2026-08-22. Fixes neither
  case and breaks tpc-ds q29 with `Could not find CTE for datasource ...`:
  extent routing is part of `QueryDatasource._compute_identifier`, so once
  nearly every node carries a non-empty suppression set, a scan re-planned under
  different routing lands a second identity the CTE lookup cannot resolve.
- **`_complete_partial_requested` is what pulls `launch_info` in for gcat.** It
  is not. `fuel_aggregates.partial_concepts` is empty; the join comes from the
  network search's terminal set.
- **"A cover that genuinely needs the join will recover the key as a
  connector."** False. Drop the key too broadly and the search re-picks the
  source and pairs on PROPERTIES with `is not distinct from` (tpc_ds aggregates
  q03).

The identifier's dependence on a routing decision is load-bearing (it keeps a d1
WHERE-phase scan distinct from its d0 twin, tpc-h q17) but it is also the fragile
part of the design. Any change that widens which nodes carry a non-empty
`extent_free_spans` needs the q29 shape checked first.

## The bar for retiring the rule

Met 2026-08-22: the ablation reports 0 changed statements over 203, the tpc
corpus render stays byte-identical, the fuzzer stays 228/228, and
`test_gcat.py::test_aggregate_optimization` asserts on the planner's own
output. The rule, its flag, and its test are deleted.
