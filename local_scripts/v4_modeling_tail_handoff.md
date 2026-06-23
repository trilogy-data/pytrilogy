# Handoff: v4 `_MODELING`/`_RESULT` known-failing tail

**Date:** 2026-06-22  **Branch:** v4_syntax_continue

Triage of the genuine (non-SHAPE/SIZE) v4 known-failing tests. Run each in
ISOLATION — several "pass" only in a combined run (cross-file pollution) and
several "pass" combined but fail alone. Use:
```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest <test> \
  --runxfail -p no:cacheprovider -o addopts= -q
```

## Status by test (isolated run)

| Test | Class | Notes |
|---|---|---|
| tpc_ds `test_having_nested` | **FIXED 2026-06-22** | per-arm HAVING dropped in multiselect — see below |
| geography `test_exact_match_merge_preserves_subgraph_filters` | STALE (passes alone) | safe-ish to drop from known-failing; verify in full sweep first |
| test_complex `test_in_select` | STALE (passes alone) | ditto |
| tpc_ds `test_ninety_seven_two` | STALE (passes alone) | ditto (was `_RESULT`) |
| gcat `test_join_discovery` | **FIXED 2026-06-23** | condition-root bridge co-source — see below |
| ncaa `test_adhoc08` | borderline genuine | FULL JOIN on nullable `shot_display` → dup rows; test asserts `"FULL JOIN" not in sql` |
| ncaa `test_adhoc07` | SHAPE | asserts a regex over generated SQL |
| stocks `test_provider_name` | SHAPE/join-count | asserts `sql.count("JOIN") == 2` |
| usa_names `test_aggregate_filter_anonymous` | SHAPE | asserts a SQL regex + `"WHERE" not in sql` (bigquery refs, not executed) |
| usa_names `test_filter_constant` | SHAPE | asserts `"< ( 0.1"` constant rendering |
| usa_names `test_group_by_with_existing` | SHAPE | asserts a GROUP BY SQL fragment |
| tpc_h `test_adhoc07` | SHAPE | snapshot diff |
| tpc_ds `test_two_merge_aggregate_compacts_inline_window_query` | SHAPE | CTE-compaction snapshot |
| tpc_ds `test_rowset_arithmetic_argument_keeps_precedence` | SHAPE | asserts a `round((..+..)/(lead` regex |

Net: the genuine wrong-rows tail is small — `test_having_nested` (fixed),
`gcat::test_join_discovery` (deep), and `ncaa::test_adhoc08` (borderline). The
rest assert on generated SQL shape.

## FIXED: tpc_ds `test_having_nested` (per-arm HAVING dropped)

A multiselect (`MERGE`/`ALIGN`) arm with its own `having store_order_count > 1000`
had the HAVING silently dropped under v4 → unfiltered rows. `_resolve_multiselect`
applied each arm's WHERE (`arm_where`) but never its HAVING. Fix: in
`concept_strategies_v4._resolve_multiselect`, after building each arm node, apply
`built_arm.having_clause` via `_resolve_and_inject_condition` (mirrors the inner-
HAVING handling in `resolve_rowset` and the top-level `_get_query_node_v4` wrap).

## FIXED 2026-06-23: gcat `test_join_discovery` — condition-root bridge co-source

Both pieces in `group_graph.py` (v4-only). Full details in memory
`project_v4_condition_root_bridge_cosource`. Summary:
1. `build_group_graph` folds the WHERE's `clause.row_arguments` into the
   convergence set passed to `partition_roots` → the filter root (`org.state_code`)
   co-sources with the SELECT roots in their shared weakly-connected component, so
   one `plan_source` request discovers the `launch_info` connector. Component-gated
   (NOT the ATTEMPT-1 reach-traversal that ran away).
2. `_d1_calc_subgraph` narrows the `root_d1` pristine-scan split: a root feeds
   `root_d1` only if its d1 consumer is a ROW_SHAPE_BARRIER or constrains a
   NON-grouping d0 output. A plain BASIC filter (`org.flag`) now folds into the
   co-sourced root and applies inline instead of re-scanning + cross-joining.
   (Gate is the constraint TARGET: aggregate → fold safe; ROOT output → keep split
   or it 2-cycles.)

This was BROADER than the diagnosis below assumed — even a single `~merge` join
(`where org.flag='x' SELECT count(launch_tag)`) cross-joined.

ALSO fixed the `by all_rows` grand-total marker (was a separate benign `SELECT 1`
cross-join): `_drop_constant_only_parents` inside `_pre_merge_parents` drops a
constant-only parent before the merge, and `projection.concept_satisfiable`
returns True for CONSTANT (else the count whose grain references all_rows gets
pruned to an empty SELECT). Result is byte-identical to v3 (bare grand-total
aggregate). `test_join_discovery` uses strict `"1=1" not in sql` + FK-join regex.

### Original diagnosis (kept for context)

Repro:
```trilogy
import launch_dashboard;
where org.flag = 'abc123'
SELECT count(vehicle.family) by __preql_internal.all_rows -> all_vehicles LIMIT 1;
```
`org.flag` derives from `org.state_code` (organizations table); `vehicle.family`
is on `lv_info`. They connect ONLY through the fact `launch_info`, which carries
FK merges `FirstAgency: ~org.code` and `LV_Type: ~vehicle.name` /
`Variant: ~vehicle.variant` (see `launch_base.preql:81`). v3 finds the 3-table
join (organizations → launch_info → lv_info). v4 emits `... RIGHT OUTER JOIN
... ON 1=1 LEFT OUTER JOIN lv_info ON 1=1` — a cross-join — inflating the count.

Diagnosis (traced via `source_planning._bridge_plan`): the bridge planner is
asked for `{vehicle.family, vehicle.name, vehicle.variant}` and `{org.state_code}`
in SEPARATE requests, each with `conditions=None`. So:
- `vehicle.*` → `_single_source_covers_completely` True (lv_info) → no bridge.
- `org.state_code` → single source (organizations) → no bridge.
- They never appear in ONE bridge search, so `determine_induced_minimal_nodes`
  is never asked to connect them and never discovers `launch_info`.

Root cause: v4's concept_graph/group_graph treats the filter dim `org.state_code`
as a root with no lineage path to `vehicle.family`, so they land in different root
groups and only recombine at the FINAL merge (cross product). `launch_info` is a
pure REFERENCE-graph connector (neither concept's lineage references it), and
`partition_roots` only sees the CONCEPT graph, so it can't co-source them. The
disconnected pre-check does NOT fire (they ARE connected in the reference graph
via the FK merges), so it's a resolution gap, not a disconnection.

### ATTEMPT 1 (2026-06-22) — reverted: co-source via reach-following-CONSTRAINT

Tried avenue 2 the cheap way: in `partition_roots`, extend the per-root reach
traversal to follow `EdgeKind.CONSTRAINT` edges (a filter arg's implied JOIN to
its consumer) in addition to `LINEAGE`. This makes `org.state_code` reach the
shared `all_vehicles` aggregate, so it co-sources with `vehicle.*` in one root
bucket. RESULT:
- It DID trigger bridge discovery — the Steiner walk then searched
  `{org.code, vehicle.family, vehicle.name, vehicle.variant}` together and found
  `launch_info`; the `thoughtful` CTE correctly joined organizations→launch_info→
  lv_info on real keys.
- BUT only a HALF fix: a `1=1` remained. The `org.flag` filter still came from a
  SEPARATE `@condition` group (org re-scanned) cross-joined onto the bridge,
  because the bridge node didn't EXPOSE the org join key (org.code/state_code) —
  the backward pass prunes it (not "needed" as an output), and the condition
  atom (`org.flag`, a BASIC) can't be hosted on the d0 root since
  `_candidate_groups`/`_reachable_input` only count a group's own members +
  lineage ancestors, and `org.flag` is DOWNSTREAM of the member `org.state_code`.
- WORSE: following CONSTRAINT edges blanket co-sources too many roots in complex
  queries (TPC-DS), exploding `plan_source`'s bridge/Steiner search → a RUNAWAY
  (a single planner process pinned ~1GB, no output after 11 min). Reverted.

Lessons for the next attempt:
- Co-sourcing must be TARGETED (only when a bridge is genuinely needed — i.e. the
  filter arg and outputs share no direct datasource), not blanket CONSTRAINT
  reach, or complex queries run away.
- Even with co-sourcing, a second sub-fix is required so the filter is HOSTED at
  the bridge root rather than re-scanned + cross-joined. Either (a) let the d0
  root expose the org join key so the condition source joins on it (correct rows,
  extra self-join — passes the `1=1` assertion), or (b) extend `_reachable_input`
  with a BASIC-derivability closure so `org.flag` is hostable on the root that
  carries `org.state_code`, and apply the filter there (matches v3's inline WHERE).

Remaining fix is therefore TWO coupled pieces (targeted co-source + filter
hosting/key-exposure), each non-trivial. Guard the disconnected abstract-aggregate
case (`select sum(av), sum(bv)` must still split) and run the FULL v4 sweep —
watch for runaways (a hung process at high RAM), not just failures.

Verify: `tests/modeling/gcat/test_gcat.py::test_join_discovery`
(`assert "1=1" not in sql`).
