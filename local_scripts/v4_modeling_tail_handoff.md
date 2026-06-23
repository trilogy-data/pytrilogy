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
| gcat `test_join_discovery` | **GENUINE wrong-rows (deep)** | bridge cross-join — see below |
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

## GENUINE (deep): gcat `test_join_discovery` — bridge cross-join

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

Fix direction (NOT done — needs care): get `{vehicle.family, org.state_code}`
into one bridge search. Two avenues:
1. Attach the `org.flag` WHERE to `vehicle.family`'s root source request, so
   `_search_concepts_for_bridge`'s `_condition_arg_lineage_roots` pulls in
   `org.state_code`/`org.code` and the Steiner walk finds `launch_info`.
2. Co-source FK-bridged roots in the group graph using REFERENCE-graph
   connectivity (the concept graph can't see the bridge). Mirrors v3's single
   Steiner over all_concepts.
Whichever path: regression-guard against the disconnected abstract-aggregate
case (`select sum(av), sum(bv)` must still split) and re-run the full v4 sweep.

Verify: `tests/modeling/gcat/test_gcat.py::test_join_discovery`
(`assert "1=1" not in sql`).
