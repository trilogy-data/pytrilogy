# RESOLVED — 7 failing tests + 1 xfail (all fixed forward)

Branch `v4_more_parity_work_three`. Full suite after the fixes: **6259 passed, 0
failed**, 102 skipped, 6 xfailed, 11 xpassed (baseline at handoff: 6251 passed,
7 failed, 7 xfailed, 11 xpassed). `mypy trilogy` clean, `black --check` clean,
`ruff --select E,F,I` clean on every changed file.

---

## 1. WHERE not applied to the window CTE (5 tests) — FIXED

`trilogy/core/processing/v4_helper/group_graph.py`.

Not predicate pushdown: the atom never had a host that gated the window.
`_split_root_dimension_clusters` peeled `vehicle_name` out of the ROOT bucket
feeding the window into its own `grp:root:root:dim:local.launch_id` bucket, so
`vehicle_name` was not a member (nor a lineage ancestor's member) of the
window's root. `_candidate_groups` then had exactly one candidate — the dim
bucket — and the filter landed on a sibling scan while the window's own source
stayed unfiltered. The `vehicle_variant` filter passes because that column is
NOT an output, so it is never peeled and stays in the window-feeding root.

The peel is gated by `_preaggregate_filter_allows_dimension_member`, whose
"the filter still drops whole groups after the peel" reasoning is sound for an
AGGREGATE (a group is kept or dropped entire) but false for a WINDOW: the
window emits one row per input row and its value is a function of the whole
input POPULATION, so a post-window entity-key semijoin is not equivalent. A
WINDOW among the d0 grouping buckets now disqualifies the peel; the column stays
at fact grain and the WHERE hosts on the window's own source.

`tests/test_window_where_pushdown_matrix.py`: 74 passed.

---

## 2. Partial merge does not narrow directionally — FIXED

`trilogy/core/optimizations/value_set_join_upgrade.py`.

The rowset-boundary exemption in `_pair_side_fully_matches` already existed and
is the right rule (a rowset body's WHERE DEFINES the handle's domain, so a
filtered rowset superset side still proves). It did not fire because
`_rowset_definition_boundary` required `side_cte.parent_ctes` — and with
datasource inlining on, the rowset body CTE reads the tables directly and has
no parent CTEs at all. So the pair fell through to `_accumulate_filter(sup) is
None`, which the body's own `where year = 2001` fails, and the FULL stayed FULL.
(Confirmed by `datasource_inlining = False`, which narrows to LEFT correctly.)

An inlined boundary now qualifies. The external-filter distinction the
parent-exclusion test used to carry is kept explicitly: an external filter on
the rowset OUTPUT names one of `lineage.rowset.derived_concepts`, which a body
WHERE (written pre-rename) never can — `_filters_own_rowset_outputs`.

Checked both spellings and the contrast cases: `merge c.brand into ~p.brand`
and `subset join c.brand = p.brand` both → LEFT OUTER, `[(10,7,7),(20,3,15)]`;
`where p.p_qty > 5` + subset join → LEFT, `[(10,7,7)]`; `union join` still FULL
keeping the 2002-only brand 30.

`tests/test_join_merge_parity.py` + `tests/join_matrix`: 310 passed.

---

## 3. Rich console styling — FIXED (was environment, as suspected)

`tests/cli/test_display.py`.

Rich reads `NO_COLOR` from the environment at Console construction and collapses
every style to bare bold, so `success_output != error_output` was comparing the
terminal's color capability, not our style choices. `NO_COLOR=1 pytest
tests/cli/test_display.py` reproduces the exact failure string. The capture
helper now pins `color_system="truecolor", no_color=False`; passes with and
without `NO_COLOR` set.

---

## 4. Arm LIMIT lost — FIXED, xfail marker removed

`trilogy/core/models/execute.py`.

Not CTE assembly either. `QueryDatasource._compute_identifier` ignored `limit`,
so arm A's limited QDS and arm B's unlimited one hashed to the SAME identity and
`MergeNode._resolve` merged them (`merged[id] = merged[id] + source`) — fusing
both arms into one aggregate CTE, which is why the align FULL join vanished too,
and why the surviving limit floated to the root where `root_cte.limit =
statement.limit` (None) erased it. The v4 planner was correct throughout: the
strategy tree carries `SelectNode limit=1` above arm A's group.

A row limit (and, under it, the ordering that picks which rows survive) is now
part of QDS identity — the same rule `deduplicate_nodes` already applies with
its "a row-limited source is a proper row subset" veto. The arms render
separately, arm A gets `LIMIT (1)`, the FULL align join returns.

`tests/core/processing/test_v4_nested_select_parity.py`: 6 passed.

---

## Repro harnesses

`local_scripts/repro_window_pushdown.py` (item 1) still runs and now prints
MATCH. Group-graph/placement dumps and the merge-narrowing instrumentation used
for items 2 and 4 were scratch-only.
