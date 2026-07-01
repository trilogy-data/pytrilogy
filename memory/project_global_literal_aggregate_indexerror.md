---
name: project_global_literal_aggregate_indexerror
description: FIXED — global literal aggregate (count(1)/sum(1)/count(1) by *) with no grain now resolves to a constant 1 instead of crashing (IndexError / empty-SELECT CTE)
metadata:
  type: project
---

`select count(1) as c;` / `sum(1)` / `count(1) by *` (aggregate over a LITERAL that references NO concept, no grain key) used to → unguarded `IndexError: list index out of range` (opaque `Unexpected error`). `count(<key>)`, `sum(<real col>)`, `count(1) by <key>` were always fine.

DECISION (owner, 2026-06-30): treat a sourceless literal aggregate as a **constant source** → resolve to a single global-scalar row (`count(1)`/`sum(1)`/`count(1) by *` all = 1); `by <select grain>` groups normally. This OVERRIDES an earlier speculative note in this file that argued for a clean validation error — the owner chose resolve-to-1.

FIX (two parts):
1. `calculate_effective_parent_grain` (`discovery_utility.py:72`) — literal-only agg's QueryDatasource has an EMPTY `datasources` list; `return qds.datasources[0].grain` was unguarded. Added `if not qds.datasources: return BuildGrain()` (sourceless = abstract/global-scalar grain). Fixes `count(1)`/`sum(1)`.
2. `render_cte` (`dialect/base.py`, ~:2200) — `count(1) by *` builds a sourceless constant GROUP node (`quizzical` CTE) with zero output columns → rendered empty `SELECT` → DuckDB ParserException. Added `if not render_columns: render_columns = ["1 as __constant"]` so any zero-column CTE is valid SQL.

All three now execute → `[(1,)]`. Stack for the crash: search_concepts→generate_loop_completion→group_if_required_v2(:260)→check_if_group_required(:130)→calculate_effective_parent_grain(:72).

Tests: `tests/engine/test_duckdb.py::test_global_literal_aggregate` (parametrized count(1)/sum(1)/count(1) by *, asserts single-row `1`). Discovered on q97 + q38 (rebaseline 20260630-235635). Handoff+repro: `evals/tpcds_agent/bug_global_literal_aggregate_indexerror.md` + `repro_global_literal_aggregate_indexerror.py`.
