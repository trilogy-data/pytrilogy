# Handoff: v4 drops both WHERE atoms when a where-aggregate's `by` key is not in the SELECT

**Status**: pre-existing bug on `main` (verified byte-identical at HEAD `4825c9090` on a clean worktree — NOT introduced by the `hierarchical_where` branch). Wrong rows, silently. Found 2026-08-09 while testing the `then where` feature; recorded in agent memory as `project_v4_minimal_model_where_agg_bug`.

## Repro (fails on main and on `hierarchical_where`, both parser backends)

```python
from trilogy import Dialects, Environment

MODEL = """
key id int;
property id.cat string;
property id.val int;
datasource d (id: id, cat: cat, val: val) grain (id)
query '''
select 1 as id, 'a' as cat, 5 as val
union all select 2, 'a', 8
union all select 3, 'b', 3
union all select 4, 'b', 2
''';
"""
env = Environment(); env.parse(MODEL)
ex = Dialects.DUCK_DB.default_executor(environment=env)
Q = "where val < 8 and sum(val) by cat > 10 select id order by id asc;"
print(ex.generate_sql(Q)[-1])
print([tuple(r) for r in ex.execute_query(Q).fetchall()])
```

- **Expected**: `[(1,)]`. Row gate = `val < 8` AND `sum(val) by cat > 10` where (per the flat-WHERE dual-scope contract) the sum is the *population* value over ALL rows: `{a: 13, b: 5}`. Only id=1 (val 5 < 8, cat a, 13 > 10) survives.
- **Actual**: `[(1,), (2,), (3,), (4,)]` — all rows. The generated SQL contains **neither** atom applied to the output stream:

```sql
WITH quizzical as (SELECT cat, id, val FROM d),          -- no `val < 8` anywhere
highfalutin as (
  SELECT cat FROM quizzical GROUP BY 1
  HAVING sum(val) > 10)                                  -- gate exists but...
SELECT quizzical.id
FROM highfalutin
  LEFT OUTER JOIN quizzical on 1=1                       -- ...joined on NOTHING
ORDER BY quizzical.id asc
```

Three distinct symptoms: (1) scalar atom `val < 8` appears nowhere; (2) the gate CTE is joined back on `1=1` instead of `cat`; (3) LEFT join direction means even a working gate could not filter.

**The trigger**: the SELECT projects `id` only — neither the aggregate's `by` key (`cat`) nor the aggregate itself. The same query with `select cat, sum(val) as v` (or any select including `cat`) plans correctly (equijoin on cat, scalar atom on the scan) — `tests/test_where_select_dual_scope.py` covers only those shapes. Use this working analog for A/B diffing (edit the select, diff the two plans).

## Diagnostic evidence (placement dump)

Monkeypatch spy on `plan_condition_placements` (patch BOTH `trilogy.core.processing.v4_helper.condition_placement.plan_condition_placements` and the direct import in `group_graph`; run with `PYTHONIOENCODING=utf-8` — bucket ids contain `∅`):

```
=== placements (main search) ===
  local.val < 8                              -> ('grp:[@condition]aggregate:d1:local.cat:input:local.id',)  upstream_most
  local._virt_agg_sum_..._wscope > 10        -> ('grp:root:root:∅',)                                        upstream_most
=== buckets ===
  grp:root:root:∅                   ROOT      / DepthLabel.ROOT     primary=[cat, id, val]
  grp:[@condition]aggregate:d1:...  AGGREGATE / DepthLabel.D1       primary=[_virt_agg_sum_..._wscope]
  grp:root:root_d1:∅                ROOT      / DepthLabel.ROOT_D1  primary=[cat, id, val]
=== second build_group_graph invocation (nested feeder search, NO conditions) ===
  buckets: grp:aggregate:d0:... (AGGREGATE/D0), grp:root:root:∅ — placements: EMPTY
```

Leads, most suspicious first:

1. **`val < 8` is hosted ONLY at the d1 condition-side aggregate group.** Its sole placement is inside the `@condition` side channel — the main row stream (ROOT → FINAL) never receives it. This is exactly the "condition-only group covers no mandatory output → pruned → WHERE silently vanishes" failure class that `plan_condition_placements` itself documents and guards with a `DisconnectedConceptsException` (`condition_placement.py`, the `all(gid not in main_lineage ...)` check near the disconnected-raise, ~line 1130 post-branch edits). **Why doesn't that guard fire?** Likely because `main_lineage_groups` seeds from groups producing a mandatory concept and the shared ROOT produces `id` — so ROOT is main-lineage, and the d1 host probably counts as an ancestor through the lineage subgraph. Verify whether the d1 group is in `main_lineage` and whether hosting a scalar atom at a D1 aggregate group (rather than its ROOT_D1 feeder or the main ROOT) is ever renderable here. Note the flat-WHERE contract says the d1 aggregate must see the PRISTINE population — placing `val < 8` below it would be *wrong rows of a different kind*; the correct hosts are the main ROOT scan (for the row gate) — the d1 side channel should arguably not even be a candidate for a scalar atom that FINAL needs.
2. **Even the d1-hosted copy vanishes.** The rendered gate CTE has no `val < 8`. The second `build_group_graph` invocation (from the nested condition-feeder search — `resolve_and_inject_condition` / `condition_sources.py` re-sourcing the gate aggregate) runs with `conditions=[]` and its D0 aggregate plan is what lands in the final SQL. This mirrors the OLD v3 q32/q83 root cause verbatim ("the aggregate is resolved twice; the unbounded copy lands in the final plan" — see memory `project_tpcds_then_where_audit`). Check what `resolve_and_inject_condition` passes as conditions when it re-plans the gate's producer.
3. **`on 1=1`**: FINAL joins the gate CTE without demanding `cat` as a (hidden) join column, because no mandatory output covers it. The fix probably requires FINAL to pull the gate's grain key in as a keyed side input — compare with `_uncovered_exposing_output_contributor` / the FINAL keyed-side-input machinery referenced around the presence-probe FINAL routing comments in `condition_placement.py`. Also note the join is LEFT OUTER — even with a key, direction matters (the gate must restrict, i.e. INNER).

A plausible minimal-fix shape: route BOTH atoms to FINAL-visible hosts for this query shape — the scalar atom to the main ROOT scan (it is reachable there; why did `_choose_groups`/`_upstream_most` prefer the d1 group?), and the gate atom via the post-aggregation-producer path (`_post_aggregation_producers` should catch `_virt_agg_sum_wscope > 10` since its row input IS an aggregate output — why did it land at ROOT with `upstream_most` instead?). Answering those two "why"s is the real investigation.

## Constraints and house rules

- Venv: `.venv/Scripts/python.exe` (Windows). Never `git stash` / `git checkout --` / `git reset` in this tree (shared with parallel agents) — A/B by editing a line and editing it back, or use a `git worktree` (that's how the HEAD baseline was verified: `git worktree add <tmp> HEAD`, run with `PYTHONPATH=<worktree>` and the **lark** backend — the installed pest wheel carries the new `then where` grammar which HEAD's hydrators don't know).
- **This is a condition-placement change: TPC-DS is the mandatory regression signal.** Run `tests/modeling/tpc_ds_duckdb` (165 tests, ~2 min) plus `tests/core/processing`, `tests/test_where_select_dual_scope.py`, `tests/join_matrix`, and the new `tests/test_then_where_execution.py` (the `then where` staged feature rides the same placement machinery — `PlacementReason.STAGE_PRECONDITION` delivery to ROOT_D1 feeders — and must stay green). Compare FAILED lists, not totals. Never run two suites concurrently.
- Shape asserts prove a rewrite fired, not that it's correct — always assert ROWS (`[(1,)]` here).
- No belt-and-suspenders: fix at the placement/planning source, don't add a defensive re-filter at render time.

## Acceptance

1. Repro query returns `[(1,)]` with `val < 8` on the scan (or FINAL), the gate joined on `cat` (INNER or equivalent), and the gate's HAVING computed over the pristine population (NOT val<8-filtered — flat-WHERE conjuncts must not filter each other; `where val < 8 then where sum(val) by cat > 10` is the spelling that filters, and its behavior must stay unchanged).
2. A new test in `tests/test_where_select_dual_scope.py` (or a sibling) covering the select-without-by-key shape, asserting rows for both spellings: flat `where val < 8 and sum(val) by cat > 10 select id` → `[(1,)]` (population sums `{a: 13, b: 5}`, only cat=a passes, only id=1 also has val<8); staged `where val < 8 then where sum(val) by cat > 10 select id` → `[]` (stage-filtered sums `{a: 5, b: 5}`, neither exceeds 10).
3. Suites above green.
