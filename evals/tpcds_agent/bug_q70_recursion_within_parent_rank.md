# Bug: RecursionError building a CASE/window concept whose condition references `grouping()` (q70 rank-within-parent)

**Status:** OPEN (diagnosis only, no fix). NEW bug, distinct from the two earlier q70 reports
(both of which are now FIXED — see "Relationship to prior q70 reports").
**Severity:** high — q70 is now the worst token sink in the eval (2.08M tokens, +283%). The agent
hits an *uncaught* `RecursionError` (surfaced as `Unexpected error: ... maximum recursion depth
exceeded` / `Recursion error building concept local.within_parent_rank ...`) which the harness reports
as a crash, so the agent thrashes blindly with no actionable signal.
**Run:** `evals/tpcds_agent/results/20260628-194910/` (`agent_log.q70.jsonl`).
Signatures in that log: `Recursion error building` / `maximum recursion depth` ×3,
`GROUPING child ... must be a grouping column` ×1, and **zero** of the two prior q70 signatures
(`GROUPING statement cannot be used without groups`, `INVALID_REFERENCE_BUG`).

## Symptom

`trilogy run query70.preql` fails at `generate_sql` (build phase, before any SQL is emitted) with:

```
Unexpected error in query70.preql: Recursion error building concept local.within_parent_rank
with grain Grain<sales.item.id,sales.ticket_number> and lineage
case(WHEN add(grouping(ref:sales.store.state),grouping(ref:sales.store.county)) >= 1 THEN 1,
     ELSE rank([sales.store.state, sales.store.county])
          over [add(grouping(...state),grouping(...county))]
          order [sum(sales.net_profit) by sales.store.state desc]).
This is likely due to a circular reference.
```

(A second agent variant recurses identically on `local.within_parent_rank` with the ELSE branch a
`row_number() over [partition state] order by sum(net_profit)`.) The error is raised from
`trilogy/core/models/build.py:2824-2826` after Python blows the recursion limit inside
`Factory._build_concept` / `__build_concept`.

## Failing construct (from the agent log + canonical)

The agent (and the canonical `tests/modeling/tpc_ds_duckdb/query70.preql`) models the q70
"rank within parent" column as a CASE/window whose **CASE condition references `grouping()`** and
whose **branch is an aggregate-over-window** (`rank`/`row_number` over `... order by sum(net_profit)`).
The window often also *partitions by* the rollup level (`grouping(state)+grouping(county)`), but that
is **not** required for the recursion (see bisection variant D). The minimal trigger is just:

> a `Derivation.BASIC` concept (e.g. a CASE) that references a `grouping()` concept **and** contains
> an aggregate (the window's `order by sum(...)`), used as a SELECT output.

The canonical `query70.preql` writes the same shape via named `auto` concepts
(`lochierarchy <- grouping(state)+grouping(county)`, `rank_within_parent <- rank(...) over (partition
by lochierarchy ...)`). **The canonical query currently also fails to build** on this engine — the
recursion is in the framework, not a quirk of the agent's phrasing.

## Minimal repro (recurses; no rollup, no membership, no top-N needed)

`.venv/Scripts/python.exe`, model = `tests/modeling/tpc_ds_duckdb`:

```python
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment

text = '''
import store_sales as ss;
auto g_state <- grouping(ss.store.state);
auto wpr <- CASE
    WHEN g_state >= 1 THEN 1
    ELSE rank(ss.store.state) over (partition by ss.store.state order by sum(ss.net_profit) desc)
END;
where ss.date.year = 2000
select ss.store.state as s_state, sum(ss.net_profit) as total_sum, wpr limit 100;
'''
eng = Dialects.DUCK_DB.default_executor()
eng.environment = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
eng.generate_sql(text)   # -> RecursionError: Recursion error building concept local.wpr ...
```

### Bisection (what is / isn't required)

| Variant | Result |
|---|---|
| CASE cond = `grouping(state)`, branch = `rank()/row_number() over (... order by sum(net_profit))` | **RECURSION** |
| CASE cond = `grouping(...)`, partition by `lochierarchy` **or** by plain `state` | both **RECURSION** (partition target irrelevant) |
| with `by rollup(...)` present | **RECURSION** (rollup not required) |
| CASE cond = plain column (`state = 'TX'`), same window branch | **OK** |
| CASE cond = `grouping(...)`, branch = plain literal (no window/aggregate) | **OK** |
| window referenced directly (no CASE wrapper) | **OK** |

So the trigger is the **conjunction**: `grouping()` in the CASE condition **and** an aggregate inside
a branch, on a single BASIC output concept. Either alone is fine.

## Root cause (file:line)

The build is a mutual `_build_concept` cycle between the CASE concept and the `grouping()` concept.
Instrumenting `Factory._building` shows the stack oscillating `local.wpr -> local.g_state -> local.wpr
-> local.g_state -> ...`. The mechanism:

1. `__build_concept(wpr)` resolves `wpr`'s grain via `get_select_grain_and_keys`
   (`build.py:2893`); `wpr` is `Derivation.BASIC`, so the BASIC branch in
   `trilogy/core/models/author.py:1289-1311` walks every `concept_argument` of the CASE — including
   `g_state` — to union their grain keys. Then `build(new_lineage)` (`build.py:2898`) builds the CASE
   `BuildFunction`, which builds the comparison `g_state >= 1`, which calls `_build_concept(g_state)`.
2. `g_state = grouping(ss.store.state)` is an aggregate-class function. Building it reaches
   `_build_aggregate_wrapper` (`build.py:3006`). Its `by` resolves to the **enclosing SELECT grain's
   component concepts** (`build.py:3007-3011` for empty `by`, or `_build_over_items` at `3013` when
   `get_select_grain_and_keys` filled `by` from the resolution grain in
   `author.py:1267-1278`). That SELECT grain **contains the output concept `wpr`** (the row-grain
   window/CASE column is a grain component).
3. So building `g_state`'s grouping `by` calls `_build_concept(wpr)` again → step 1 → ∞.

In short: `grouping()`'s grain/`by` is the **consuming SELECT grain**, the SELECT grain **includes
the CASE/window output concept**, and that output concept's lineage **references the `grouping()`** in
its CASE condition — a genuine cyclic dependency. With a plain-column condition (bisection row 4) the
condition concept has a fixed, source-derived grain that does not pull in the SELECT grain, so no cycle
forms.

The guard at `build.py:2820/2823-2828` only *renames* Python's eventual `RecursionError` into the
"Recursion error building concept ..." message; `self._building` is an append/pop list with **no
on-stack cycle short-circuit**, so a self-referential grain dependency is never broken (it should
either detect the address already on `_building` and return a placeholder, or — better — `grouping()`
should resolve its `by` to the rollup keys / base grain without re-including downstream row-grain
output concepts like `wpr`).

## Separate variant in the same run: `GROUPING child ... must be a grouping column` (BinderException)

A *different* agent attempt (file write at log line 99) inlined `grouping()` directly in projection
CASEs + a `row_number() over (partition by grouping(state)+grouping(county) ...)` under
`by rollup(state, county)`. That one builds SQL but DuckDB rejects it:

```
(_duckdb.BinderException) Binder Error: GROUPING child "sales_store_county" must be a grouping column
LINE 118:   grouping("concerned"."sales_store_county") as "_virt_agg_grouping_..."
```

i.e. the `grouping()` is rendered in a CTE (`concerned`) that references the raw column **before** the
`GROUP BY ROLLUP`, so its argument is not a grouping column at that point. This is the same family as
the MEMORY-noted *fixed* q14 `grouping over aliased rollup key` binder bug
(`_normalize_grouping_args_to_rollup_keys` in `select_finalize.py`), but here the `grouping()` is
buried inside a CASE / window-partition expression so the normalization does not reach it. It is a
**distinct, separate bug** from the recursion (binder vs. build-phase) and is NOT the dominant token
sink (1 occurrence vs. 3 for the recursion). Worth its own follow-up; not analyzed further here.

## Relationship to prior q70 reports

- `bug_q70_grouping_without_groups_binder.md` — **FIXED.** `grouping()` in a WHERE clause now raises a
  clean author-time error; the "GROUPING statement cannot be used without groups" string appears **0
  times** in this run's log.
- `bug_q70_invalid_reference_rollup_grouping.md` — **FIXED.** The top-N rank-filter
  `INVALID_REFERENCE_BUG` sentinel (inline order-by aggregate not collapsed onto the projected
  concept) appears **0 times** in this run's log.
- **This recursion is NEW** and now the dominant q70 failure. It is *not* a regression of either fix
  and does not require rollup, membership, or a top-N rowset (minimal repro has none). It is a
  build-phase cyclic-grain bug specific to a single BASIC concept that both **references `grouping()`**
  and **embeds an aggregate** (the rank/row_number window's `order by sum(...)`) — exactly the
  natural way to express q70's "rank within parent level" column.
