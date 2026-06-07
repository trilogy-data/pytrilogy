# Bug: multi-select with a hidden derive-arg column forces an outer GROUP BY that omits the derive projections → invalid SQL

**Status:** FIXED 2026-06-07 (found same day, enriched eval q44 + q64).

## Root cause (corrected — it is NOT the HAVING)

The HAVING theory below is **wrong**: the spurious outer `GROUP BY` appears even
with the `having` line removed. The real trigger is a **hidden derive-arg
column whose source key is absent from the align keys**.

In q64 every arm projection is `--`-hidden, including `--ss.date.year as yr_a`,
which is consumed only by `derive coalesce(yr_a, 0) as yr_1999`. Because it's
hidden, `yr_a` is dropped from the merge node's *output_concepts* (so the
merge-node grain is just the 14 align keys), but the arm CTE still physically
groups by it, so it survives in the *joined pregrain*. `ss.date.year` is keyed
by the date dimension — not one of the align keys — so it isn't covered by the
align-key grain. That extra pregrain component makes the planner think the
join's grain is finer than the target and force a regroup, which then projects
the raw arm aggregates (`coalesce(cnt_a, 0)`) outside the GROUP BY → invalid SQL.

(The minimal-shape note below was right that single-dim/single-agg arms don't
reproduce — the trigger needs the hidden non-align derive-arg keyed off an
independent dimension. See `tests/engine/test_duckdb.py::
test_multi_select_having_hidden_derive_arg_no_outer_group` for the minimal repro.)

## Fix

A multiselect outer is a pure FULL JOIN of pre-aggregated arms on the align
keys; it must never re-group. Two independent force-group paths both had to be
closed:
1. `gen_multiselect_node` now builds the base `MergeNode` with `whole_grain=True`
   (was `force_group=False`), so `MergeNode._resolve`'s
   `elif self.whole_grain: force_group=False` branch wins over the
   `grain_satisfied_by_pregrain` recomputation.
2. `group_if_required_v2` now short-circuits for any `whole_grain` MergeNode
   (returns it un-grouped), so the later `check_if_group_required` pass can't
   re-set `force_group=True`. This also correctly covers the `group_to_node`
   enrichment merge, which already used `whole_grain=True`.

---

## Original report (HAVING theory — superseded, kept for history)

**Status:** OPEN (found 2026-06-07, enriched eval q44 + q64).
**Severity:** high — the single dominant token sink across q44 (75 calls, **exhausted**, 2.99M) and
q64 (73 calls, **5.28M**). The BinderException recurs within a run (q44 ~8×, q64 ~3×) because the
agent cannot work around a bug in Trilogy's own generated SQL — it just rephrases the same doomed
multi-select.

## Symptom

```
(_duckdb.BinderException) Binder Error: column "cnt_a" must appear in the GROUP BY clause
  or must be part of an aggregate function.
```
`generate_sql` succeeds; DuckDB rejects at execution. The outer SELECT over the `align`/`derive`
FULL JOIN emits a `GROUP BY` (often `GROUP BY 1`) while projecting `coalesce(arm.col, arm.col)`
derive columns that are NOT in that GROUP BY:

```sql
SELECT
    coalesce("scrawny"."cnt_a", 0) as "cnt_1999",   -- not in GROUP BY → error
    coalesce("late"."cnt_b", 0)    as "cnt_2000",
    ...
FROM "scrawny" FULL JOIN "late" on ...
GROUP BY 1
HAVING ...
```

The arms are already aggregated CTEs and the outer is a JOIN — it should **not** group at all (or
must carry every projected derive column). A multi-select `HAVING` (on derived outputs like
`cnt_2000 <= cnt_1999`) is what drives the spurious outer GROUP BY.

## Deterministic reproduction (checked-in / workspace model)

The exact q64 query reproduces. Driver:
```python
from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
WS = Path("evals/tpcds_agent/results/20260607-133609/workspace")  # raw/ model + tpcds.duckdb
eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=WS))
sql = eng.generate_sql(BODY)[-1]                  # succeeds
eng.execute_raw_sql(f"ATTACH '{(WS/'tpcds.duckdb').as_posix()}' as s"); eng.execute_raw_sql("USE s")
eng.execute_raw_sql(sql).fetchall()               # BinderException: column must appear in GROUP BY
```
`BODY` = the 2nd distinct `align`-containing file the q64 agent wrote (extract from
`agent_log.q64.jsonl` file-writes; note this run the content flag is `-c`, not `--content`). Shape:
two `merge`-joined arms `where year=1999 select <dims>, count() as cnt_a, sum() as ws_a, …` /
`… as cnt_b`, `align <dims>: a,b`, `derive coalesce(cnt_a,0) as cnt_1999, …`,
`having cnt_2000 <= cnt_1999 and cnt_1999 > 0 and cnt_2000 > 0`, `order by …`.

**Minimization status:** the trigger needs the *combination* of (a) `HAVING` referencing derived
aggregate outputs, (b) `derive coalesce(arm_agg, …)` projections, and (c) non-align-key dimensions
projected through. Stripped-down 2-arm shapes (single dim, single agg) do NOT reproduce — so the
outer-GROUP-BY decision is sensitive to the projection/grain mix. The real q64 body is the reliable
repro; isolating the minimal trigger is the first task for whoever picks this up.

## Relationship to prior fixes

Distinct from `bug_aggregate_as_align_target.md` (aligning an aggregate — FIXED) and the
derive-output-name-collision in `bug_query_scoped_join_conflicting_filter.md`. Here the aggregate is
in a `derive coalesce(...)` and **`HAVING` is what forces the outer GROUP BY**. Same family as the
"multi-select HAVING implies grouping" concern flagged earlier (HAVING should not imply a GROUP BY
in Trilogy) — this is its invalid-SQL consequence.

## Suggested fix

The outer SELECT over an `align`/`derive` multi-select is a JOIN of pre-aggregated arms — it must
not emit a GROUP BY for a `HAVING` that only filters already-computed derive outputs. Either (a)
render `HAVING` as a post-join `WHERE`/filter without grouping, or (b) if a GROUP BY is truly needed,
include every non-aggregate projected column. Validate against both q44 (rank-arms shape) and q64
(count/sum-arms shape).

## SEPARATE verified bug from q78: `is_returned` declared `bool` but rendered as INTEGER 1/0

(First-pass analysis wrongly dismissed this as a null-semantics idiom error — it is a real type bug.)

The ingested `web_sales` model declares `is_returned bool` but maps it via
`raw('''CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else 0 END''')` — an **integer 1/0**, NOT a
boolean. (`physical_sales` correctly uses `raw('''SR_RETURN_TIME_SK IS NOT NULL''')` → real bool, so
the channels are inconsistent.) Because the declared type is `bool`, boolean comparisons render
against the integer expression and DuckDB evaluates them inconsistently — `= false` / `!= true`
**silently return 0 rows** even though 9229 rows have value 0:

| Trilogy | generated SQL | rows | correct? |
|---|---|---|---|
| `is_returned = true`     | `<case 1/0> = True`     | 7137 | ✓ (1=1 coercion) |
| `is_returned = false`    | `<case 1/0> = False`    | **0** | ✗ (should be 9229) |
| `is_returned != true`    | `<case 1/0> != True`    | **0** | ✗ (should be 9229) |
| `is_returned is not true`| `<case 1/0> is not True`| 9229 | ✓ |
| `is_returned is null`    | `<case 1/0> is null`    | 0 | (no nulls — it's 1/0) |

Repro: `import raw.web_sales as web; where web.date.year=2000 and web.is_returned = false select
count(web.item.id);` → 0 (wrong). The agent CANNOT get the right answer: `= false`/`!= true` are
silently wrong with no error, only `is not true` works, and `is null` (the documented never-returned
idiom) returns 0 here while it would work on `physical_sales` — so even the idiom is inconsistent
across channels.

**Fix (model + framework):**
- Ingest must render a `bool`-declared derived flag AS a boolean (`WR_ORDER_NUMBER IS NOT NULL`), not
  `CASE … THEN 1 ELSE 0 END`. The physical model already does this; make ingest consistent.
- Framework: when a concept's declared type is `bool` but its source expression is non-boolean,
  cast it to bool (so comparisons are consistent) or reject the type mismatch — never silently emit
  `integer = True` and return wrong rows.

(Separately confirmed NOT a bug: `all_sales.is_returned` does have both `True` and `None` buckets per
channel — the agent's "always true" claim was wrong.)
