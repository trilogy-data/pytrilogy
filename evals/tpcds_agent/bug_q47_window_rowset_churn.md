# bug_q47: two silent planner/renderer bugs behind the 880k-token q47 churn

STATUS: both bugs FIXED 2026-08-20; this file is kept only until it has been committed
once, so its closure leaves a `git log --diff-filter=D` record. Bug A in
`trilogy/dialect/base.py` (`_protect_conditional_child`), bug B in
`QueryDatasource.__add__` (`trilogy/core/models/execute.py`). Regression tests:
`tests/rendering/test_engine_or_chain_precedence.py` and
`tests/core/processing/test_query_datasource_add_nullability.py`; both were confirmed to
fail with the respective fix reverted at runtime. The two non-bug polish items from the
tail moved to `handoff_q47_diagnostic_polish.md`.

Re-verified: 2026-08-20 against the working tree (branch `more_eval_tuning`, base `c40ef023b`),
reproduced from a scratchpad copy of run
`results/20260820-031800_enriched_deepseek_deepseek-v4-flash` (q47, final status: pass).

## Symptom

q47 (month-over-month category/brand/store window query) burned 880,142 tokens.
The agent wrote a correct answer on its FIRST attempt (event 46 of 74, ~40k prompt tokens);
it ran cleanly, and the final submitted file is byte-identical to that first body.
Scoring re-verified from the copied workspace: `status='pass', ref_rows=100, cand_rows=100`.

Everything after that clean run, ~575k tokens (65% of the total), was spent adjudicating
between engine behaviors that a correct engine would not have exhibited:

1. The same logical query returns a different full row universe with lag/lead present
   (50,537 rows) vs without (53,194 rows), and the rowset (`with`) formulation WITH
   lag/lead returns the no-window universe (53,194). Three formulations, two answers.
2. The agent's diagnostic probe for that divergence, `having ... ss.store.name is null`,
   silently filtered NOTHING (returned the full 53,194-row set with non-null store names
   in every displayed row), destroying its ability to reason about which universe was real.

Both are reproducible framework bugs. The single hard error in the log (undefined concept
`monthly_totals.store_name`) is NOT one of them, see the last section.

## Rewrite-by-rewrite timeline (from agent_log.q47.jsonl)

| event | action | outcome / why the next write happened |
|---|---|---|
| 2-44 | agent-info, explore, 8 exploration probes | normal enriched flow, ~250k cumulative tokens |
| 46 | writes `answer_2118989494.preql` (auto aggregates + lag/lead), `--run` | clean, 100 rows, full universe 50,537. Two `window_filter_needs_having` warnings |
| 49 | probe_verify: hand-checks one group's 14 months | values confirmed correct |
| 52 | probe_cte: rowset (`with monthly_totals as ...`) variant, "to avoid the heuristic warning" | FAILS: undefined `monthly_totals.store_name` (agent-invented spelling) |
| 55 | probe_cte retry using the did-you-mean spelling `monthly_totals.ss.store.name` | clean, same top-100, but full universe 53,194 with 2,654 NULL-store rows. Divergence discovered |
| 58 | probe_null: counts NULL-dim fact lines | 15,690 in-window lines have NULL store |
| 60 | probe_null2: plain monthly group-by | NULL-store groups DO appear (as they should) |
| 63 | probe_null3: auto variant + `having ss.store.name is null` | SILENTLY IGNORED: returns non-null-store rows, full count still 53,194. Agent: "behaved unexpectedly" |
| 66 | probe_iso: auto variant, no windows, no is-null | 53,194. Agent concludes "windows exclude NULL-store rows, top-100 identical either way" |
| 69 | re-runs the untouched answer file | clean, submits |

The answer file was written once. The churn was not rewrite thrash, it was the agent
correctly detecting two real engine inconsistencies and failing to explain them because a
third bug made its probe lie.

## Bug A (P1, silent wrong rows): engine-combined conditions render without parentheses

Predicate pushdown ANDs pushed-down HAVING atoms onto a CTE's WHERE condition. The
condition TREE is correct (`AND(or_chain, atom)`), but the renderer emits `BuildConditional`
as `left op right` with no parenthesization of an OR child under an AND parent, so SQL
operator precedence reparses it.

probe_null3's `thoughtful` CTE rendered as:

```sql
WHERE "D_YEAR" = 1999 or ("D_YEAR" = 1998 and "D_MOY" = 12) or ("D_YEAR" = 2000 and "D_MOY" = 1)
      and "D_YEAR" = 1999 and "S_STORE_NAME" is null
```

DuckDB parses that as `A or B or (C and year=1999 and is-null)`. Consequences:
the appended `is null` and `year = 1999` apply only inside the third OR arm; that arm is
also self-contradictory (`year=2000 and year=1999`), so Jan-2000 rows silently vanish and
the is-null filter is a complete no-op on the surviving rows. Exactly what the agent saw.

### Minimal repro (9 lines, run in any tpcds workspace)

```trilogy
import raw.store_sales as ss;

where (ss.sale_date.year = 1999) or (ss.sale_date.year = 1998 and ss.sale_date.month_of_year = 12)
select
    ss.sale_date.year,
    ss.sale_date.month_of_year,
    sum(ss.sales_price) as total
having ss.sale_date.year = 1998
order by ss.sale_date.year asc, ss.sale_date.month_of_year asc;
```

Returns 13 rows: 1998-12 plus ALL TWELVE months of 1999, despite `having year = 1998`.
Rendered WHERE: `"D_YEAR" = 1999 or ("D_YEAR" = 1998 and "D_MOY" = 12) and "D_YEAR" = 1998`.

### Root cause

- `trilogy/dialect/base.py:2412-2413`: the `CONDITIONAL_ITEMS` branch of `render_expr`
  returns `f"{render(e.left)} {e.operator.value} {render(e.right)}"` with no parentheses,
  regardless of child operator precedence.
- The hazardous trees are engine-built, not authored: `append_condition`
  (`trilogy/core/optimizations/utils.py:71-80`) via `merge_conditions_and_dedup` /
  `combine_condition_atoms` (`trilogy/core/processing/condition_utility.py:666-712`)
  AND-chains atoms where one atom is an OR-rooted `BuildConditional`. Authored parentheses
  become `BuildParenthetical` and render fine; the pushdown-merged OR chain has none.
- Fires for any query whose WHERE has a top-level OR and which gets any predicate pushed
  into the same CTE (HAVING atoms on group keys, optimizer pushdown at
  `predicate_pushdown.py:462/464/725/827`). Broad, silent class.

### Fix

`_protect_conditional_child` (`trilogy/dialect/base.py`) wraps an OR-rooted
`BuildConditional` child in parentheses when the parent operator is AND. AND under OR needs
nothing (AND already binds tighter), so the change is precedence-minimal.

Corpus footprint: the parenthesizing branch fires ZERO times across all 186
`tests/modeling/*/query*.preql` + `adhoc*.preql`, and the rendered SQL is byte-identical
with the fix reverted. The regression test is the only coverage, which is exactly why the
bug survived this long.

## Bug B (P1, silent row loss / plan-dependent semantics): `QueryDatasource.__add__` drops `nullable_concepts`

With lag/lead present, the fact-to-store join renders `INNER JOIN store` and the final
merge join renders plain `=` on `ss_store_name` (while `company_name`, declared `string?`,
keeps `is not distinct from`). Without windows, the same monthly aggregate renders
`LEFT OUTER JOIN store` and null-safe `is not distinct from` on store_name. The 2,654
NULL-store monthly groups (15,690 fact lines with NULL `SS_STORE_SK`, bound `?store.sk`)
exist in one plan and are silently absent in the other: 53,194 vs 50,537 rows.

### Traced mechanism (instrumented, no source edits)

1. The fact-store join is planned `LEFT_OUTER` in every branch (`get_join_type` traced).
   The month-grain group QDS is built 4 times; 3 copies carry `ss.store.name` in
   `nullable_concepts`, 1 does not. The bad copy's construction stack bottoms out in
   `QueryDatasource.__add__` (`execute.py:1284/1338`), reached from
   `merge_node.py:493 _resolve` when two same-identifier parent QDSs merge.
2. `QueryDatasource.__add__` builds the merged QDS
   (`trilogy/core/models/execute.py:1338-1369`) passing `partial_concepts`,
   `rollup_concepts`, `hidden`, etc. but **omitting `nullable_concepts`**. The field falls
   back to empty; `__post_init__` (execute.py:1136-1141) restores only INTRINSIC (`?`)
   nullability. Join-analysis null-extension (outer-join padding on `store.name`) is erased.
   Contrast `CTE.__add__` at execute.py:344-345, which unions `nullable_concepts` correctly.
3. Downstream, `get_modifiers` (`trilogy/core/processing/join_resolution.py:611-629`)
   traced `side_nullable=(True, False)` for `ss.store.name` on the avg-side vs window-side
   pair, so it returns `[]` and the join key renders plain `=` instead of null-safe.
4. The null-rejecting `=` then licenses `join_upgrade` to flip the shared scan's
   store join `LEFT_OUTER -> INNER` (the visible INNER in the `thoughtful` CTE).
5. Proof: monkeypatching `__add__` to union both sides' `nullable_concepts` restores
   `LEFT OUTER JOIN store` and `is not distinct from` on store_name for the identical
   query text, making the window and no-window plans agree.

This is precisely the trap class the code already documents at
`trilogy/core/processing/nodes/base_node.py:584-593` (q29 divergent-copy note) and
`base_node.py:639-651` (resolve-time sync-back, "q51, q86 rowset variant"):
resolve-time nullability lives on the QDS stamp, and `__add__` is a hole that erases it.

Why the eval still passed: NULL-store groups sort outside the top-100 for this question,
and the canonical `tests/modeling/tpc_ds_duckdb/query47.preql` sidesteps the whole area by
pinning `store_sales.store.sk is not null` in its WHERE, so the canonical corpus never
exercises the padded groups.

### Fix

`__add__` now passes `nullable_concepts=unique(self.nullable_concepts +
other.nullable_concepts, "address")`, matching what `CTE.__add__` already did.

Repro note for the regression test: the trigger is the *output aliases*
(`avg_monthly as avg_monthly_sales`, ...). Without them the month-grain group is never
built as same-identifier copies, `__add__` is not reached at all, and the plan is correct
either way. The unaliased spelling of the same question is not a repro.

Corpus footprint: `__add__` runs 78 times across the 186-query corpus and now carries
padding nullability through in 70 of them, yet the rendered SQL is byte-identical with the
fix reverted. Broad reach, zero corpus behavior change.

## Not a bug: the `monthly_totals.store_name` undefined-concept error

Tested against the documented rowset leaf shorthand (`rs.col` resolves at parse time when
unambiguous):

- `monthly_totals.name` (the actual leaf of `ss.store.name`) resolves and runs.
- `monthly_totals.ss.store.name` (the did-you-mean) resolves and runs (the agent's retry
  used it successfully).
- `monthly_totals.store_name` was an agent-invented flattened spelling; it is not the leaf
  of any output, so the error and its suggestion were both correct.

The polish opportunity that observation raised now lives in
`handoff_q47_diagnostic_polish.md`, together with the `window_filter_needs_having`
false positive on the bookend pattern.

## Classification

Silent framework bugs (two, both reproduced with minimal cases), not agent error. The
agent's first answer was correct and finally submitted unchanged; ~575k of the 880k tokens
were the direct cost of Bug B creating an unexplainable row-universe fork and Bug A making
the natural diagnostic probe return silently unfiltered rows.
