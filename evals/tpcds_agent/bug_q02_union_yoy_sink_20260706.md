# q02 union YoY-weekly-ratio token sink (run 20260706-222300, 1.41M tok) — NO framework bug

**Verdict: NOT a framework bug.** q02 PASSES (ref 53 / cand 53). The +870k jump vs the
prior `20260706-135542_enriched` run (539k → 1.41M) is driven by (1) deepseek approach
variance and (2) a genuine-but-CORRECT SQL window-after-WHERE semantics trap the agent
misdiagnosed and probed for ~20 tool calls. All three previously-reported q02 framework
bugs are FIXED and did NOT recur here. Commit `4e69c5547` (landed between the two runs)
did not cause the jump.

## What actually happened (new run, `agent_log.q02.jsonl`, 39 tool calls / 117 events)
- Old cheap run chose the **union-join CTE self-join** idiom (`with cur_totals … future_totals …
  union join cur_totals.ws+53=future_totals.ws`); it built and passed in 19 calls / 539k.
- New expensive run chose the **window `lead(daily_sales,53)`** idiom (same shape as canonical
  `tests/modeling/tpc_ds_duckdb/query02.preql`). This is pure deepseek nondeterminism in the
  initial approach — both idioms are valid and both engine paths work.
- The window path led the agent into a semantic trap: it tested `lead(…,53)` while a top-level
  `where s.date.week_seq in (5270,5271,5272,5323,5324,5325)` (6 weeks) restricted the input,
  so `lead(53)` correctly returned **NULL for every row** (no row 53 positions ahead in a
  6-row partition). Agent read the all-NULL output (idx 51/83) as a possible framework fault and
  burned idx 52–113 (~20 calls) probing week_seq/year data to understand it.

## The "does not exist" signature (idx 11)
`Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.` — the agent guessed a
non-existent model filename; trivially worked around via `file list raw` → `raw/date.preql` /
`raw/all_sales.preql`. Not a framework bug (clean argparse error, one call lost).

## Window-after-WHERE is CORRECT, not an unsound pushdown
Generated SQL for the agent's idx-79 diagnostic shape (via `Executor.generate_sql`):
```
cheerful CTE:    ... WHERE D_WEEK_SEQ in (5270,5271,5272,5323,5324,5325)   -- base scan filter
cooperative CTE: sum(...) GROUP BY dow, week_seq
OUTER select:    lead(daily_sales,53) OVER (PARTITION BY dow ORDER BY week_seq)  -- over filtered set
```
The filter lands on the base scan and the window is computed in the outer select over the
already-filtered rows. This is exactly standard SQL semantics and exactly what the user wrote —
NOT an unsound predicate-pushdown-below-window. The canonical relies on the same behavior; it
just filters to weeks in **both** years (`sales.date.year in (2001,2002)`) so every 2001 week has
its +53 partner inside the filtered set. The agent's final passing form dodges it the other way:
compute `daily_sales_data` (+ its window) in a **rowset over all web/catalog data**, then filter
only the output (`where daily_sales_data.week_seq in wk2001`). idx-98/110 produced 53 rows; agent
hand-verified 3.52 / 1.07 / 0.89 / 5.8 all match manual ratios. Correct.

## The two in-run errors are clean friendly errors (agent mistakes)
- idx 41 `Aggregate concept local.sun_ratio cannot reference itself` — agent wrote
  `sum(sun_ratio) as sun_ratio`, reusing the concept name as its own output alias. Clear message.
- idx 92 parse error `daily_sales_data.dow = daily_sales_data.dow` — a leftover comment line the
  agent failed to prefix with `#`. Clear location-pointed message.

## Commit 4e69c5547 (landed 18:22, between the 13:55 and 22:23 runs) — ruled out
- `predicate_pushdown.py` (+11): only makes pushdown into set-op arms MORE conservative
  (UNION_ALL only; skip EXCEPT/INTERSECT). The window path uses neither.
- `ai/constants.py` (+10) / `ai/syntax_examples.py` (+53): only ADD `except`/`intersect` examples;
  the union-stack-channels / scoped-join / pivot-columns / query-structure examples the agents
  read are unchanged. Guidance did not steer the new agent toward the window idiom.
- Sanity: the old run's winning union-CTE query still `generate_sql`s to 1 statement on the
  current post-commit engine (9419 chars). No union regression.

## Prior q02 framework bugs — all FIXED, none recurred
- `bug_q02_derived_rowset_union_join_base_where_recursion.md` (RecursionError): FIXED. The D4
  minimal repro now raises a clean `DisconnectedConceptsException` (join/merge guidance), no
  RecursionError / "this is a bug". Reproduced on current engine.
- `bug_q02_resolution_214830.md` Bug#1 (self-join key ambiguity in `def` bodies) and Bug#2
  (`_virt_filter` orphaning): not hit — the new agent never used the two-rowset self-join-inside-
  `def` shape, and the old run's union-CTE + `def ratio_for_dow` shape resolved fine.

## Classification
AGENT / deepseek variance + correct-semantics confusion. No engine defect. No regression from
4e69c5547.

## Optional guidance improvement (NOT a bug fix)
Neither `agent-info` nor the syntax examples state that a window function sees only **post-WHERE**
rows, so a narrow `where week_seq in (…)` starves `lead/lag(…, N)` to NULL. A one-line note in the
window/lead example ("filter must retain the offset rows, or compute the window in a rowset over
the full population and filter the output") would have saved the ~20-call probe. This is the same
"include both years / window-in-rowset" pattern the canonical already encodes.
