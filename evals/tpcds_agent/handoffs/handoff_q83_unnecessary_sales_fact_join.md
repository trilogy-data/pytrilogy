# Handoff — q83: source-selection joins huge `*_sales` facts that contribute no output column (runaway perf)

**Verification:** ⚠️ SUBAGENT-REPORTED, STILL OPEN (2026-07-08) — CONFIRM the repro/timing before
fixing. No guard test exists for the unnecessary-fact-join shape (the `test_duckdb_rowset.py::q83`
guard covers a *different*, already-fixed ORDER-BY-measure issue). The former detail report
`bug_q83_timeout_regression_20260706.md` has been deleted; this handoff is now the sole record.

## Confirmed-by-subagent bug
The symptom is a RUNAWAY QUERY, not a planner loop: `generate_sql` is <1s but each
`trilogy run query83.preql` takes ~4.6 min (`ROWS 24 in 277.2s`) and returns CORRECT rows; 2–3
iterate-and-run cycles blow the 900s agent wall → scored `timeout`.
- The generated SQL RIGHT-JOINs the huge `*_sales` fact tables (store 2.88M + catalog 1.44M + web 719k)
  that contribute NO output column — `item` is reachable via the returns FK, but source-selection
  sources the shared grain-key `s.item.id` from the SALES datasource instead of the already-selected
  RETURNS datasource.
- Trigger (all three needed): item dimension output + a return measure + a row-level `where s.channel=`
  filter, over `import raw.all_sales`. Remove any one → the sales join vanishes. Canonical dodges it via
  filtered aggregates (`? channel=`).

## Root cause (locus)
`trilogy/core/processing/concept_strategies_v4.py` — datasource/source selection picks the sales
datasource to satisfy a shared grain key (`s.item.id`) already satisfiable from the selected returns
datasource, forcing an unnecessary multi-million-row join. Trace where a shared/pseudonym grain key's
source datasource is chosen when a cheaper already-in-plan datasource provides it.

## Fix direction
Prefer an already-selected datasource that can supply a shared grain key over introducing a new
(large) fact-table join that yields no output column. Perf-only (results are already correct), so a
safe heuristic: when a needed key is available from a datasource already in the join graph, don't add a
second source for it.

## Guard test
Hard to assert on wall-time. Assert on GENERATED SQL structure instead: for the trigger shape (item
output + return measure + row-level channel filter over `all_sales`), assert the compiled SQL does NOT
join the `store_sales`/`catalog_sales`/`web_sales` fact tables (only the `*_returns` + item/dims).

## Not a regression
Reproduces on a 2-line query independent of `4e69c5547`. Pass→timeout is agent-strategy variance
(3 separate returns rowsets last run → `all_sales` + `union join` this run), nudged by the `all_sales`
model doc ("default for multi-channel").
