# q14 current-engine verdict — run 20260706-001731 (930,508 tok, FAIL)

Investigated on today's engine against the failing run's own workspace DB
(`evals/tpcds_agent/results/20260706-001731/workspace`, opened READ-ONLY).
q14 exit 0, 100 rows, `"result set differs from reference"` — **silent wrong
rows**. The driver this run is **agent modeling error, not a framework bug**.
The engine bug the prior recheck doc flagged as "still open" is now **FIXED**.

## Headline

- The recheck doc (`bug_q14_recheck_20260705.md`) root-caused the churn to a
  multi-key `subset join` onto a rowset emitting a DuckDB `BinderException`
  (`column "..._category_id" must appear in the GROUP BY`). **That is FIXED** —
  see MEMORY.md top entry `FIXED 2026-07-05 q14 multi-key subset join onto
  rowset + by rollup (4 fixes)`, guards `test_duckdb_rollup_scoped_join.py` +
  `test_group_node_grouping_grain.py`.
- The agent this run **never used `subset join`** (0 occurrences in any query
  attempt; all 3 hits in the log are the agent-info docs). It went straight to
  the canonical-family **concat composite-key** idiom — and still failed, on
  its own modeling choices.

## What actually failed (submitted `workspace/query14.preql`)

Submitted grand total `(None,None,None,None, 674058899.43, 25813)`
vs reference `(None,None,None,None, 673409655.64, 155567)`. Three **independent**
correctness bugs, each of which alone fails the multiset compare:

1. **UPPERCASE channel** — projects raw `all.channel` (`'STORE'/'CATALOG'/'WEB'`);
   reference/canonical emit lowercase `'store'/'catalog'/'web'`. The model's
   `channel` enum is uppercase; canonical explicitly maps it down. Guaranteed
   label mismatch on every row.
2. **`num_sales = count(all.item.id)`** — `item.id` is a KEY, so `count(key)` is
   distinct-by-grain: `count(item.id)` for Nov-2001 = **9000** (distinct items),
   grand total **25813**; reference `count(*)` = row count, Nov-2001 = **157093**,
   grand **155567**. Off by ~6x. The model exposes `sale_line_item_counter` for
   exactly this (canonical uses `sum(sales.sale_line_item_counter)`).
3. **`combo_key = concat(coalesce(brand::string,'NULL'),'_',…)` + no `grouping()`
   guard** — the `coalesce(...,'NULL')` over-qualifies null-attribute combos, and
   with bare (non-`case grouping()`) rollup projections the real-NULL-attribute
   leaf rows collide with rollup subtotal rows. Result: multiple distinct
   `('CATALOG',None,None,None,…)` rows (leaf-nulls indistinguishable from the
   channel subtotal). Grand total drifts to 674058899.43 vs 673409655.64.

## Trigger matrix — current engine (all against workspace DB, ref = `PRAGMA tpcds(14)`)

| shape | current-engine result |
|---|---|
| canonical `tests/modeling/tpc_ds_duckdb/query14.preql` (concat `tuple_key` membership + rollup + having-vs-scalar) | **builds + MATCHES-REF** (100 rows) |
| **3-key `subset join` onto rowset, NO rollup** | EXEC OK 100 rows (was DisconnectedConcepts) |
| **2-key `subset join` + rollup** | EXEC OK 100 rows (was DisconnectedConcepts) |
| **3-key `subset join` + rollup + `case grouping()` + having-vs-scalar** (full q14 shape) | **EXEC OK 100 rows** — no BinderException (was BinderException) |
| …same, with lowercase channel mapping + order-by-aliases | EXEC OK 100 rows, DIFFERS only in null-tuple qualification (674.7M/156542) — a null-handling nuance vs canonical's concat form, NOT a crash |
| submitted `workspace/query14.preql` (concat + coalesce combo, uppercase channel, `count(item.id)`) | EXEC OK 100 rows, **DIFFERS** (agent bugs 1-3 above) |

Every subset-join shape from the recheck doc that previously raised
`BinderException` / `DisconnectedConcepts` now **builds and executes**. The
recheck doc's minimal repro (its lines 56-80) returns 100 rows here.

## What drove the 930k churn

**Not an engine wall.** 25 iterations / 31 `trilogy` calls (5 agent-info, 11
file, 6 explore, 9 run), 4 self-inflicted parse errors:
comma-RHS `auto qualifying <- a, b, c`, a `FROM` keyword, `ORDER BY contains
aggregate`, and one explore stdin syntax slip. Prompt tokens (915k) are
dominated by repeatedly re-sent large agent-info / model-explore dumps (the
`item` and `all_sales` model dumps run up to ~32.8k chars each) across a growing
context. The agent reached a **validating-but-wrong** query and submitted it —
the wrong answer was never surfaced because scoring is offline.

## Root cause / classification

- **Framework bug: NONE live for q14 on the current engine.** The prior
  BinderException (recheck doc root-caused to `MergeNode._inject_scoped_join_key_exposure`,
  `trilogy/core/processing/nodes/merge_node.py:311-348`, grain-blind key
  exposure into a ROLLUP CTE) is fixed; subset-join-onto-rowset + rollup +
  grouping now builds and runs.
- **Agent error (primary driver):** channel not lowercased; `count(item.id)`
  (distinct-key) instead of `sum(sale_line_item_counter)` / row count;
  `coalesce`-combo over-qualification with no `case grouping()` guard.
- **Guidance contributors (soft):**
  - The agent independently found the concat composite-key idiom, so the
    "no composite-membership example" gap from the recheck doc is **less
    load-bearing** than claimed — but agent-info still lacks a worked
    composite-membership example and offers no steer on (a) mapping the
    uppercase `channel` enum to the reference's lowercase labels, or (b) that
    `count(<key>)` is distinct-by-grain so row counts need
    `sale_line_item_counter` (the guidance says "count(key) is already
    distinct" at log line ~567 but the agent still used `count(item.id)`).
  - The `--`-commented-alias masking bug noted in the recheck doc was not
    re-exercised this run (agent used no commented aliases in the final file).

## Bottom line

On the current engine q14's failure is **agent modeling** (three independent
correctness bugs), amplified by large-context exploration churn. The multi-key
`subset join` engine bug the recheck doc reported as still-open is **FIXED**, and
the canonical `.preql` **matches reference**. No framework fix is indicated for
q14; the remaining leverage is guidance (channel-label lowercasing +
count-of-key-is-distinct + a composite-membership example) so the agent lands on
the canonical shape it is already circling. Not fixed (per task).
