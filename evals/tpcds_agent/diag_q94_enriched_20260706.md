# Diagnosis: q94 FAIL in ENRICHED leg, run 20260706-135542

## Classification: AGENT error (output-shape / wrong output grain) — sibling pattern 4 (same family as q74)

## Symptom
- `QueryResult(id=94, status='fail', ref_rows=1, cand_rows=32, detail='result set differs from reference')` — reproduced against a DB copy.
- Reference (`tests/modeling/tpc_ds_duckdb/query94.sql`): ONE global row `(32, 65144.28, -19548.52)` = count(DISTINCT ws_order_number), sum(ws_ext_ship_cost), sum(ws_net_profit).
- Candidate (`results/20260706-135542_enriched/workspace/query94.preql`): 32 rows at ORDER grain — selects `ws.order_number` visibly plus `count(ws.order_number)` (=1 per row) and per-order sums.

## Error inventory (transcript agent_log.q94.conversation.txt)
1. msg 21 — one Trilogy syntax error: `Undefined concept: warehouse.id` (agent wrote bare `warehouse.id` instead of `ws.warehouse.id`); self-corrected immediately using the suggestion list. Unrelated to the failure.
2. No framework errors (no Binder/Catalog/Unexpected/planner errors) anywhere in the transcript. Final query ran cleanly.

## What the 32 rows are — decomposition
Candidate rows = the 32 qualifying orders, one row each: `(order_number, 1, per-order sum(ext_ship_cost), per-order sum(net_profit))`.
Re-aggregating them — `count(distinct order_number)=32`, `sum(col3)=65144.28`, `sum(col4)=-19548.52` — reproduces the reference row EXACTLY.
Therefore this is a PURE output-grain mismatch: all filtering logic is correct, including
- ship-date window / IL / company 'pri',
- multi-warehouse condition (`count_distinct(ws.warehouse.id) by ws.order_number > 1` ≡ reference EXISTS-different-warehouse),
- not-returned condition (`max(ws.is_returned::int) by ws.order_number = 0` ≡ reference NOT EXISTS web_returns).
No recurrence of the 2026-07-05 UpgradeJoinOnGuards semijoin/padded-join bug: the anti-join semantics are numerically exact.

## Regression check (join-upgrade fix of 2026-07-05)
Canonical `tests/modeling/tpc_ds_duckdb/query94.preql` built through the same engine (schema-qualified `"memory".` prefix stripped to match the scoring attach) returns `(32, 65144.28, -19548.52)` — byte-equal to the reference. The fix holds; NOT a framework regression.

## Counterfactual proof of agent error
Transcript msg 26 shows the agent explicitly considered the correct single-row reading and rejected it:
- "maybe the output should be a single row aggregating all orders? ... But the question says 'limit 100 rows' and 'order by the order count' which suggests multiple rows."
- Then a false recall: "In the standard TPC-DS Q94, the result shows each order with its details ... it typically returns one row per order" (false — Q94 returns one aggregate row).
- Concluded "The output looks correct actually - 32 rows, each with order_count=1" and submitted.
The question text is unambiguous ("report THE count of unique orders, THE total extended shipping cost, and THE total net profit" — three scalars), and the db-only SQL leg produced the correct single-row shape from the same wording (`results/20260706-135542_sql_bare/workspace/query94.sql`: `COUNT(DISTINCT ...), SUM(...), SUM(...)` with no GROUP BY → pass). The `order by the order count; limit 100` boilerplate (faithful to the TPC-DS template) contributed to the confusion but did not mislead the SQL leg.

## Root cause
Agent added `ws.order_number` as a visible select column, changing the output grain from global (1 row) to per-order (32 rows), and rationalized the shape mismatch with an incorrect memory of the benchmark's expected output. Scorer hashes whole rows, so extra key column + wrong grain = hard fail despite numerically perfect underlying logic.
