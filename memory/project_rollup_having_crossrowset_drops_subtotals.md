---
name: project_rollup_having_crossrowset_drops_subtotals
description: SILENT — `by rollup` + `having <agg> > <cross-rowset scalar>` drops ALL subtotal/grand-total rows; HAVING→CASE-key-nulling rewrite collides with ROLLUP's NULL keys
metadata:
  type: project
---

OPEN (handoff written). SILENT WRONG RESULTS. A `by rollup (...)` select whose `having` compares an aggregate to a scalar from a SEPARATE rowset (cross-rowset/cross-source) emits leaf rows fine but drops EVERY subtotal + grand-total row. Query "works", but the requested rollup is gone.

Confirmed on q14 (THE 3.76M-token sink, run 20260701-013044): agent noticed "no rollup rows appearing" and thrashed. Toggle proof against workspace: WITH `having sum(total_sales) > overall_avg.avg_sale_value` → 0 null-key rows; drop that one line → 32 null-key rows. `ROLLUP` is in the SQL both times — DuckDB makes the subtotal rows, the HAVING rewrite eliminates them.

Root cause (generated SQL): the HAVING vs cross-rowset scalar is NOT a post-aggregation row filter — it's rewritten to `CASE WHEN <agg> > <scalar> THEN <rollup_key> ELSE NULL END` per key, feeding ROLLUP. This is the finer-dim/non-output HAVING rewrite (`_rewrite_having_finer_dims_to_membership`, `trilogy/parsing/v2/select_finalize.py:1203`; render side `dialect/base.py:963`) — fundamentally incompatible with ROLLUP, which already uses NULL keys for subtotals, so CASE-nulling conflates "filtered" with "subtotal" and the real subtotals vanish.

Fix: gate the CASE-key-nulling rewrite on `select.grouping mode == STANDARD` — for ROLLUP/CUBE/GROUPING_SETS route to a genuine post-agg HAVING/outer filter so NULL-key rollup rows survive. Family: [[project_q05_rollup_sibling_join_upgraded_to_inner]], [[project_having_post_aggregation_non_output_refs]], [[project_q70_grouping_in_where_groupless_binder]].

Trigger scope confirmed: plain-fact rollup + having-vs-literal/own-agg/same-source-scalar all KEEP subtotals (no CASE rewrite). Needs q14's layered shape (rollup keys = rowset/union outputs + HAVING scalar = a DIFFERENT rowset) — minimal isolation still TODO. Repro: `evals/tpcds_agent/repro_rollup_having_crossrowset_drops_subtotals.preql` (+ `_CONTROL_nohaving.preql`) + handoff `bug_rollup_having_crossrowset_drops_subtotals.md`.
