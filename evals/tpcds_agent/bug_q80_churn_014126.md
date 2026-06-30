# q80 churn (run 20260630-014126) — 1.33M tokens, FAILED

**Verdict: NO framework bug.** The 1.33M is context-replay over a 41-turn exploratory
session (not an error loop). The FAILED result is two independent **agent semantic
errors**; trilogy faithfully generated correct SQL for what the agent wrote, and a
minimally-corrected variant matches the reference **exactly (0/427 rollup rows differ)**.

## What actually drives the 1.33M tokens

- q80 = **41 LLM turns / 52 tool calls, all exit 0** (no retries, no error loop, no
  silent-wrong-result detection — the agent can't see the reference, so it submitted a
  result it believed correct).
- Token sink is pure **context replay**: prompt grows monotonically 1.8k → 52.4k tokens
  across the 41 turns, summing to 1.318M prompt + 11.9k completion = **1.330M**.
- Large blocks replayed every turn: `agent-info` (32.6k chars), `explore all_sales`
  (318 concepts, 8.1k), `explore --show datasources` (5.8k), `repro.preql` (6.4k),
  recursive `file list` (7.8k), plus 8 exploratory `test_check*.preql` data probes.
- This confirms the prior verdict's "replay" half. The prior "raw-model net-loss gap" is
  **resolved**: `return_net_loss` now resolves in the workspace model (catalog_sales.preql:94
  `CR_NET_LOSS`, web_sales.preql:99 `WR_NET_LOSS`) and the agent used it without error.
  No NEW framework obstacle was found.

## Why the result is wrong (two agent errors, both silent)

Reference grand-total profit = -3,597,547.40; agent = -3,599,175.01. Sales and returns
match everywhere; only **profit** differs, and only on 2 leaves + their rollup subtotals:

1. **`sum(net_profit) - sum(loss)` instead of per-row `sum(net_profit - loss)`** (delta
   -1285.37, store `AAAAAAAAIAAAAAAA`). `store_sales` has 129,573 NULL `net_profit` rows;
   store I has 4 NULL-profit lines that matched returns totalling exactly 1285.37 in loss.
   The reference's per-row `sum(net_profit - coalesce(loss,0))` drops those rows (NULL−x =
   NULL), but the agent's two-aggregate form keeps `sum(loss)` → over-counts. The canonical
   query80.preql uses the per-row form (`profit_minus_loss <- net_profit - coalesce(...)`).

2. **Spurious `sales.outlet_id is not null` filter** (delta -342.24, catalog page
   `AAAAAAAAKDCBAAAA`). `outlet_id` is a deliberately *distinct* concept from
   `channel_dim_id` (all_sales.preql:23 comment: store=`SS_STORE_SK`, **catalog=`CS_CALL_CENTER_SK`**,
   **web=`WS_WEB_PAGE_SK`**). The agent read "outlet" in the prompt and filtered on it; for
   catalog this means `CS_CALL_CENTER_SK is not null`, which drops a valid catalog sale line
   (net_profit 342.24) whose call_center_sk is NULL but catalog_page_sk is set. The canonical
   query filters only `channel_dim_text_id is not null`.

## Proof it's not the framework

- Agent's rollup is **internally consistent** (leaf-sum == subtotal == grand, 0.00 drift) —
  the rollup/grouping/nulls-first machinery is correct.
- The 3-channel `union(...)` + per-outlet returns `LEFT JOIN` (on channel,item,order) +
  `ROLLUP(1,2)` all generate correct SQL (`C:/.../q80_agent_real.sql`).
- Corrected variant = agent query with per-row profit AND the `outlet_id` filter removed →
  **0 diffs vs reference across all 427 rollup rows**. Canonical test query80.preql also
  builds and references NET_LOSS.

## Classification

- **Framework bug: none.** Compound-`grouping()`-in-ORDER-BY gap did not recur; rollup,
  union, returns join, nulls-first all correct.
- **Agent errors: 2** (two-aggregate profit under NULLs; misuse of `outlet_id` as "outlet").
- **Churn: replay**, not a planner obstacle.

## Latent footgun (model quality, not a bug)

`outlet_id` in all_sales.preql is a naming trap: the name reads as "the selling outlet"
(store/catalog_page/web_site) but it maps to call_center/web_page. A prompt saying "outlet"
pulls an agent straight to it, silently dropping rows. Worth renaming or documenting on the
concept, but it is a modeling-clarity issue, not a framework defect.

Files: workspace/query80.preql (agent), tests/modeling/tpc_ds_duckdb/query80.{sql,preql}
(reference/canonical), raw/all_sales.preql:23,71,104,137 (`outlet_id` definition).
