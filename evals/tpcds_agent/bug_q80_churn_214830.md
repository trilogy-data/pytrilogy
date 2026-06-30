# q80 churn (run 20260629-214830): NOT a framework crash — token sink is schema-dump accumulation; the FAIL is an agent/model correctness divergence

**Status:** investigated. **Verdict:** **no framework code bug.** The 1.22M tokens were burned on
**large `explore` schema dumps replayed through history** (deepseek-chat re-sends full context each
turn), not framework thrash. The recorded **FAIL** (`status:fail`, `exit_code 0`, not timed out,
`detail: "result set differs from reference"`) is a **correctness divergence driven by (a) a raw-model
data gap and (b) agent ordering/labeling choices** — both outside the framework engine. The one true
framework gap (compound `grouping()` in ORDER BY, case C) is **low-sev and was worked around by the
agent**; it confirms the prior report `bug_q80_churn_030015.md`.

Run: `evals/tpcds_agent/results/20260629-214830/` (`agent_log.q80.{jsonl,conversation.txt}`,
`workspace/query80.preql`). Model: **deepseek-chat**, `max_iterations 75`.

## What actually drove 1.22M tokens

Only **38 tool calls / 28 LLM turns / 5 error events** in the whole q80 log — this is NOT high
iteration count. The cost is **per-turn context size**:

- A *single* pass of tool results in the q80 log is **~150k chars (~37.5k tokens)**, dominated by:
  `agent-info` (32.4k chars), and `explore` dumps of the fact models — `all_sales` 318 concepts (8.1k),
  `web_sales` 440 (11k), `store_sales` 316 (8.1k), `catalog_sales` 291, plus `--show all` / `--regex`
  re-explores. ~37.5k tokens × 28 replayed turns ≈ the observed ~1.2M.
- Error events were only: 1× `Undefined concept: catalog_sales.return_net_loss` (step 51), 2× `refused
  to write … Parse error` (steps 77/80, a malformed write the agent fixed in one shot), 1× `ORDER BY
  contains aggregate grouping(combined.channel_label)` (step 86, recovered next write). Each recovered
  in ONE iteration. **No thrash loop.**

Conclusion: under the >500k⇒bug heuristic this run's tokens are a **harness/agent-strategy cost**
(massive schema dumps under a no-prompt-cache provider), not a framework defect that loops the agent.

## The recorded FAIL = correctness divergence (exit 0, clean run)

The agent's final `query80.preql` runs clean and returns 100 rows, but differs from the reference on:

### (1) PRIMARY: wrong catalog/web profit — **raw-model data gap, not engine bug**
Canonical profit is per-row `sum(net_profit - coalesce(return_net_loss, 0))`. The agent's union arms:
- store: `sum(net_profit) - coalesce(sum(store_sales.return_net_loss),0)`  ✓ (store model has it)
- catalog: `sum(catalog_sales.net_profit)` — **return_net_loss dropped**
- web: `sum(web_sales.net_profit)` — **return_net_loss dropped**

Root: the **per-channel ingested models are inconsistent**.
`workspace/raw/store_sales.preql:43` maps `SR_NET_LOSS → return_net_loss`, but
`raw/catalog_sales.preql` (CR_RETURN_AMOUNT only, **CR_NET_LOSS unmapped**) and `raw/web_sales.preql`
(WR_RETURN_AMT only, **WR_NET_LOSS unmapped**) expose **only `return_amount`**. Only the conformed
`raw/all_sales.preql` maps SR/CR/WR_NET_LOSS → `return_net_loss` (lines 219/234/249). So
`catalog_sales.return_net_loss` genuinely **does not exist**; the framework's `Undefined concept …
Suggestions[…]` was correct. The agent recovered by *deleting the loss term* instead of switching to
`all_sales` (which it had explored). Verified diff vs a canonical-style query on the same DB: outlets
with returns differ by exactly the omitted net loss, e.g. `catalog_pageAAAAAAAAACABAAAA` profit
agent `-3106.70` vs canonical `-4656.66` (Δ=1549.96), `…AGCBAAAA` `252.94` vs `-322.98`; outlets with
zero returns match to the cent. **Classification: data-model/usability gap + agent recovery error.**

### (2) SECONDARY: ordering/labeling divergence — **agent spec deviation**
Spec & canonical: `order by channel asc nulls first, id asc nulls first` (rollup NULLs sort first,
kept as NULL). Agent added `case when grouping(...)=1 then 'ALL CHANNELS'/'ALL OUTLETS'` **and**
`order by _level asc, …` (a hierarchy-level sort the spec never asked for). With `limit 100` this can
reorder/relabel which rows survive vs the reference. Pure author choice; engine rendered it faithfully.

### (3) NOT a bug: per-outlet returns matching is correct — **no fan-out**
Both the union-arm form and the all_sales form produce **identical** per-outlet `sales` and
`returns_amount` to the cent (returns matched by order/ticket+item, missing→0). No fan-out, no silent
miscount in the returns join. The only value delta is the deliberately-dropped net-loss term above.

## Framework gap actually present (low-sev, worked around) — case C

`order by grouping(a)+grouping(b)` **inline** (compound) still raises the clean author error even when
an **identical hidden projection** `--grouping(a)+grouping(b) as _level` exists:
`Syntax error: ORDER BY contains aggregate grouping(local.channel) …`. Reproduced on the workspace DB.
The agent's **final working query orders by `_level` (the alias name)** — that path works (exit 0), so
**this never blocked q80**. Trigger matrix matches prior report: order-by an alias-name ✓, single inline
grouping over its projection ✓, leaf-projected groupings + alias arithmetic ✓; only the inline *compound*
form against a compound projection ✗.

### Root cause (file:line, unchanged from prior report, lines shifted)
`trilogy/parsing/v2/select_finalize.py`:
- `_aggregate_full_signature` (**L278**) signs only a *single* aggregate node; returns `None` for an
  arithmetic `add(grouping(a),grouping(b))`, so a projected **compound** `…as _level` is never
  registered in `_select_order_match_outputs` (**L1446**) / `_substitute_order_by_aggregates` (**L1460**).
- `_validate_order_by_aggregates` (**L1490**) collects the **leaf** `grouping(channel)`, finds no
  single-aggregate projection signature for it, and raises at **L1520**. Fix surface (not applied):
  register compound-expression outputs by structural signature, or match an ORDER-BY sub-expression
  whose whole shape equals a projected compound output.

## Relation to the q70 rollup/grouping family
Same `select_finalize.py` rollup/grouping area as the FIXED q14 grouping-normalization and the q70
work, and conceptually adjacent to the q70 "two equal aggregates not collapsed" theme. But q70's bugs
are genuine **crashes** on valid queries (RecursionError / INVALID_REFERENCE sentinel); q80's case C is
a **non-crashing auto-resolver coverage gap** with a working alias-name path. Lowest priority of the family.

## Bottom line for the user (re: "suspicious it's bigger")
The prior low-sev verdict on the grouping-in-ORDER-BY signal **holds** — but the prior report missed
that *this* run **FAILED on correctness**. That failure is **not** a framework engine bug: it is a
**raw-model inconsistency** (`catalog_sales`/`web_sales` omit `return_net_loss` that `store_sales` and
`all_sales` carry) compounded by the agent dropping the term and adding an unrequested `_level` sort.
The token sink is **schema-dump replay**, not framework iteration. **No fix to `trilogy/` warranted for
the FAIL.** Optional, separate improvements: (a) close the model gap by mapping CR_NET_LOSS/WR_NET_LOSS
in the per-channel raw models (or steer agents to the conformed `all_sales`); (b) the low-sev case-C
compound-grouping auto-resolve in `select_finalize.py`.
