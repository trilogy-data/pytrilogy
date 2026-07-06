# q80 returns-netting sink (run 20260706-222300) — NOT a framework bug; NOT a FULL-vs-LEFT join defect

**Target:** q80 FAIL, 521k-token sink (down from 553k old run `20260706-135542_enriched`). Result
differs (ref 100 / cand 100 rows), no hard error → SILENT. Model deepseek-chat.

**Verdict:** **No `trilogy/` engine bug.** The token sink is **schema-dump context replay** (same as all
three prior churn reports). The FAIL is the **agent's two-aggregate profit form** under NULL `net_profit`
rows — the *exact* finding of `bug_q80_churn_014126.md`, still applying byte-for-byte on the current
engine (same −1285.37 delta). The returns-netting join is **CORRECT** — it is **NOT** the
"union join = FULL vs ref LEFT" issue the task/memory flagged.

## The diff (candidate vs canonical reference)

Harness: `scoring.make_scoring_engine` on a copy of `workspace/tpcds.duckdb` (the live file was locked
by another PID), `generate_sql(query80.preql)`, reference = `tests/modeling/tpc_ds_duckdb/query80.sql`.

- 100 cand rows, 100 ref rows, **all 100 (channel,outlet) keys present in both** — no missing/extra rows.
- **Sales and returns match to the cent on every row.**
- **Exactly ONE value differs:** the fully-rolled-up grand-total row `(channel=NULL, outlet=NULL)`
  **profit**: cand `-3598832.77` vs ref `-3597547.40`, **Δ = -1285.37**.

The per-leaf store row that carries the error and the store subtotal fall outside the top-100
(ordered by channel/outlet), so only the grand total (which sorts first, nulls-first, and sums all
leaves) exposes the discrepancy.

## Root cause of the FAIL — agent semantic error (NULL net_profit × two-aggregate form)

Agent's union arms (`workspace/query80.preql:15,26,37`) compute profit as **two aggregates**:
```
sum(store_sales.net_profit) - sum(coalesce(store_sales.return_net_loss, 0)) as profit
```
The canonical reference computes it **per-row**: `sum(net_profit - coalesce(return_net_loss, 0))`.

These differ only over rows with **NULL `net_profit`**. Verified against the DB: in the q80 window
there are **exactly 2 store lines with NULL `net_profit` whose matched returns carry
`sr_net_loss` totalling 1285.37**. Per-row `net_profit - coalesce(loss,0)` = `NULL - 1285.37 = NULL`
→ `sum()` drops them → they contribute 0. The two-aggregate form drops the NULLs from `sum(net_profit)`
but still adds their 1285.37 to `sum(loss)` → grand-total profit lower by exactly 1285.37.

**Proof:** rewriting the three arms to the per-row form
`sum(net_profit - coalesce(return_net_loss, 0))` builds cleanly and yields **0 value diffs vs the
reference across all 100 rows.** (scratch `perrow80.py`).

## The returns-netting join is CORRECT — NOT FULL-vs-LEFT

The task/memory hypothesis (per-channel returns netting = union join FULL vs ref LEFT, or a returns
fan-out) is **disproven**:
- `sales` and `returns` match the reference **exactly on every outlet** — a FULL join or a returns
  fan-out would perturb these too; they don't.
- With per-row profit the whole result matches **to the cent** — the sales→returns→promotion→item
  anti-pattern (`return_amount`/`return_net_loss` sourced from the returns datasource in
  `store_sales.preql:81` etc., padded/nullable) resolves and nets correctly.
- Rollup, 3-arm `union(...)`, and `nulls first` ordering are all internally consistent.

## What actually drove 521k tokens (not a planner loop)

78 jsonl records, a handful of LLM turns; the cost is **per-turn context size** (agent-info + `explore`
fact-model dumps re-sent every turn under a no-prompt-cache provider), identical to
`bug_q80_churn_{014126,030015,214830}.md`. Only **3 framework-surfaced errors, each a clean authoring
error recovered in ONE turn**:
1. `Syntax [222]: Missing \`;\`` — agent omitted the `;` after the `union(...) -> (...)` signature
   (jsonl ~msg 39). Actionable message, fixed next turn.
2. `\`by rollup (…)\` requires at least one aggregate … found none` (jsonl:69) — the agent's outer
   select first listed passthrough columns `combined.sales, combined.returns, combined.profit` with no
   `sum()`. The union output signature `-> (… sales numeric …)` strips aggregate lineage, so a bare
   passthrough under `by rollup` genuinely has nothing to group; the error is **correct and actionable**,
   and the agent recovered by wrapping in `sum(...)`.

No thrash loop, no `Unexpected error`/`Binder`/`Catalog`/sentinel from generated SQL.

## Mild guidance contribution (not an engine bug)

The rollup guidance demonstrates the composite-measure form **`sum(a) - sum(b) as net by rollup (…)`**
verbatim — `trilogy/ai/constants.py:177` and `trilogy/ai/syntax_examples.py:653` — **without noting it
differs from per-row `sum(a - coalesce(b,0))` when an operand can be NULL.** This is the closest thing
to a framework contribution to the FAIL: it reinforces the exact two-aggregate shape the agent used.
Optional (separate from any bug fix): add a one-line caveat that composite measures with a nullable
operand should be spelled per-row when the semantics must match SQL's row-wise arithmetic.

## Which prior finding still applies on the current engine

- `bug_q80_churn_014126.md` (two-aggregate profit vs per-row under NULL `net_profit`): **STILL APPLIES
  EXACTLY** — same −1285.37 delta, same store leaf.
- Earlier reports' catalog/web `return_net_loss` **data gap: RESOLVED** — `catalog_sales.preql:96`
  (`CR_NET_LOSS`), `web_sales.preql` (`WR_NET_LOSS`); the agent referenced them without error.
- Case-C compound-`grouping()`-in-ORDER-BY gap (`select_finalize.py`): **did NOT recur** — the agent
  ordered by plain `channel/outlet nulls first`, no `grouping()`; a quick probe on the current engine
  built OK, so it never touched q80 this run.

## Classification

- **Framework engine bug: NONE.** Both profit forms build; per-row matches reference to the cent;
  returns join / rollup / union / nulls-first all correct. NOT a FULL-vs-LEFT returns-netting defect.
- **Agent semantic error:** two-aggregate `sum(np) - sum(loss)` instead of per-row
  `sum(np - coalesce(loss,0))`, wrong only because `store_sales.net_profit` has NULL rows.
- **Guidance nuance (low sev):** `constants.py:177` / `syntax_examples.py:653` model the two-aggregate
  composite form without a nullable-operand caveat.
- **Token sink:** schema-dump replay, not planner iteration.

No fix to `trilogy/` is warranted for the FAIL.

Files: `workspace/query80.preql` (agent), `tests/modeling/tpc_ds_duckdb/query80.{sql,preql}`
(reference/canonical), `raw/store_sales.preql:81` (returns datasource), `trilogy/ai/constants.py:177`,
`trilogy/ai/syntax_examples.py:653`.
