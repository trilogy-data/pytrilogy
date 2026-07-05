# q64 regression — run 20260630-014126 (1.52M tokens, FAILED)

> **SPLIT INTO TWO ACTIONABLE HANDOFFS (2026-06-30):**
> - 🔴 **CRITICAL** — Obstacle #2's mislabeled error → [bug_planner_raises_syntaxerror_with_internal_repr.md](bug_planner_raises_syntaxerror_with_internal_repr.md) (planner raises builtin `SyntaxError` w/ internal repr at `concept_strategies_v3.py:280`).
> - Obstacle #1's silent no-op → [bug_run_zero_statements_silent_success.md](bug_run_zero_statements_silent_success.md) (0 executable statements reported as success).
>
> This file remains the discovery context. The "regression" was iteration-climb, not a code regression; the wrong answer is agent modeling (see below).

**From-scratch investigation, 2026-06-29.** q64 burned 1,502,223 prompt + 19,287
completion = **1.52M tokens** across **41 LLM round-trips / 46 tool calls**, and the
final submitted query *runs but is wrong* (2 cand vs 2 ref rows, differ).

## Regression cause (lead)

The "3×" framing (522k→1.52M) understates a **monotonic climb across consecutive
runs of the SAME query**: `925k` (20260628-194910) → `1.24M` (20260629-013151) →
`1.52M` (this run). The token sink is **iteration count × growing context** — every
one of the 41 turns re-sends the whole conversation (prompt grows 1.9k→55k tokens;
the agent-info guide alone is 32.5k chars / ~8k tokens re-sent each turn). There is
**no single regressing commit** I can pin from the data (no prior-run per-turn jsonl
to diff), but the *dominant framework contributor changed*: prior runs churned on the
`_virt_filter` repr leak (see `bug_q64_churn_013151.md`); **this run sidesteps that**
(agent used `ss.is_returned` from the start) and instead fell into a **silent
"0 statements" false-success trap (3×)** plus the long-standing cross-import
conform obstacle. The canonical `tests/modeling/tpc_ds_duckdb/query64.preql` still
**builds clean (1.7s, 17,967-char SQL)** — no regression there.

## Obstacle #1 (PRIMARY, framework UX bug) — silent "0 statements" false success

The agent wrote a file whose only statements are rowset/`with` definitions and **no
final consuming SELECT**, three separate times (tool calls #13/#14, #16/#17, #35/#36).
Each `run` returned, verbatim:

```
Executing 0 statements...
Execution Complete   |  Statements: 0   (exit 0)
```
…surfaced to the agent as `{"ok": true, "rows": 0}`. **No error, no warning that the
file is inert.** The agent had to infer on its own each time that a file of only
rowset definitions executes nothing — three full write→run→diagnose cycles plus the
LLM turns to realize it.

**Minimal repro** (in the run workspace):
```trilogy
import raw.store_sales as ss;
with base_agg as
select ss.item.id, count(ss.line_item) as line_count;
```
→ `Executing 0 statements... Execution Complete`, exit 0.

**Root cause — `trilogy/executor.py:711-730`** (`parse_text_generator`): only
`SelectStatement / PersistStatement / MultiSelectStatement / ShowStatement /
RawSQLStatement / CopyStatement / ValidateStatement / CreateStatement /
PublishStatement / MockStatement / ChartStatement` are "generatable". A
rowset/`with` derivation registers into the environment and yields nothing, so an
all-rowset file returns `[]`. **`trilogy/scripts/single_execution.py:251-281**
(`execute_run_mode`) then runs the empty list and calls `show_execution_summary(0,
…, exception is None=True, 0)` — there is no `len(queries)==0` guard to warn "file
parsed N definitions but has 0 executable statements; add a final SELECT".
**Classification: REAL framework bug** (silent no-op reported as success). A
zero-executable-statement warning/non-zero signal would have saved ~6+ turns.

## Obstacle #2 (recurring, correct-by-design disconnect + cryptic error)

The natural way to express "items where cumulative catalog list-price > 2× catalog
refund" is to import `catalog_sales as cs` and `catalog_returns as cr` separately and
compare `sum(cs.ext_list_price) by cs.item.text_id` against
`sum(cr.refund…) by cr.item.text_id`. **`cs.item` and `cr.item` are distinct
concepts** (separate imports of the same logical dimension), so the two aggregates
live in disconnected subgraphs.

- Agent's form (call #28, membership wrapper) → `Resolution error: Discovery error:
  cannot merge all concepts into one connected query … split into 2 disconnected
  subgraphs: {cs.item.id, cs.item.text_id}; {local._virt_agg_sum_…}`.
- Minimized direct form → a **worse, mislabeled** message:
  `Syntax error … Have {'GroupNode<cs.item.text_id,local.a>': None} and need
  local.a > multiply(2, local.b@Grain<cr.item.text_id>)` — an internal planner repr
  mislabeled as a *Syntax* error.

The canonical query exists almost entirely to dodge this: it reaches catalog_sales
*through* `cr.sales` (cr already imports catalog_sales) "so we avoid the cross-import
merge" (its own comment). **Classification: correct-by-design disconnection, but the
errors are a framework UX bug** — neither message names the conform/`cr.sales` idiom,
and one path mislabels a resolution failure as Syntax. Agent recovered once (added an
explicit `inner join cs.item.text_id = cr.item.text_id`, call #32) but it cost turns.

## Obstacle #3 (agent/model friction, framework already helps)

FROM keyword (#18,#40), GROUP BY (#23), missing-alias (#41), undefined-concept typo
`first_shipto_year` vs `_yr` (#34), and a chained-`by` inside a membership subselect
(#31). Trilogy already emits friendly [101]/[103]/[201] hints for these.
**Classification: mostly agent/model** (deepseek fighting Trilogy syntax).

## Why the final answer is wrong (not a token issue, noted for completeness)

The submitted `query64.preql` models "has a matching store return" as
`ss.is_returned = true` and groups on `ss.item.text_id` (string) rather than a real
store_returns join on (ticket_number, item) + the surrogate `item.id`. It runs (2
rows, 160ms) but differs from the reference.

## Recommendation (do NOT fix here)

1. **Highest leverage:** warn / non-zero-signal when `run` produces 0 executable
   statements (guard at `single_execution.py:251` / `executor.py:711`). Directly kills
   the 3× false-success churn.
2. Make the cross-import comparison error name the conform idiom (reach the second
   fact through the first's import, e.g. `cr.sales.*`), and stop mislabeling the
   `Have … and need …` resolution failure as a *Syntax* error.
