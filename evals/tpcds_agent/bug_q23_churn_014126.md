# q23 token-sink autopsy — run 20260630-014126 (1.57M tokens, FAILED)

Model: **deepseek-chat** (not Claude). q23: 46 turns, exit_code 0, **no framework
sentinel**. Final result FAIL: 19 candidate rows vs 4 reference ("result set differs").

## TL;DR classification

| aspect | verdict |
|---|---|
| What drives the 1.57M tokens | **Context replay, no prompt caching** — structural, NOT a framework obstacle |
| Why the query FAILED (19 vs 4) | **Agent logic error** (frequent-item membership by *description-prefix* instead of *item.id*) |
| Genuine framework finding | **`--` silently parses as double unary-minus, not a comment** (a real silent-semantics footgun; **benign here**) |

There is **no crash loop, no silent re-try loop, no framework sentinel**. The agent
converged in 46 turns and submitted once (single `return_control_to_user`).

## 1. The 1.57M is replay, not churn (hard evidence)

Summing `usage.prompt_tokens` over the 46 `llm_response` records in
`agent_log.q23.jsonl`:

```
turns=46  sum_prompt=1,556,214  sum_completion=11,165  sum_total=1,567,379
```

Per-turn prompt grows **linearly** 1.7k → 44.6k (turn 0 → 45). The **max single-turn
context is only ~44.6k tokens**; the 1.55M is just that growing prefix re-sent every
turn with no caching. Completion is trivial (11k total). So the "sink" is structural to
the harness/provider cost model (deepseek, uncached prefix) × 46 turns — every query in
this run pays it; q23 merely had more turns. This is **replay**, not a q23 obstacle.

Turn budget breakdown (from the jsonl call/result trace): 8 exploration calls
(agent-info ×3, explore ×6, file list), 8 `query23.preql` writes (4 rejected on
footguns), plus 3 scratch "check" files each with write+run+revise cycles, then deletes.
Only ~6 tool errors, all recoverable footguns with helpful messages:
- Syntax **[104]** ×3 — agent placed `auto`/`rowset` definitions *after* `where/select`.
- `rowset frequent_desc **as** …` instead of `<-`.
- Type error — `store.item.id` (INTEGER) into `||`/CONCAT without `::string` (the
  "INTEGER into CONCAT" the prompt mentioned). One-shot fixed.

None are framework bugs.

## 2. Why it returns wrong rows = agent logic, not framework

Canonical `tests/modeling/tpc_ds_duckdb/query23.preql` **builds** (generate_sql, 8159
chars) and matches items by **`sales.item.id in frequent_items.frequent_item_id`**.

The agent's `workspace/query23.preql` instead matches by **description prefix**:
`substring(all.item.desc,1,30) in frequent_desc.desc_prefix`. Different items share a
30-char desc prefix, so the frequent set is broader → more customers pass → 19 rows.
This is a modeling choice by the agent, reproducible and deterministic. Not a framework
defect.

## 3. The real framework footgun: `--` is NOT a comment

The agent "commented out" two lines with SQL-style `--`:
```
--count_distinct(store.item.id::string || store.date.date::string) as pair_count,
--cust_total as total,
```
Trilogy's grammar recognizes **only `#` and `//`** as comments
(`trilogy/parsing/trilogy.lark:752` `COMMENT: /#.*(\n|$)/ | /\/\/.*\n/`; same at
PARSE_COMMENT line 26). `--` is therefore parsed as **two unary minuses**
(`UNARY_MINUS: /-(?![0-9.>])/`, trilogy.lark:395). Proof:

```
auto a <- --5;   -- a = 5     (double negation = identity)
auto b <- -5;    -- b = -5
```

So both "commented" lines stayed **LIVE**: `--count_distinct(…) as pair_count` defines a
real concept `pair_count` (confirmed: `frequent_desc.pair_count` is a selectable output),
and `having pair_count > 4` resolves. Minimal trigger — with the `--` line present the
HAVING resolves and runs; **delete that one line and you get `Undefined concept:
local.pair_count`** (because `--` was the only thing defining it).

**This is benign in q23**: double-negating a non-negative `count` is identity, so
`> 4` and `> 0.5*max` are unaffected; the query did roughly what the agent intended. But
it is a genuine **silent-wrong-results trap**: an author/agent who writes `--expr` as a
comment gets a live double-negated column with no warning. (Contrast: a *plain* `--`
comment like `-- note` does error, because its tokens aren't a valid expression — so the
trap specifically bites when the "commented" text is itself valid syntax, exactly the
copy-paste-then-`--`-out case.)

## Root cause (do NOT fix — informational)

- Token sink: uncached prefix replay × 46 turns — harness/provider cost model, not code.
- Failure: agent logic (desc-prefix vs item.id membership) in `workspace/query23.preql`.
- Footgun: no `--` line-comment token; `--` falls through to
  `UNARY_MINUS` ×2. `trilogy/parsing/trilogy.lark:752` (comment def) +
  `:395` (`UNARY_MINUS`). A friendly "did you mean `#`/`//`? `--` is not a comment in
  Trilogy" detector (like the existing detect_* syntax helpers) would close it.

## Verdict

Prior "RowsetNode crash" handoffs are indeed stale — that sentinel is gone and the
`max(...) by *` membership + intersect-of-rowsets **build and execute fine**. q23 today
is a **non-bug for the planner**: a replay-cost token total on top of an
agent-logic-wrong (but cleanly running) query, with one latent `--`-isn't-a-comment
footgun that happened to be harmless this time.
