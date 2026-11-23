# Handoff: general agent optimizations (token use + success rate)

Goal: make the Trilogy agent (`trilogy agent`, toolset `trilogy`) cheaper and
more reliable **in general**. TPC-DS is the measuring stick, not the target.

## Ground rules

- **No benchmark-specific fixes.** A change must be a tool, output-shape,
  error-message, doc or harness improvement that would help on any model.
  No per-question hints, no model edits keyed to a question. Confirm a win on
  a second suite (`tpch_agent`, `acme_insurance_agent` or `thelook_agent`)
  before calling it general.
- **Do not point the system prompt at `agent-info query`.** Finding the
  language reference is part of what the eval tests; the bare `agent-info`
  round trip stays.
- Method, question-fix philosophy and the framework-bug prior (>500k tokens on
  a query = framework bug) are in `evals/EVAL_LOOP_INSTRUCTIONS.md`. Read it.

## Baseline (DeepSeek v4-flash, sf=1, 99 questions, unspliced)

Runs `evals/tpcds_agent/results/20261122-145208_<leg>_deepseek_deepseek-v4-flash`
(local, gitignored).

| leg | pass | total tok | median tok/q | median turns | context tax |
|---|---|---|---|---|---|
| sql_schema | 95 | 8.0M | 60k | 6 | 2.6M |
| sql_bare | 95 | 10.3M | 74k | 8 | 2.9M |
| ingest | 92 | 23.5M | 170k | 9 | 11.3M |
| enriched | 96 | 23.4M | 168k | 10 | 11.6M |

The Trilogy legs pass as often as SQL at ~3x the tokens. The gap is turn count
x a large re-sent context, not authoring errors.

## Measuring

- `python evals/common/context_tax.py <run_dir> [--status pass|fail]`:
  tool-result size x the later LLM calls that re-send it, per tool category,
  plus a tally of non-zero tool exits by message.
- `report.json` -> `per_query_metrics[]`: `total_tokens`, `prompt_tokens`,
  `cached_prompt_tokens`, `completion_tokens`, `iterations`, `tool_errors`.
- A/B a change on a subset: `run_eval.py --categories ingest,enriched
  --query-ids <ids> --splice-from none`. Pass rate per question:
  `repeat_query.py --query-id N --repeats 10 --scale-factor 1`.
- Subset runs overwrite `evals/tpcds_agent/charts/*`; copy the charts aside
  first and put them back, or don't commit them.

### Running evals here

- Concurrency **1 per leg** (the user's preference; ignore the `--concurrency 2`
  suggestion in EVAL_LOOP_INSTRUCTIONS). A full four-leg run takes ~3h.
- Launch detached from your own process tree so a memory reap of your
  session doesn't kill it, and check progress from the log/result dirs.
- The provider key must be a real key in the process environment. If every
  query ends `score=crashed` within ~4s, the key didn't resolve: stop and ask
  the user how to launch, don't retry.
- Look at the first few `done in ... score=` lines before walking away.

## Levers, ranked by measured cost

1. **`agent-info query` is 37-39% of Trilogy context tax** (19.1k chars, read
   once per run around turn 2, then re-sent every turn). Find which sections
   agents actually use (grep later queries/probes for the constructs each
   section teaches) and move the rest behind `agent-info syntax <topic>`
   drilldowns, which already exist. Measure tokens and pass rate together:
   docs that disappear can cost passes.
2. **Stale tool results stay in history.** Every early result is re-sent on
   every later turn. A harness-level policy in `trilogy/scripts/agent.py`
   (stub a result once it's superseded, e.g. a full `explore` after a narrower
   one) would help every task. Watch prompt caching: editing mid-history
   breaks the cached prefix, so compare `cached_prompt_tokens` and a
   cache-adjusted cost, not raw tokens.
3. **`file list` is 26% of ingest tax** (13.8k chars average vs 4.4k on
   enriched; ingest's `raw/` is larger). A compact default (names, maybe one
   line of description, details behind a flag) and a cap. It also still lists
   paths outside the workspace (`file list ..`); file reads and DuckDB are
   sandboxed, listing is not.
4. **`explore`**: full explore averages 7.7k and is usually followed by a
   narrowed `--ns/--regex` call (6.1k). Consider a summary default (root
   namespace, counts per imported namespace) that points at `--ns`.
5. **Re-verification after a clean answer**: Trilogy agents spend ~3 turns
   after their answer first runs clean; SQL agents spend 1. Look at what
   `file write --run` returns on success (does it make "done" obvious?)
   before trying a prompt line. A/B either.
6. **Error-message actionability.** Most frequent kinds in the rerun:
   undefined concept (check it suggests near matches), `Output column 'x'
   renames 'local.x'` (`select x as x` could be a no-op), "Nothing was
   executed: parsed N definition statements" (definition-only probes),
   "refused to write: not syntactically valid", and parse errors
   `syntax[231]`/`[101]`/`[103]`/`[202]`. For each kind, count the turns
   until the agent recovers; fix the slowest-recovering messages first.
7. **Grammar gaps seen in probes**: no null-safe comparison spelling
   (`IS NOT DISTINCT FROM`); needs a pest change and rebuild.

## Framework-bug leads from the rerun (send through the bug process)

Per EVAL_LOOP_INSTRUCTIONS these are framework bugs until proven otherwise.
They waste turns in every task that hits them.

- q70: DuckDB binder error from generated SQL (ingest and enriched).
- q81: "Could not render the query: Missing source reference to cs.return_cust..." (enriched).
- q38: "Planner emitted a keyless join" on a union probe (enriched).
- q82: "Could not resolve connections" (ingest and enriched).
- q8, q23, q76: "Discovery error: cannot merge all concepts" on probes (ingest, enriched).

## Open decisions (ask the user)

- Rank over a named rollup whose keys omit the grouping level: a window ranks
  the distinct tuples of its keys, so a subtotal row and a data-NULL leaf
  with equal visible keys collapse. Error, or a run warning like
  `window_filter_needs_having`?
