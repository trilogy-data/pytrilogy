# Evals

On-demand evaluations for the Trilogy agent harness. **These are not part of the
normal test suite** — they depend on a live LLM (network + API key + cost) and
are run manually when iterating on the agent loop. The full iteration loop and
accumulated lessons live in `evals/EVAL_LOOP_INSTRUCTIONS.md`.

## tpcds_agent

Drives the agent end-to-end against a real DuckDB/TPC-DS task and measures how
well it does, across four levels of scaffolding.

What it does:

1. Builds (and caches) a DuckDB database loaded with TPC-DS at a given scale
   factor via the duckdb `tpcds` extension.
2. Spins up an isolated workspace with a `trilogy.toml` pointing the agent at a
   private copy of that database.
3. Sets the workspace up for the chosen **category** (see below) and runs the
   agent once per business question (`query_prompts.json`), each with fresh
   context, to write `queryNN.sql` (SQL legs) or `queryNN.preql` (Trilogy legs).
4. Scores each generated query: it is executed and compared (order-independent)
   against the TPC-DS reference (`tests/modeling/tpc_ds_duckdb/queryNN.sql`, or
   `PRAGMA tpcds(n)` as a fallback).

### The four categories

The same business question is asked four ways, in increasing order of
scaffolding (the funnel report reads them in this order to show marginal lift):

| Category | Scaffolding given to the agent | Toolset |
|---|---|---|
| `sql_bare`   | a DuckDB database only; agent discovers the schema | plain SQL |
| `sql_schema` | + a generated `schema.md` table/column map | plain SQL |
| `ingest`     | an auto-ingested Trilogy model (`trilogy ingest --all`) | trilogy |
| `enriched`   | the hand-curated Trilogy model | trilogy |

`sql_bare`/`sql_schema` are the no-Trilogy baselines; `ingest`/`enriched` use the
Trilogy CLI.

### Running

```bash
# Single category (defaults to enriched if --enriched-model-dir is set, else ingest):
python evals/tpcds_agent/run_eval.py --category sql_schema

# All four in parallel, then render the cross-category funnel + matrix:
python evals/tpcds_agent/run_eval.py \
  --categories sql_bare,sql_schema,ingest,enriched --concurrency 2

# Legacy two-way (alias for --categories ingest,enriched):
python evals/tpcds_agent/run_eval.py --both-modes
```

Requires `DEEPSEEK_API_KEY` (read from the repo `.env.secrets` by default, or
the environment). Useful flags:

| Flag | Default | Purpose |
|---|---|---|
| `--model` | `deepseek-chat` | LLM model id |
| `--provider` | `deepseek` | LLM provider (`deepseek`, `openrouter`, `anthropic`, `openai`, `google`) |
| `--scale-factor` | `1` | TPC-DS scale factor |
| `--num-queries` | `20` | how many queries to attempt |
| `--query-ids` | — | comma-separated ids to run instead (e.g. `5,13,18`); splices the rest from the latest run |
| `--max-iterations` | `75` | agent tool-loop budget per query |
| `--timeout` | `900` | agent subprocess timeout per query (seconds) |
| `--concurrency` | `1` | parallel agents (>1 forces `--monitor quiet`) |
| `--env-file` | `.env.secrets` | file providing the API key |
| `--monitor` | `feed` | live monitoring mode (see below) |

### Validating a single query (10x)

A single `run_eval` result is noisy (LLM variance). To A/B a change, repeat one
query N times in one category and compare `pass_rate`:

```bash
python evals/tpcds_agent/repeat_query.py --query-id 13 --repeats 10 \
  --scale-factor 1 --category enriched
```

### Live monitoring

The agent run is the long phase. `--monitor` controls what you see while it
runs (the agent's JSONL trace is tailed as it is written):

- `feed` *(default)* — a parsed progress feed: one line per tool call with
  iteration number, elapsed time, the call, and its `ok`/`ERROR` + duration.
- `raw` — the agent subprocess output streamed straight to the console.
- `both` — raw output plus a periodic one-line tally heartbeat.
- `quiet` — only the `[n/5]` phase markers (forced when `--concurrency > 1`).

Regardless of mode, the per-query `agent_log.qNN.jsonl` is written live, so a
second terminal can always `tail -f` it.

### Provider routing (OpenRouter)

The default provider is direct `deepseek`. With `--provider openrouter`, the
runner exports `OPENROUTER_PROVIDER` (consumed by trilogy's OpenRouter provider)
— by default `{"ignore": ["AtlasCloud"], "allow_fallbacks": true}` — to block a
known-bad route that hard-`400`s tool requests and let OpenRouter pick from the
rest. Set `OPENROUTER_PROVIDER` yourself (env or `.env.secrets`) to override it;
see the [provider-routing docs](https://openrouter.ai/docs/features/provider-routing).

### Output

Each leg writes to `evals/tpcds_agent/results/<timestamp>[_<category>]/`:

- `report.md` / `report.json` — metrics and per-query results
- `agent_log.qNN.jsonl` — full LLM + tool-call trace per query
- `agent_output.txt` — agent process stdout/stderr
- `task.qNN.txt` — the exact task prompt per query
- `workspace/` — the agent's working dir (its `.sql`/`.preql` files + DB copy)

Cross-category charts land in `evals/tpcds_agent/charts/`:
`dashboard_<category>.png` (per leg), `funnel.{png,md}` (rendered when ≥2 legs
ran), and `trilogy_failures_<category>.md` (per-leg failure detail).

### Metrics

- **Query pass rate** — generated queries matching the reference, with per-query
  status: `pass` / `fail` / `error` / `missing`, plus `timeout` / `exhausted` /
  `crash` for agent-loop failures.
- **Total time** — agent wall-clock duration (and sum-of-per-query at
  concurrency > 1).
- **Tool-call metrics** — total calls, breakdown by tool, `trilogy` subcommand
  breakdown, and tool success rate (non-error results / total results).
- **LLM usage** — iteration count and token totals.

### Caveats

- Scoring compares result sets order-independently — `ORDER BY` correctness is
  not graded, only whether the right data was computed.
- Default scale factor is `1`; smaller factors leave many queries with empty
  result sets, which agents spin on (re-exploring instead of accepting a valid
  0-row answer), and some filter literals won't match. Override `--scale-factor`
  only for quick local runs.
- `results/` and `.cache/` are git-ignored.
