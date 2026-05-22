# Evals

On-demand evaluations for the Trilogy agent harness. **These are not part of the
normal test suite** — they depend on a live LLM (network + API key + cost) and
are run manually when iterating on the agent loop.

## tpcds_agent

Drives the `trilogy agent` CLI end-to-end against a real database task and
measures how well it does. This is the baseline for harness-loop improvements.

What it does:

1. Builds (and caches) a DuckDB database loaded with TPC-DS at a small scale
   factor via the duckdb `tpcds` extension.
2. Spins up an isolated workspace with a `trilogy.toml` pointing the agent at a
   private copy of that database.
3. Runs `trilogy agent` with a **minimally-steered** task: the 10 TPC-DS
   business questions (`query_prompts.json`) and the instruction to build a
   Trilogy model and write `query01.preql` … `query10.preql`. The agent must
   discover the CLI workflow itself.
4. Scores the run: each generated query is executed and its result compared
   (order-independent) against the TPC-DS reference `PRAGMA tpcds(n)`.

### Running

```bash
python evals/tpcds_agent/run_eval.py
```

Requires `OPENROUTER_API_KEY` (read from the repo `.env.secrets` by default, or
the environment). Useful flags:

| Flag | Default | Purpose |
|---|---|---|
| `--model` | `deepseek/deepseek-v4-flash` | LLM model id |
| `--provider` | `openrouter` | LLM provider |
| `--scale-factor` | `0.01` | TPC-DS scale factor |
| `--num-queries` | `10` | how many queries to attempt |
| `--max-iterations` | `120` | agent tool-loop budget |
| `--timeout` | `2400` | agent subprocess timeout (seconds) |
| `--env-file` | `.env.secrets` | file providing the API key |
| `--monitor` | `feed` | live monitoring mode (see below) |

### Live monitoring

The agent run is the long phase. `--monitor` controls what you see while it
runs (the agent's JSONL trace is tailed as it is written):

- `feed` *(default)* — a parsed progress feed: one line per tool call with
  iteration number, elapsed time, the call, and its `ok`/`ERROR` + duration.
- `raw` — the agent subprocess output streamed straight to the console.
- `both` — raw output plus a periodic one-line tally heartbeat.
- `quiet` — only the four `[n/4]` phase markers.

Regardless of mode, `agent_log.jsonl` is written live, so a second terminal
can always `tail -f` it.

### OpenRouter provider routing

OpenRouter multiplexes a model across several upstream providers, and some of
them reject otherwise-valid tool requests (a hard `400`, which is not retried).
The runner therefore pins routing for OpenRouter runs by exporting
`OPENROUTER_PROVIDER` (consumed by trilogy's OpenRouter provider) — by default
`{"order": ["DeepInfra"], "ignore": ["AtlasCloud"], "allow_fallbacks": true}`.
Set `OPENROUTER_PROVIDER` yourself (env or `.env.secrets`) to override it; see
the [provider-routing docs](https://openrouter.ai/docs/features/provider-routing).

### Output

Each run writes to `evals/tpcds_agent/results/<timestamp>/`:

- `report.md` / `report.json` — metrics and per-query results
- `agent_log.jsonl` — full LLM + tool-call trace (the agent's `--log-file`)
- `agent_output.txt` — agent process stdout/stderr
- `task.txt` — the exact task prompt
- `workspace/` — the agent's working dir (its `.preql` files + DB copy)

### Metrics

- **Query pass rate** — generated queries matching the TPC-DS reference, with
  per-query status: `pass` / `fail` / `error` / `missing`.
- **Total time** — agent wall-clock duration.
- **Tool-call metrics** — total calls, breakdown by tool, `trilogy` subcommand
  breakdown, and tool success rate (non-error results / total results).
- **LLM usage** — iteration count and token totals.

### Caveats

- Scoring compares result sets order-independently — `ORDER BY` correctness is
  not graded, only whether the right data was computed.
- At `sf=0.01` some queries return few or zero rows, so a `pass` with `ref_rows`
  near zero is weak signal — check the row counts in the report.
- `results/` and `.cache/` are git-ignored.
