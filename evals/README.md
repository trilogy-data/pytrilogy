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

### Viewer UI

```bash
python evals/trajectory_viewer.py --serve 8080     # http://127.0.0.1:8080/viewer.html
```

Three screens, all suites in one server:

- **Runs** — the trajectory of every question in a run: tool calls, token cost,
  canonical-vs-agent query compare, plus Replay (one question) and Rerun-all
  (fork the run and redo it).
- **All evals summary** — latest pass rate per suite and variant, with trend.
- **Launch runs** — build a `run_eval.py` invocation (eval, categories,
  questions, model, scale factor, concurrency) and start it. The exact command
  is shown before you launch it; the run streams its output into the page and
  links its result dirs once they have trajectories. One run executes at a
  time, so several combinations can be queued in one sitting.

Without `--serve` the page is a static snapshot of one run dir (no pickers,
replay, or launching).

### Viewer UI

```bash
python evals/trajectory_viewer.py --serve 8080     # http://127.0.0.1:8080/viewer.html
```

Three pages behind the left ribbon, all suites in one server:

- **Evals** - latest pass rate per suite and variant, with trend. Click a run to
  debug it.
- **Launch** - build a `run_eval.py` invocation (eval, categories, questions,
  model, scale factor, concurrency) and start it. The exact command is shown
  before you launch it; the run streams its output into the page and links its
  result dirs. One run executes at a time, so several combinations can be
  queued in one sitting.
- **Debug** - a question-by-run grid: one row per run (newest first), one cell
  per question, plus a pass-rate strip, so a red column is a problem question
  and a red row is a bad run. Filter by category/model, cap the rows, or show
  only questions that fail somewhere. Click a cell for that trajectory, a run
  name to open the run. The drilldown has the question list, the trajectory
  with per-tool token cost, the canonical-vs-agent query compare, and
  Replay / Rerun-all / Archive.

Reads are lazy and cached per request: the grid reads `report.json` only
(~30ms for 94 runs), a question's log is parsed when you open it (~40ms), and
the SQL render happens only when you open the compare panel (~0.2s, plus a
one-off engine boot). Anything slower than that reports progress rather than
blocking on a blank page.

### History outlives the run dirs

Run dirs are enormous (TPC-DS results here run to hundreds of GB) and get
reclaimed; `evals/eval_history.db` is a few KB per run and doesn't. Building the
grid syncs every changed run dir into it, so **the grid spans both what is on
disk and everything ever archived** - a cleaned-up run keeps its row, its
per-question results, and the query the agent wrote. What cleanup does take away
is the turn-by-turn trajectory.

- Archived-only rows are italic/dimmed in the grid; "on disk only" filters them
  out, and the drilldown says plainly what was reclaimed.
- Every run publishes at the end (`run_eval.py`), and the viewer picks up
  anything else it finds. Runs whose numbers are curated (a `--query-ids` rerun
  that spliced older results in, or a run with offline replays) are stored with
  `curated = 1`: they show in the grid but stay out of the summary's trend.
- It is sqlite, not duckdb, because an eval run writes to it while the viewer
  holds it open and duckdb takes a single-writer lock on its file. To query it
  analytically: `INSTALL sqlite; ATTACH 'evals/eval_history.db' AS h (TYPE sqlite);`
### Reclaiming disk

Run dirs are ~99.9% regenerable byproduct. A measured TPC-DS tree: 435 GB of
DuckDB temp spill, 151 GB of per-run database copies, and **0.7 GB of agent
logs** - the only part that is evidence. So sweep the byproduct first:

```bash
python evals/clean_results.py --spill                          # dead temp files, loses nothing
python evals/clean_results.py --db-copies --older-than 7       # re-copied from .cache next run
python evals/clean_results.py --runs --older-than 2            # archive, then delete whole run dirs
```

Runs now clean up their own spill as they go: each worker is purged the moment
its agent exits, the run purges again at the end, and an `atexit` hook covers a
crash or Ctrl-C. So `--spill` is mostly there for what a hard kill leaves behind
(nothing runs on `taskkill`) and for runs from before this existed.

Every mode dry-runs until you pass `--yes`, skips anything touched in the last
`--skip-recent` hours (6 by default, so a run in flight is never disturbed), and
covers all suites unless you pass `--eval`. `--db-copies` costs in-place Replay
for the runs it touches; `--runs` costs the trajectories (results and the
agent's final query survive in the history db). `tpcds_agent/clean_runs.py` is a
deprecated shim that forwards to `--runs`.

Without `--serve` the run is baked into `viewer.html` so the file reads offline
on its own (tens of seconds to write, since every query is transpiled up front);
there is no grid, summary or launching in that mode.

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

### Mining a run for framework bugs

`error_scan.py` turns a run's per-query `agent_log.qNN.jsonl` traces into one
markdown report: for every failing `trilogy` call it captures the **trigger**,
the **error message**, the **query that produced it**, and the agent's **next
thought** — so you can quickly separate real framework bugs from agent thrash on
well-messaged errors.

```bash
# Latest run, all queries -> <run>/error_scan.md
python evals/tpcds_agent/error_scan.py

# Specific run + a subset of queries to a custom path
python evals/tpcds_agent/error_scan.py --run results/20260626-125555 \
  --query-ids 5,11,14 --out scan.md
```

Defaults to the most recent run dir; `--max-query-chars` / `--max-thought-chars`
bound the per-error excerpts. It handles both file-write forms (`--content` and
stdin) and inline `run --import … <query>` calls when recovering the producing
query.

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
