## Context TPC-DS/H

Goal: improve our CLI + tooling for general use cases by evaluating on curated datasets.

Our eval harness uses a deepseek agent to answer questions.

### The four eval cases

The same business question is asked four ways, in increasing order of scaffolding
(defined in `evals/common/categories.py`; the funnel report reads them in this order
to show the marginal lift each layer adds):

| Category | What the agent gets | Toolset | Candidate file |
|---|---|---|---|
| `sql_bare`   | a DuckDB database only; discovers the schema itself | plain SQL | `queryNN.sql` |
| `sql_schema` | same, **plus** a generated `schema.md` table/column map | plain SQL | `queryNN.sql` |
| `ingest`     | an auto-ingested Trilogy model (`trilogy ingest --all` → `raw/*.preql`) | trilogy | `queryNN.preql` |
| `enriched`   | the hand-curated Trilogy model (`tests/modeling/tpc_ds_duckdb`) | trilogy | `queryNN.preql` |

`sql_bare`/`sql_schema` are the **no-Trilogy baselines** (added so the funnel shows
what Trilogy buys over raw SQL). `ingest`/`enriched` are the Trilogy legs. When
diagnosing, remember the SQL legs write `queryNN.sql`, not `.preql`.

### Running the eval cases

Needs `DEEPSEEK_API_KEY` (provider/model default to `deepseek`/`deepseek-chat`,
read from repo `.env.secrets`). Defaults: `--scale-factor 1`, `--num-queries 20`.

```bash
# All four legs in parallel, then render the cross-category funnel + matrix.
# Each leg writes results/<ts>_<category>/. Pass --concurrency 2 (≈8 concurrent
# across 4 legs); the default (1) is auto-split to 1/leg, and 3/leg = 12 is too
# much DeepSeek pressure.
python evals/tpcds_agent/run_eval.py \
  --categories sql_bare,sql_schema,ingest,enriched --concurrency 2

# One leg only:
python evals/tpcds_agent/run_eval.py --category sql_schema --num-queries 10

# Legacy two-way (alias for --categories ingest,enriched):
python evals/tpcds_agent/run_eval.py --both-modes

# Specific queries (overrides --num-queries; splices the rest from the latest run):
python evals/tpcds_agent/run_eval.py --category enriched --query-ids 5,13,18
```

**Outputs** (under `evals/tpcds_agent/`):
- `results/<ts>_<category>/` per leg — `report.{md,json}`, `agent_log.qNN.jsonl`,
  `task.qNN.txt`, `workspace/` (the agent's `.sql`/`.preql` files + DB copy).
- `charts/dashboard_<category>.png` — per-leg dashboard.
- `charts/funnel.{png,md}` — cross-category lift (only when ≥2 legs ran).
- `charts/trilogy_failures_<category>.md` — per-leg failure detail.

### Validating a candidate change (10x harness)

`repeat_query.py` repeats ONE query N times in ONE category (default `enriched`).
Pass `--category` to validate a fix in the SQL legs too:

```bash
python evals/tpcds_agent/repeat_query.py \
  --query-id 13 --repeats 10 --scale-factor 1 --category sql_schema
```

Loop:
Run a target portion of the test set across the relevant categories. Typically pick ~10 questions.

Identify the following, in order of priority:

### Bugs
Unexpected frameowrk level errors or bugs (generally, unhandled exceptions) -> Bug report MD to hand off to another agent

### Agent Prompt Issues
Does answering the question require a language feature we have not exposed?

### Agent Tool Issues
Is the agent thrashing on tool use - truncated results, too many results, unclear what to use, args, tool failuer?

### Bad Questions
Not all the questions fully specify criteria or accurately map to the output or are phrased generically. Review 
them so they seem to generic business questions, not "how to write SQL" but contain all required info for a correct
answer

### Model Problems
Particularly for our enriched model, is there a comment/guidance/precalculation we could add to help an agent answer this question?


## Data Collection
When we have identified a candidate, it will typically be a single question. To measure improvement, run the same question repeatedly
using our 10x harness to validate. 

Overall goals:
- Success rate
- Token minimization


## Trilogy lessons (append as you learn)

These were learned against the **Trilogy legs** (`ingest`/`enriched`) on TPC-DS.
Scope when re-applying: **Method** and **Eval-harness fixes** are category-agnostic
(they're about diagnosing and scoring any leg); the **question-wording** fixes also
help the SQL legs (a clearer business question helps any agent); the **language
idioms** (grain inheritance, `date_diff` arg order, anti-joins, `is null` semantics)
are Trilogy-specific and do not apply to `sql_bare`/`sql_schema`.

### Method
- **Single-shot `run_eval` is NOISY. Never conclude from one run.** Per-query pass/fail
  flips across identical re-runs (deepseek variance). Validate every before/after with
  `repeat_query.py --query-id N --repeats 10 --scale-factor 1` and compare `pass_rate`.
- **Diagnose from artifacts, not theory.** Read the agent's generated query
  (`results/<run>/workspace/queryNN.preql`, or `.sql` for the `sql_bare`/`sql_schema`
  legs), its run output, and the reference
  (`tests/modeling/tpc_ds_duckdb/queryNN.sql` AND `.preql` — the `.preql` is the
  hand-authored canonical Trilogy answer; the scorer prefers `.sql`). I misdiagnosed q96
  as a count bug off one trace; the passing vs failing reps revealed it was the demographic
  path. Compare a passing rep to a failing rep.
- Fan out diagnosis with subagents, one per query, each reading that query's
  `agent_log.qNN.conversation.txt` + generated preql + reference.
- `repeat_query.py` defaults to `--scale-factor 0.1`; pass `--scale-factor 1` for queries
  whose filter literals (store/company/item names) only exist at full scale.
- Never edit source/model/`.preql` files while a repeat run is in flight — in-flight worker
  subprocesses import the half-edited tree and crash.
- **To pin down WHY a result differs, score candidate forms directly** instead of running the
  agent. Build the engine once and diff rows:
  ```python
  import sys; sys.path.insert(0,'evals'); from common import scoring
  ws=Path('.../<run>/workspace'); eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
  rows = lambda body: list(eng.execute_raw_sql(eng.generate_sql(body)[-1]).fetchall())
  ```
  Then `scoring.score_query(eng, ws, NN, 'tpcds', custom_refs_dir=Path('tests/modeling/tpc_ds_duckdb'))`.




### Question-fix philosophy
- Phrase as business intent; never leak implementation (no "is not null", no "inner join",
  no SQL). For nulls use "where we have data" / "where X is recorded".
- Specify the output grain when the reference groups finer than it reports (e.g. q91 groups by
  marital/education but only shows call-center columns → say so) and the exact identifier
  (id vs name vs full_name).


