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
read from repo `.env.secrets`). Defaults: `--scale-factor 1`, `--num-queries 99`
(the full TPC-DS set — pass `--num-queries`/`--query-ids` to scope a quick run).

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
Is the agent thrashing on tool use - truncated results, too many results, unclear what to use, args, tool failure?

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
- **High token churn IS a framework-bug detector — trust it, don't explain it away.**
  Empirically, EVERY question that runs past ~500k tokens has been an actual framework issue
  driving the agent's confusion (bad codegen, `INVALID_REFERENCE_BUG` sentinels, lost/fanned-out
  grain, a construct the engine can't resolve), NOT a "hard question," "agent difficulty," or
  "deepseek variance." When a query is a token sink, the prior is: there is a framework bug —
  go find it (read its generated SQL for sentinels/Binder errors, minimize a repro, file it).
  Do not re-litigate this or attribute the churn to model noise.
- **A single normal-harness pass over ~10 queries is useful signal — in aggregate.** One pass
  gives weak signal for any ONE query, but the SET of high-churn / failing queries is reliable.
  Use a single pass to rank the token sinks and pick framework-bug targets; you do NOT need
  repeated runs before acting on them.
- **The "noisy, never conclude from one run" caveat is about per-query PASS/FAIL deltas — not
  about churn-as-detector.** When you need a real before/after PASS-RATE for a specific fix,
  validate with `repeat_query.py --query-id N --repeats 10 --scale-factor 1` and compare
  `pass_rate`.
- **A big token SWING is signal, NOT variance — never dismiss it, especially over 500k.** The
  only "noise" band is queries that stay LOW in both runs (small absolute counts jittering).
  Any of the following is a framework-bug lead to investigate, not "deepseek variance":
  (a) a query that swings INTO >500k, (b) a large swing (say >50%) on a query already >500k,
  (c) a query that jumps by hundreds of k tokens. A 500%/2x swing on a >500k query is the
  opposite of noise — it is the detector firing. If a swing lines up with a change you just
  landed (model edit, engine change), treat it as a probable REGRESSION and reproduce it
  deterministically (grep the run's `agent_log` for error signatures — `Unexpected error`,
  `Binder`, sentinels — that are absent in the prior run's log). Do NOT attribute large
  high-token swings to model non-determinism.
- **A `BinderException` / `Catalog Error` / any unhandled "Unexpected error" from
  Trilogy-GENERATED SQL is ALWAYS a framework bug** (the engine emitted invalid or
  self-inconsistent SQL), even when the agent's own query was awkward. The agent writing
  unusual Trilogy is not an excuse — the framework must either compile it or reject it with a
  clear authored-error, never emit SQL the database rejects. Only a clear *parse/authoring*
  error with an actionable message (e.g. "move to HAVING", "join or merge") is non-framework.
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

### Probing a token sink (reusable investigation-prompt — this works, use it)
Fan out ONE read-only subagent per >500k query. The framing matters: tell it the bug exists
and its job is to FIND it, not to decide whether it exists. Template:

> Find the FRAMEWORK bug behind a TPC-DS token sink. READ-ONLY (no edits under `trilogy/` or
> `tests/`; a `.md` under `evals/tpcds_agent/` is fine). `generate_sql`/CLI on small snippets
> only; no full eval.
> OPERATING ASSUMPTION (proven repeatedly): a query that burns >500k tokens has a FRAMEWORK
> bug driving the agent's confusion — a construct the engine wrongly rejects, mis-resolves, or
> mis-codegens (often SILENT: wrong rows or a hang, no error). FIND and REPRODUCE it; do NOT
> conclude "agent error." Only fall back to "no framework obstacle" with concrete proof every
> failing attempt was the agent's own clearly-erroneous syntax with a correct, clear error.
> TARGET: q<NN> burned <X> tokens (run `<run dir>`). Real errors seen: `<paste signatures>`.
> <one-line business description of the query>.
> TASK: (1) read `<run>/agent_log.q<NN>.{jsonl,conversation.txt}` for the EXACT failing
> constructs + `<run>/workspace/query<NN>.preql`; (2) read canonical
> `tests/modeling/tpc_ds_duckdb/query<NN>.{sql,preql}`, confirm it builds on the current
> engine; (3) reproduce each framework-looking error AND each silent wrong-result via
> `generate_sql`/execute against `<run>/workspace`, then MINIMIZE to the smallest snippet +
> build a trigger matrix (what makes it pass vs fail); (4) classify real-bug vs
> guidance-defect vs (rarely, with proof) agent; (5) root-cause with file:line.
> DELIVERABLE: `evals/tpcds_agent/bug_q<NN>_<slug>.md` with symptom, minimal repro, trigger
> matrix, root cause + file:line. Do NOT fix. Return an 8-line summary.

Two non-obvious tools the probes lean on: the `generate_sql`/`score_query` engine harness
above (reproduce + diff without running the agent), and a **trigger matrix** — toggle one
ingredient at a time to find the minimal failing combination (e.g. q75 needed
filter+window+select on the *same* nested-rowset column; any one removed → clean). Silent
bugs (wrong rows / timeout, no sentinel) are the dangerous class — the token bar is their only
detector, which is why the >500k prior is load-bearing. Collect findings into a single
`INDEX_pre_merge_framework_fixes.md` (open vs fixed, silent vs loud, fix locus per bug).

### Question-fix philosophy
- Phrase as business intent; never leak implementation (no "is not null", no "inner join",
  no SQL). For nulls use "where we have data" / "where X is recorded".
- Specify the output grain when the reference groups finer than it reports (e.g. q91 groups by
  marital/education but only shows call-center columns → say so) and the exact identifier
  (id vs name vs full_name).


