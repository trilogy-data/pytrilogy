# Natural-language queries + LLM validation — `select natural` / `validate ... matches` design

Status: IMPLEMENTED (2026-07-24; see "Implementation deltas" at the end for
where the build deviated from the sketch)

## Goal

Two composable language features:

1. **`select natural '<question>'`** — a first-class, executable natural-language
   query: a one-shot agent loop turns the question into a Trilogy query against the
   current model, executes it, and returns rows like any select.
2. **`validate [name] select natural '<question>' matches ( select ... )`** — a new
   branch of the existing `validate` statement that runs the natural select N times
   and compares its rows against an authored expected query. This is how a model repo
   (e.g. `trilogy-public-models`) embeds LLM eval questions.

Validation is just another *test type*: it runs under the existing `trilogy unit` /
`trilogy integration` commands, not a new CLI surface — `unit` runs the agent against
**mocked datasources** (no warehouse hit), `integration` runs it live. Both commands
grow a generic test-type selection flag so the `agent` type (and others) can be
skipped or included per run.

This deliberately differs from the TPC-DS/H eval harness (`evals/`):

| | eval harness | `validate ... matches` |
|---|---|---|
| Reference | SQL against raw tables (`PRAGMA tpcds(n)` / `.sql`) | An **authored Trilogy query** in the same model |
| Comparison | trilogy → SQL (catches framework bugs) | trilogy → trilogy (framework bugs cancel out) |
| Measures | engine correctness + agent + model | **model curation + agent guidance quality only** |
| Runs in | dedicated harness scripts | any model repo, via `unit`/`integration` |

Both the agent's candidate and the expected query run through the same engine on the
same model, so an engine defect affects both sides equally. What's left is the signal
we want when curating a public model: "can an agent, given this model's concepts,
comments, and guidance, answer this business question?"

Not run in CI (cost): the `agent` test type is excluded by default and included
explicitly per run; CI's normal `unit`/`integration` runs still compile-check the
expected queries for free so they don't rot.

## Statement syntax

### Standalone natural select

```trilogy
select natural 'Which aircraft manufacturer has the most registered aircraft?';
```

Executes live wherever selects execute (`trilogy run`, serve): agent loop → generated
query → rows. Authoring the statement is the cost opt-in; it fails loudly when no
agent provider resolves from `trilogy.toml [agent]`. `unit`/`integration` do not
execute selects, so a stray natural select in a tested file costs nothing there.

Lexing note (lark/LALR backend): `natural` stays a valid concept name everywhere. The
keyword is a guarded terminal that only fires when followed by a string literal —
`/natural(?=\s*['"])/i` — same trick as the legacy-window keyword guard, so
`select natural, x` still parses `natural` as an identifier. The pest backend's
ordered-choice backtracking needs no guard.

### Validation branch

```trilogy
validate manufacturer_most_registered
select natural 'Which aircraft manufacturer has the most registered aircraft?'
matches (
    select
        model.manufacturer,
        count(aircraft.id) -> registered_count
    order by registered_count desc
    limit 1
) with (repetitions = 3, target = 0.67, comparison = tolerant);
```

Pieces, in order:

1. `validate` — an internal branch of the **existing** `validate_statement` rule
   (`validate all | concepts | datasources ... | <this>`). One grammar rule keeps
   top-level statement semantics clear, and `show validate <anything>` works
   uniformly because `show_statement` already wraps `validate_statement`.
2. Optional `IDENTIFIER` label — used in reports. Defaults to `<file_stem>_<index>`
   when omitted.
3. An embedded `select natural '<question>'` — the thing under test. Question phrased
   as business intent (eval-harness philosophy: no implementation leakage, fully
   specified grain/identifiers).
4. `matches ( select ... )` — the authored expected answer. Paren-wrapped select is an
   established grammar shape (`scalar_subquery`, `tvf_rel_arg`), which sidesteps
   nested-terminator ambiguity. v1: exactly one select.
5. Optional `with ( key = value, ... )` config clause. Statement-initial `with`
   (rowset derivation) is unambiguous with this trailing clause.

Because there are no LLM-specific CLI arguments, the statement carries **all**
pass/fail semantics:

| key | type | default | meaning |
|---|---|---|---|
| `repetitions` | int | 1 | independent agent attempts |
| `target` | float | 1.0 | min pass rate over repetitions to count as PASS |
| `comparison` | enum | `tolerant` | `tolerant` \| `exact` \| `ordered` |
| `tags` | list[string] | `[]` | reporting/grouping (`smoke`, `hard`, ...) |
| `timeout` | int (s) | 600 | per-repetition agent wall clock |

Operational config (provider, model, API key env) is *not* in the statement either —
it resolves from the model repo's `trilogy.toml` `[agent]` section exactly as
`trilogy agent` does. The statement says what a pass means; the environment says how
to run it.

Comparison modes:

- `tolerant` (default): unordered multiset of rows, positional columns, exact
  non-numeric cells, `isclose` fractional cells — a direct port of the eval
  harness's `_results_equal` (proven on 99 TPC-DS queries).
- `exact`: unordered, all cells strictly equal.
- `ordered`: `tolerant` cell semantics but row order enforced — use when the
  question specifies ordering and the expected query has `order by`.

Columns are compared positionally: the agent's output names will never match the
authored ones, and positional matching is what the eval harness already relies on.

## AST + parsing

Two dataclasses in `trilogy/core/statements/author.py`:

```python
@dataclass
class NaturalSelectStatement:
    question: str

@dataclass
class ValidateNaturalStatement:
    query: NaturalSelectStatement
    expected: SelectStatement
    name: str | None = None
    repetitions: int = 1
    target: float = 1.0
    comparison: QueryComparison = QueryComparison.TOLERANT
    tags: list[str] = field(default_factory=list)
    timeout: int | None = None
```

`ValidateStatement` (scope validation) is untouched; the merged grammar rule's
hydrator dispatches on node shape: a nested `natural_select_statement` child →
`ValidateNaturalStatement`, else the existing `ValidateStatement`.

Wiring checklist (both parser backends — pest is default and needs a maturin
rebuild of `trilogy/scripts/dependency`):

- `trilogy/parsing/trilogy.lark`: guarded `NATURAL_KW` terminal +
  `natural_select_statement` rule + new alternative inside `validate_statement` +
  `validate_query_option`/`validate_query_config` rules; `natural_select_statement`
  added to the statement and `show_statement` alternations.
- `trilogy/scripts/dependency/src/trilogy.pest`: same rules (no lexer guard needed).
- `trilogy/parsing/v2/syntax.py`: `SyntaxNodeKind.NATURAL_SELECT_STATEMENT`,
  `VALIDATE_QUERY_OPTION`, `VALIDATE_QUERY_CONFIG` + mappings (the validate branch
  stays under `VALIDATE_STATEMENT`).
- `trilogy/parsing/v2/rules/operational_rules.py`: hydrators; `validate_statement`
  dispatches on shape.
- `trilogy/parsing/v2/statement_plans.py` / `statement_planner.py`: plans. The
  expected select is *planned* (compiled) like any select so authoring errors surface
  at parse/build time; the validate statement produces no `ProcessedQuery` for normal
  execution. `NaturalSelectStatement` plans to an executor-dispatched statement (LLM
  at execution time, not plan time).
- `trilogy/parsing/render.py`: renderers for `fmt` round-trip.

## Execution semantics — a test type under `unit` / `integration`

| surface | what happens | cost |
|---|---|---|
| `trilogy run` / executor — `select natural` | LIVE: agent loop → query → rows (that's the feature) | $ |
| `trilogy run` / executor — `validate ... matches` | inert: parse + build expected select, no-op result row | free |
| `trilogy unit` (default types) | compile-check: expected select built + SQL generated (piggybacks on the existing `validate_environment(mock=True)` phase) | free |
| `trilogy unit` with `agent` type | validation loop against **mocked datasources in local DuckDB** — LLM tokens spent, warehouse never touched | $ (LLM only) |
| `trilogy integration` (default types) | compile-check against the live env | free-ish |
| `trilogy integration` with `agent` type | validation loop against the **real backend** — real-data pass/fail | $$ (LLM + warehouse) |
| `show validate <anything>` | scope branches: existing generated-check display; natural branch: task prompt + compiled expected SQL, no LLM call | free |
| `show select natural '...'` | task prompt only, no LLM call | free |

The two test tiers answer different questions. Unit-tier (mock data) validates
*structure*: the agent found the right concepts, grain, filters, joins — both sides
run over the same synthetic rows, so equality exercises the query shape.
Integration-tier validates against real values and is the authoritative curation
signal. Cheap loop: iterate on model guidance with `unit`, confirm with
`integration`.

### Test-type selection (no LLM-specific flags)

`unit` and `integration` both gain a generic test-type selector rather than anything
LLM-specific:

```
--skip-type [datasources|concepts|agent]   (repeatable)
--include-type [agent|...]                 (repeatable)
```

Types map onto `ValidationScope` plus the new `agent` type. Defaults: `datasources`
and `concepts` on (today's behavior), `agent` **off** — it costs money, so it must be
asked for (`--include-type agent`); CI can never trigger an LLM call by accident even
with API keys in the environment. When `agent` is included but no agent provider/key
resolves from `trilogy.toml [agent]`, the run fails loudly (opt-in features fail
loudly, never warn-and-continue). When excluded, questions are still compile-checked
(that's part of parse/build, not a test type) and reported as `skipped`.

Question selection within the agent type: the file/directory argument is the selection
mechanism. Put smoke questions in their own `validations_smoke.preql` if you want a
cheap subset; `tags` are reporting metadata only in v1.

### Mock materialization (unit tier)

`unit` already validates with mocked datasources (`validate_environment(mock=True)`);
the agent tier extends this: materialize the mock rows for every datasource into a
workspace-local DuckDB file and point the workspace `trilogy.toml` at it (unit always
uses DuckDB). The agent's own exploration queries and the scoring engine both run
against that file — nothing reaches the real backend. Mock generation must be
deterministic per run so candidate and expected see identical rows (they also share
one engine, so this holds by construction), and guarantees populated, non-null rows —
so a pass is never the degenerate "both sides empty" case and structural comparison
is always meaningful.

### Hook points in `testing.py`

`execute_script_for_unit` / `execute_script_for_integration` today do parse →
`validate_environment`. Both gain a phase: collect `ValidateNaturalStatement`s from
the parsed queries and, when the `agent` type is selected, run each through the loop
below (unit against the mock-materialized DuckDB workspace, integration against the
live backend config). `ExecutionStats` gains `agent_question_count` / `agent_passed`
/ `agent_skipped`; a failed question fails the script node like any other validation
failure (non-zero exit).

Parallelism note: `unit`/`integration` already parallelize across script nodes; agent
questions within one script run serially in v1 (provider pressure). `--parallelism`
therefore bounds concurrent agent conversations too. There is no write contention to
schedule around: question runs are read-only (selects), the unit tier's mock DB is
written once at setup then only read (agent + scoring connections open read-only),
and any setup scripts run once for the whole integration run.

### Per-question run loop

For each `validate ... matches` statement in script `F`, per repetition (the
standalone `select natural` execution path is the same loop with one repetition and
rows returned instead of compared):

1. **Workspace**: the model directory is NOT copied. The agent gets a fresh scratch
   directory to write in (fresh per repetition — agents must not see a prior
   attempt's files), seeded with the model's `trilogy.toml` (connection/relative
   paths absolutized) and with import resolution pointed at the model directory, so
   `import entrypoint;` works from scratch. The agent's cwd is the scratch dir, so
   its writes can never dirty the model repo; model files remain readable in place.
   (Working directories rarely contain DB files, so there's nothing heavy to copy;
   the unit tier's mock DuckDB is materialized into the scratch dir.)
2. **Task prompt**: template ported from the eval harness — answer the question using
   the Trilogy model in this directory, write the final query to `answer.preql`.
   The question text is inserted verbatim.
3. **Agent**: spawn `trilogy agent --toolset trilogy --log-file <rep>.jsonl` as a
   subprocess in the workspace (a slimmed port of `evals/common/agent_runner.run_agent`
   into `trilogy/scripts/validate_agent.py`). Timeout / iteration-exhaustion / crash
   classification mirrors the harness (`timeout`/`exhausted`/`crashed` statuses).
4. **Score**: one engine per question workspace; execute the candidate's final
   statement and the expected select through the *same* engine, compare rows per the
   statement's `comparison` mode. Candidate statuses: `pass | fail | error | missing |
   timeout | exhausted | crashed` (same vocabulary as eval scoring).
5. **Aggregate**: `pass_rate = passes / repetitions`; question PASSES iff
   `pass_rate >= target`. Also collect per-rep tokens/iterations from the agent JSONL
   (the light half of `parse_agent_log`) — token burn is the curation signal for
   "the model made this answerable but expensive".

### Output

Folded into the normal `unit`/`integration` result display: one row per question
(`name | reps | pass_rate | target | status | avg tokens | detail`), NDJSON events in
`--format json` mode. In addition, a `report.json` summary is written **by default**
to `.trilogy/validate_runs/<ts>/` alongside the per-rep agent JSONL logs and scratch
workspaces (trace-level debugging); a `--no-report` flag suppresses it.

## Code moves (shared with evals, single source of truth)

- Row comparison (`_results_equal`, `_comparison_row`, `_bucket_matches`) moves from
  `evals/common/scoring.py` into `trilogy/core/validation/rows.py`; evals imports it
  from trilogy so the two surfaces can't diverge. (Note: bring the logic, not the
  eval-side quirks — the exact-integer carve-out defect tracked for q66 should be
  fixed on the way in.)
- Agent subprocess runner: minimal port into `trilogy/scripts/validate_agent.py`
  (Popen + heartbeat + timeout kill + exit-code classification). Backs both the
  standalone `select natural` execution and the validation loop. The eval harness
  keeps its own richer runner (monitors, categories, splicing).
- Task prompt template: shared constant in trilogy, imported by evals where the
  Trilogy legs use the same phrasing.

The evals harness itself does not change: it remains the trilogy-vs-SQL surface for
sniffing out framework bugs.

## Model-repo convention

One `validations.preql` per model, importing the entrypoint:

```trilogy
import entrypoint;

validate busiest_airport
select natural 'Which airport had the most departing flights in 2005?'
matches (
    select flight.origin.code, count(flight.id2) -> departures
    where flight.dep_time.year = 2005
    order by departures desc limit 1
) with (repetitions = 3, target = 0.67, tags = ['smoke']);
```

- `trilogy unit trilogy_public_models/duckdb/faa` — free health check (CI-safe).
- `trilogy unit ... --include-type agent` — cheap structural curation loop (mock
  data, no warehouse).
- `trilogy integration ... --include-type agent` — authoritative live curation run.
- Because it's a plain `.preql` file, questions render/fmt, live next to the model
  they test, and version with it.

## Implementation deltas (vs the sketch above)

- **`import_paths` is now a general language/config feature** (added to close
  the original gap): `Environment.import_paths` holds fallback roots tried in
  order when an import misses under `working_path`, configured via a top-level
  `import_paths = [...]` array in `trilogy.toml` (relative entries resolve
  against the toml's directory; nested imports inherit the roots). Workspace
  tomls always carry `import_paths = [<model dir>, ...model's own roots]`, so
  the agent's `answer.preql` resolves `import entrypoint;` regardless of
  seeding.
- **Unit tier repoints + re-renders the model into a "mock image"** (the
  robust replacement for the original address-preserving mock, which had an
  expected-side gap — see below). Setup, once per script:
  1. **Materialize** deterministic mock rows for every datasource into a DuckDB
     file, one table per datasource named by `_mock_name` (a hash of the
     physical address, so it's stable). A single `MockManager` over the
     flattened env gives shared concepts (join keys) the same values across
     datasources, so **cross-datasource joins still match**.
  2. **Repoint + re-render**: each model `.preql` is parsed, every datasource
     `repoint`ed at its mock table (a new generic `Datasource.repoint(address)`),
     and re-rendered with the standard Renderer into an image directory.
     Because repointing replaces the address entirely, the original address
     *type is irrelevant* — table, file, remote (`gs://`), query, and script
     datasources all become mock tables, so the unit tier has **no
     address-shape restrictions and never touches a remote**. Concept and
     datasource descriptions round-trip through the renderer, preserving the
     guidance the eval measures.
  3. The image's toml points DuckDB at the mock database.
  Each repetition workspace is a copy of the image. Both the candidate and the
  expected are compiled *against the image*, so both read the mock tables.
- **Expected side recompiled against the mock image**: the earlier design
  compiled the expected once from the model env, which baked the datasources'
  *original* absolute file paths — so for file/remote datasources the expected
  read the real model file while the candidate read the mock (a correctness
  gap that only table addresses, being location-independent names, avoided).
  The unit tier now re-parses the validations file against the image
  (`compile_expected_against_image`), making expected and candidate symmetric.
- **The validations file is excluded from every workspace/image** — it holds
  the `matches (...)` expected answers, and the agent has file-read access, so
  seeding it would let the agent read the answer. (This was a latent leak in
  the copy-everything seeding, fixed here.)
- **Integration tier is unchanged**: real backend, original addresses,
  precompiled expected; it seeds the real model files (minus the validations
  file) and needs no repointing.
- **Live-verified** end-to-end against a **parquet file datasource** (the case
  the earlier design got wrong): `trilogy unit <model> --include-type agent`
  with an Anthropic agent → the agent's query and the recompiled expected both
  read the mock tables and matched (pass). Per-file re-render preserves std
  imports and traits (`int::year`, `string::us_state_short`, `enum<...>`), so
  real trait-using public models build cleanly.
- **Unit-tier workspace toml** = model toml minus its `[engine]` sections plus
  a DuckDB engine over the mock DB — so `[agent]` provider config carries
  through and the agent + scoring use identical connections.
- The expected select's SQL is compiled once by the test executor
  (`generator.compile_statement` of the already-processed query) and executed
  on the scoring connection; the candidate compiles in a fresh env over the
  workspace. Verified end-to-end with a live DeepSeek run
  (`trilogy unit <model> -e .env.secrets --include-type agent` → 1/1 passed,
  report.json written).
- `validate_environment` (CLI layer) grew a `scope` parameter so
  `--skip-type datasources/concepts` maps onto `ValidationScope`.

## Explicitly deferred (decided, don't redesign)

- **Multiple acceptable answers** (`matches (...) or (...)`): punt until a real need
  appears; one canonical answer + `tolerant` comparison covers the current need.
- **Ties/non-determinism**: rely on authors writing deterministic questions (the
  eval-harness question-fix philosophy applies); no engine-side tie handling.
- **Tags as selection**: wait. If file-level organization proves too coarse, the
  test-type selector could grow tag awareness (`--include-type agent:smoke`).
- **History**: per-repo pass-rate history (à la `evals/eval_history.db`) is out of
  scope for v1; the default `report.json` is the integration point.
