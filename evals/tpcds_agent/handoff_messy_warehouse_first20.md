# Handoff: messy-warehouse first-20 experiment

**Preliminary as of 2026-08-11.** This memo describes the hypothesis, the
current experiment, and why the first result should not yet be treated as an
effect. The intended audience is someone reviewing the setup with fresh eyes
before we run all 99 benchmark questions.

## Executive summary

We are testing whether a semantic layer shields a query-writing agent from the
messiness of a realistic warehouse. We added pre-aggregated physical tables and
unrelated business-domain tables. A SQL agent sees every table in a generated
schema. A Trilogy agent sees a curated semantic model in which the aggregates
are private implementation details and the unrelated tables are not modeled.

The directional hypothesis was:

1. Visible aggregates and unrelated tables will make the SQL agent spend more
   tokens auditing and disambiguating the warehouse, and may reduce accuracy.
2. The same physical additions will have little or no effect on the Trilogy
   agent because they do not expand its normal exploration surface.

Two first-20 runs now disagree on the direction of the SQL accuracy effect.
Before the tool optimizations, SQL with aggregates plus noise beat aggregates
alone 18/20 to 15/20 as reported, while using 3.4% fewer tokens. In the
2026-08-11 rebaseline, aggregates plus noise lost 14/20 to 17/20 and used 6.3%
more tokens. The current token delta closely matches the independently
predicted cost of rebilling the 2,604 additional schema characters, but no SQL
agent probed or selected an unrelated table. The accuracy drop therefore lacks
the behavioral mechanism the hypothesis predicts.

The tool optimization did move its intended metric: enriched raw tokens fell
12.5% and 11.8% relative to the previous run. However, the enriched pair is
still a useful negative control. Its task files, installed models, and
configuration are byte-identical, and the extra physical noise is not
introspectable, yet the current cells differ by two passes and 1.20 million
tokens. Replication is still needed before a full benchmark launch or a causal
accuracy claim.

## Context

The existing evaluation asks a fresh agent to solve each analytical question
against a DuckDB warehouse at scale factor 1. The candidate result is compared
with the benchmark reference result, including values rather than only row
counts.

There are two relevant agent paths:

- **SQL + schema:** the agent receives a generated physical schema and writes
  SQL directly.
- **Enriched Trilogy:** the agent explores a hand-authored semantic model and
  writes Trilogy, which is compiled to SQL.

Absolute token counts are not directly comparable across these two paths. The
Trilogy tool carries semantic exploration and language-help payloads that the
SQL tools do not. The meaningful comparisons here are primarily within a path:
SQL treatment versus SQL control, and enriched treatment versus enriched
control. SQL query and file execution results were made structurally closer to
the existing Trilogy result payloads for this experiment; the carefully tuned
Trilogy tool output was not changed.

## Warehouse treatments

The original physical warehouse contains 17 dimensions and 7 facts. For the
fresh run, all 24 benchmark tables were physically renamed to reduce the chance
that the model could reproduce memorized benchmark SQL:

- dimensions: `dim_<original_name>`, such as `dim_date_dim`;
- facts: `fact_<original_name>`, such as `fact_store_sales`.

There are no compatibility views under the familiar names.

### Pre-aggregated tables

Six daily aggregate tables were materialized for store, catalog, and web sales
and returns. Their names use `fact_agg_*`, and they contain the relevant foreign
keys, row counts, and sums at their declared grains. This brings the physical
table count from 24 to 30.

For the enriched path, all six tables are bound as additional Trilogy
datasources to the existing logical concepts. Their datasource and supporting
concept names begin with `_warehouse_`, keeping them out of normal agent
exploration. We separately validated that adding these private bindings does
not change the size of the normal `explore` output. The compiler remains free
to choose a raw or aggregate datasource without requiring the agent to know
that the aggregate exists.

For the SQL path, the six aggregate tables and their columns appear in
`schema.md`, so the agent must decide whether they are relevant and trustworthy.

### Unrelated noise

The cumulative-noise database adds 12 tables from unrelated domains: HR,
payroll, support, marketing, supplier contracts, fleet management, projects,
and application audit events. This brings the physical table count from 30 to
42.

The SQL agent sees these tables. Its schema tool payload grows from 19,445 to
22,049 characters, an increase of 13.4%.

The enriched agent cannot introspect arbitrary database tables and receives no
semantic definitions for the unrelated tables. The `raw/` directories and
`trilogy.toml` files in the two fresh enriched workspaces are byte-identical,
as are their 20 task files. Thus, in the enriched pair, physical noise should
be behaviorally inert unless it changes execution timing or some unobserved
part of the harness.

## Fresh four-way setup

The current four cells are cumulative: both noise cells also contain the six
aggregate tables.

| Category | Physical database | Agent-visible interface |
|---|---|---|
| `sql_schema_aggregates` | 24 renamed base tables + 6 aggregates | Generated schema for all 30 tables |
| `sql_schema_noise` | Same 30 + 12 unrelated tables | Generated schema for all 42 tables |
| `enriched_aggregates` | 24 renamed base tables + 6 aggregates | Curated model with private aggregate bindings |
| `enriched_noise` | Same 30 + 12 unrelated tables | The same curated model and private bindings |

Run configuration:

- questions q01-q20;
- DuckDB scale factor 1;
- `deepseek/deepseek-chat`;
- Trilogy 0.3.320;
- maximum 75 agent iterations per question;
- fresh context for every question;
- one worker per category, with the four category processes launched in
  parallel;
- canonical reference SQL executed against a separate, unrenamed reference
  database.

The last point matters because benchmark reference SQL still names the original
tables. Candidate SQL and compiled Trilogy SQL execute against the renamed
variant database; reference SQL executes against the untouched cached database.

The generic `workspace/` is the prepared template and scoring/staging location.
`workspace/_worker_0/` is an isolated execution copy used to avoid DuckDB file
locking; candidate files are staged back to the generic workspace before
scoring.

We also audited the 80 fresh agent sessions for benchmark-name leakage. The
literal benchmark name does not appear in session-start commands, task files,
configuration, schema input, installed semantic models, or tool results. It is
still used in evaluator-side report names and this memo, which are not agent
inputs.

## Tool-optimization rebaseline (current)

Run prefix: `20260811-015536`. The editable environment loaded the current
working tree and reported Trilogy 0.3.321. All four category legs exited 0;
all 80 candidates were scored, with no crash or scorer-error artifacts.

The invocation was identical to the prior run:

```powershell
.venv\Scripts\python.exe evals\tpcds_agent\run_eval.py --categories sql_schema_aggregates,enriched_aggregates,sql_schema_noise,enriched_noise --num-queries 20 --provider deepseek --model deepseek-chat
```

| Category | Pass | Tokens | Iterations | Tool calls | Failures |
|---|---:|---:|---:|---:|---|
| SQL + aggregates | 17/20 | 2,455,252 | 163 | 203 | q08, q14, q20 |
| SQL + aggregates + noise | 14/20 | 2,610,710 | 163 | 213 | q05, q08, q12, q16, q18, q20 |
| Enriched + aggregates | 17/20 | 6,686,260 | 231 | 298 | q01, q14, q20 |
| Enriched + aggregates + noise | 19/20 | 5,490,215 | 210 | 283 | q11 |

### What changed relative to `20260810-211903`

| Category | Pass change | Token change |
|---|---:|---:|
| SQL + aggregates | 15/20 to 17/20 | +451,484 (+22.5%) |
| SQL + aggregates + noise | 18/20 to 14/20 | +674,085 (+34.8%) |
| Enriched + aggregates | 17/20 to 17/20 | -957,316 (-12.5%) |
| Enriched + aggregates + noise | 16/20 to 19/20 | -733,505 (-11.8%) |

The old SQL aggregate score includes the q08 scorer timeout discussed below;
its post-run candidate-correct score was 16/20. The table deliberately retains
the immutable raw score.

The enriched token reduction is directionally consistent across both cells.
Trilogy tool calls fell from 300 to 278 in the aggregate cell and from 267 to
263 in the noise cell; average Trilogy tool-result size fell from 6,076 to
5,849 characters and from 7,112 to 6,525 characters, respectively. This is
evidence that the shorter help corpus and session-scoped output deduplication
reduced the enriched intercept, although one stochastic run cannot attribute a
precise percentage to either optimization.

The SQL comparison is also unusually close to the token-cost model. Noise adds
2,604 schema characters. The observed 155,458-token cell delta is about 7,773
tokens per question, or 2.98 billed tokens per added schema character per
question. The design estimate was roughly 2.7. Both SQL cells used exactly 163
LLM iterations, so the result is consistent with a schema-rebilling slope even
without additional audit turns.

Mechanism audit:

- no SQL probe or final candidate referenced any of the 12 unrelated tables;
- aggregate-only SQL q04 probed and ultimately used aggregate tables;
- noisy SQL q08 probed an aggregate but did not retain it in the final query;
- none of the 40 enriched candidates explicitly referenced a private aggregate;
- compiling all 40 saved enriched candidates selected no `fact_agg_*`
  datasource and produced no compile errors;
- enriched task files, `raw/` models, and `trilogy.toml` are byte-identical
  between the two current cells;
- the benchmark-name leak audit remains clean across static inputs,
  session-start commands, and tool results.

The current SQL accuracy result is in the hypothesized direction, but should
not yet be read as an effect of warehouse disambiguation. The agent did not
interact with the unrelated tables, the prior run moved in the opposite
direction, and the invisible enriched control still swung by two passes.

## Pre-optimization first-20 result

Run prefix: `20260810-211903`.

The exact invocation was:

```powershell
.venv\Scripts\python.exe evals\tpcds_agent\run_eval.py --categories sql_schema_aggregates,enriched_aggregates,sql_schema_noise,enriched_noise --num-queries 20 --provider deepseek --model deepseek-chat
```

| Category | Reported pass | Candidate-correct pass | Tokens | Iterations | Tool calls |
|---|---:|---:|---:|---:|---:|
| SQL + aggregates | 15/20 | 16/20 | 2,003,768 | 147 | 176 |
| SQL + aggregates + noise | 18/20 | 18/20 | 1,936,625 | 140 | 171 |
| Enriched + aggregates | 17/20 | 17/20 | 7,643,576 | 234 | 320 |
| Enriched + aggregates + noise | 16/20 | 16/20 | 6,223,720 | 214 | 287 |

“Candidate-correct” differs from the immutable raw report only for SQL q08.
The evaluator timed out after 180 seconds while scoring it, but the saved query
ran in 28 ms and passed a later direct in-process rescore. The raw funnel is
intentionally left at 15/20 so the original run outcome remains auditable.

Failure sets make the instability more visible:

- SQL + aggregates: q05, q15, q18, q20, plus the q08 scoring error;
- SQL + aggregates + noise: q02 and q05;
- enriched + aggregates: q11, q16, and q20;
- enriched + aggregates + noise: q06, q11, q14, and q20.

Only one of the 40 saved SQL candidates used an aggregate table: q02 in the
aggregate-only cell, where it passed. None of the 40 compiled enriched
candidates selected a `fact_agg_*` datasource. Aggregate selection is not
required by the hypothesis—the intended Trilogy benefit is insulation from
physical complexity—but this means the run does not measure a materialized-view
speedup.

## Why the two results remain non-intuitive

### 1. The visible SQL-noise result flipped direction

In the pre-optimization run, adding 12 visible tables increased the SQL schema
payload by 13.4%, but the noise cell gained three reported passes and used
67,143 fewer tokens. Even after correcting the q08 scorer anomaly, it gained
two candidate-correct passes. In the current run, the same treatment lost three
passes and used 155,458 more tokens.

There is no consistent query-level mechanism behind the accuracy changes. The
agents authored different solutions, and none inspected an unrelated table.
The current token delta is consistent with the deterministic schema-size slope;
the accuracy delta is not yet tied to a treatment mechanism.

### 2. An invisible enriched treatment also moved substantially

The enriched noise cell had the same prompt-visible model and configuration as
the enriched aggregate-only cell. Nevertheless, it used 1,419,856 fewer tokens
(18.6%), made 33 fewer tool calls, and lost one pass. That is a large swing in a
pair where the intended treatment is invisible to the agent.

This is the best evidence that one 20-question trajectory per cell has enough
variance to swamp the expected treatment effect. It also warns against
explaining the SQL improvement after the fact as a beneficial property of
noise.

### 3. Some pass/fail movement is candidate-shape or infrastructure noise

Fresh enriched q06 is a concrete example. The noisy-cell agent authored a
grouped category average whose key is later required by a join. A genuine
Trilogy compiler liveness bug prunes that key and renders the join as
`RIGHT OUTER JOIN ... ON 1=1`, producing 51 rows instead of 46. The
aggregate-only agent authored a different, explicit keyed subset join and
passed. Neither candidate selected an aggregate table. The compiler defect is
documented separately in
[`handoff_q06_rowset_union_join_key_pruned.md`](handoff_q06_rowset_union_join_key_pruned.md).

Other failures include ordinary agent mistakes, such as omitted null handling
or an incorrect rollup grain. q08 is a scorer timeout rather than a bad
candidate. End-to-end pass rate is still the benchmark outcome we care about,
but these causes should be tagged when using 20 questions to infer a warehouse
treatment effect.

### 4. An earlier exploratory run moved in different directions

For context, the prior `20260810-200843` first-20 run produced:

| Category at that time | Pass | Tokens |
|---|---:|---:|
| SQL + aggregates | 19/20 | 1,796,144 |
| SQL + noise | 17/20 | 1,829,424 |
| Enriched + aggregates | 17/20 | 5,540,578 |
| Enriched + noise | 19/20 | 4,622,135 |

This is not a clean replicate. That run retained the familiar physical table
names, and its noise variants contained noise instead of the new cumulative
aggregates-plus-noise treatment. It is useful only as qualitative evidence that
both pass sets and token counts can move substantially between runs. It should
not be pooled with either renamed-table run as if the setups were identical.

## Current interpretation

The two renamed-table first-20 runs do not support the claim that unrelated
tables improve SQL, nor do they provide clean evidence that unrelated tables
degrade SQL accuracy. The most defensible interpretation is:

- the aggregate and noise treatments are constructed as intended;
- the semantic layer successfully hides both kinds of physical complexity from
  normal agent exploration;
- SQL visibly receives the additional warehouse complexity;
- the current SQL raw-token delta is consistent with the predictable cost of
  rebilling a larger schema, even though agent behavior did not change;
- the unrelated tables did not induce audits or wrong-table selections, so the
  proposed behavioral degradation mechanism was not observed;
- the Trilogy tool/help optimization likely lowered its raw-token intercept,
  but the identical enriched controls still show a large stochastic noise floor;
- the accuracy deltas are not separable from run variance, and no causal
  accuracy conclusion should be drawn yet.

The Trilogy path's larger absolute token use is a separate tooling/language-DX
question. It does not invalidate the within-path insulation hypothesis, but it
does mean “Trilogy uses fewer raw tokens than SQL” is not the claim tested here.
The provider logs still expose only raw prompt/completion tokens, not cache-hit
and cache-miss tokens, so these totals are not a direct cost comparison either.

## Suggested next experiment

Before launching all questions once, run replicated and randomized first-20
cells:

1. Restore a full 2x2 physical treatment for each agent path: base only,
   aggregates only, noise only, and aggregates plus noise.
2. Run at least 3-5 independent replicates per cell, preferably with recorded
   provider seed/sampling settings if supported.
3. Randomize or rotate cell order instead of launching all four provider streams
   in a fixed parallel arrangement.
4. Hash and retain the initial task plus every agent-visible static input for
   each cell. For the enriched noise control, compare tool responses as well as
   installed files.
5. Report paired per-question changes, not only aggregate pass rate. Separate
   candidate errors, compiler errors, and scoring/harness errors.
6. Track aggregate discovery and selection explicitly. For SQL, record audits
   and references to `fact_agg_*`; for Trilogy, inspect compiled datasource
   selection without exposing it to the agent.
7. Predeclare the decision rule for proceeding to all questions—for example, a
   repeated SQL token/accuracy degradation with enriched deltas centered near
   zero, rather than a single favorable or unfavorable 20-question draw.
8. Add confusable in-domain noise if the goal is to test audit behavior rather
   than only the deterministic schema-size slope. The current unrelated-domain
   tables were never inspected.

A cheaper alternative is to repeat only the two enriched cells first. Because
their agent-visible inputs should be identical, their between-run dispersion
directly estimates the noise floor against which the SQL treatment delta must
be judged.

## Artifacts

- Current combined funnel: [`charts/funnel.md`](charts/funnel.md)
- Warehouse construction: [`warehouse_variants.py`](warehouse_variants.py)
- Aggregate DDL: [`warehouse/aggregates.sql`](warehouse/aggregates.sql)
- Noise DDL: [`warehouse/noise.sql`](warehouse/noise.sql)
- Category definitions: [`spec.py`](spec.py)
- Proposed confusable-noise/replicate design:
  [`design_messy_warehouse_v2.md`](design_messy_warehouse_v2.md)
- Current rebaseline reports:
  - [`results/20260811-015536_sql_schema_aggregates/report.md`](results/20260811-015536_sql_schema_aggregates/report.md)
  - [`results/20260811-015536_sql_schema_noise/report.md`](results/20260811-015536_sql_schema_noise/report.md)
  - [`results/20260811-015536_enriched_aggregates/report.md`](results/20260811-015536_enriched_aggregates/report.md)
  - [`results/20260811-015536_enriched_noise/report.md`](results/20260811-015536_enriched_noise/report.md)
- Pre-optimization reports:
  - [`results/20260810-211903_sql_schema_aggregates/report.md`](results/20260810-211903_sql_schema_aggregates/report.md)
  - [`results/20260810-211903_sql_schema_noise/report.md`](results/20260810-211903_sql_schema_noise/report.md)
  - [`results/20260810-211903_enriched_aggregates/report.md`](results/20260810-211903_enriched_aggregates/report.md)
  - [`results/20260810-211903_enriched_noise/report.md`](results/20260810-211903_enriched_noise/report.md)
