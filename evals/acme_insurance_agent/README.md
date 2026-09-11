# ACME Insurance agent eval

Trilogy agent eval over the dataset behind dbt's 2026 post
[Semantic Layer vs. Text-to-SQL](https://docs.getdbt.com/blog/semantic-layer-vs-text-to-sql-2026):
the data.world [ACME Insurance](https://github.com/datadotworld/cwd-benchmark-data)
"chat with the data" benchmark, as packaged in
[dbt-labs/dbt-llm-sl-bench](https://github.com/dbt-labs/dbt-llm-sl-bench).

The post scored eleven questions, each asked twenty times, and reported
text-to-SQL at 84-90% versus a modeled semantic layer at 98-100%. This eval
asks the same eleven questions through the shared harness (`evals/common`):
the SQL legs are the post's text-to-SQL condition (`sql_schema` gets the same
`ACME_small.ddl` the post's agent saw), and `enriched` is the semantic-layer
condition with a curated Trilogy model in place of dbt's MetricFlow project.

## Layout

| Path | What |
|---|---|
| `data/` | the thirteen `ACME_small.ddl` tables as CSV, the benchmark TTL, and their Apache-2.0 license (vendored from the two repos above) |
| `build_catalog.py` | regenerates `query_prompts.json` + `references/` from the TTL; ids 1-11 are the post's questions in its harness order, 12-43 the rest of the ACME set |
| `references/query<NN>.sql` | the benchmark's own gold SQL, DuckDB-adapted (only `DATEDIFF` needed a rewrite) |
| `schema.md` | the DDL handed to the `sql_schema` leg |
| `db_build.py` | loads the CSVs into a cached DuckDB (`.cache/acme.duckdb`); no scale factor |
| `enriched_model/` | the curated Trilogy model; `query<NN>.preql` beside it are canonical answers the installer strips before the agent sees the model |
| `test_model.py` | scores every canonical answer against its gold reference |

The dataset is tiny (2 policies, 2 claims, 6 premiums, 8 claim amounts), so
this measures whether an agent finds the right join path and grain, not
whether it copes with scale.

## The model

Six entity files: `policy` (with the holder and selling agent pivoted out
of `Agreement_Party_Role`), `coverage`, `policy_amount` (every policy-level
amount, with `is_premium` for the rows the `Premium` table marks), `claim`
(with the `Claim_Coverage` -> `Policy_Coverage_Detail` bridge flattened so
`policy.*` is one hop away), `claim_amount` (`is_loss_payment` ...
`is_expense_reserve` from the four marker tables, combined into
`amount_kind`, plus per-claim `loss_payment_amount` ... `total_loss`), and
`catastrophe`. Named metrics cover the post's questions the way a dbt
semantic model would: `total_premium`, `average_policy_size`, `policy_count`,
`claim_count`, `average_days_to_settle`, `loss_amount`, `total_loss`.

The five key-only marker tables (`Premium`, `Loss_Payment`, `Loss_Reserve`,
`Expense_Payment`, `Expense_Reserve`) are modeled the way `trilogy ingest`
now models them: each is its own key-only file, and the table it marks
imports it and defines a membership flag (`auto is_premium <- id in
premium.id`). A membership flag is a real boolean, needs no join to read,
and keeps the marker file an island, so neither `policy_amount` nor
`claim_amount` needs a query-backed datasource. (`policy` and `claim` still
use one each, for the role pivot and the coverage bridge.)

The one modeling decision that matters for scoring: claims bind their policy
as a partial (`~`) reference, because not every policy has a claim. Trilogy
therefore keeps claim-less policies in a claim-metric-by-policy query (count
0, NULL average) where the gold SQL's inner join drops them, and the agent
has to pin the population with `where claim.id is not null` (questions 2 and
8). The model comments say so; the language reference says the same.

## Question wording

The eleven blog questions are asked in a fully specified form: each states
the grain, the columns in order, and which rows count, exactly as the gold
SQL computes them (`build_catalog.py`'s `SPECIFIED` table). The verbatim
blog text stays beside each entry as `prompt_original`. The first run used
the verbatim wording, and its failures were almost all wording: q03 never
says "by policy holder" and q02/q08 never say whether claim-less policies
appear, so agents on every unmodeled leg guessed the shape. The 32 extended
questions keep their original wording.

## Run

```bash
python -m pytest evals/acme_insurance_agent/test_model.py      # model answers the gold
python evals/acme_insurance_agent/run_eval.py --category enriched
python evals/acme_insurance_agent/run_eval.py \
  --categories sql_bare,sql_schema,ingest,enriched --concurrency 2
python evals/acme_insurance_agent/run_eval.py --num-queries 43   # whole ACME set
```

Same flags as the other benchmarks (`evals/common/main.py`); needs
`DEEPSEEK_API_KEY` in `.env.secrets` or the environment. Run reports, task
prompts, readable conversation logs and the agent's final queries are
committed under `results/`; database copies, the installed model copy and raw
JSONL traces are not.

## Results

Three passes over the eleven blog questions with `deepseek/deepseek-v4-flash`
(one agent per question, fresh context each; `charts/` has the per-question
matrix of the latest run). The first used the verbatim blog wording
(`20260911-001859`); the second the fully specified wording
(`20260911-040324`), after the tooling fixes in `trace_audit.md`; the third
(`20260911-132451`) adds the spec's `dataset_note` to every task on every
leg, telling the agent the sample is tiny so a small count is not a symptom;
a fourth (`20260911-135032`, Trilogy legs only) has ingest write a real file
header (grain, properties, imports, referenced-by, key-only flag) and trims
the example summaries out of `agent-info query`:

| Leg | Post's condition | Verbatim wording | Specified wording | + dataset note | + ingest header |
|---|---|---|---|---|---|
| `sql_bare` (db only) | text-to-SQL, no DDL | 10/11, 697k tokens | 11/11, 338k | 9/11, 292k | |
| `sql_schema` (db + DDL) | text-to-SQL | 10/11, 382k | 11/11, 318k | 11/11, 278k | |
| `ingest` (auto Trilogy model) | — | 9/11, 1,574k | 11/11, 1,310k | 11/11, 884k | 11/11, 609k |
| `enriched` (curated Trilogy model) | semantic layer | 11/11, 649k | 11/11, 627k | 11/11, 425k | 11/11, 427k |

With the output shape stated, every leg answers every question; what
separates them is cost. The curated model runs at about half the auto-ingested
model's tokens, and the SQL legs are cheapest of all on a 13-table schema this
small. The two `sql_bare` misses on the third pass (q02, q08) are the post's
failure mode verbatim: the agent joined `Claim` to `Policy_Coverage_Detail`
on `Insurable_Object_Identifier` instead of walking through `Claim_Coverage`.

For comparison the post reports, on the same questions, text-to-SQL at
84-90% and the modeled semantic layer at 98-100% across twenty repetitions.
A single pass here is not a repetition study, so treat the numbers as a
first reading rather than a headline.

What failed on the verbatim wording, and why it is the semantic layer's
argument in miniature:

- **q03** (`What is the total amount of premiums that a policy holder has paid?`)
  fails on every unmodeled leg. The question never says "by policy holder",
  so each agent returned one grand total; the gold groups by holder id. The
  curated model's `premium` comment names `policy.policy_holder_id` as the
  grouping for "what each policy holder has paid", and the enriched agent
  used it.
- **q09** on `ingest`: the agent computed the right average but returned
  its numerator and denominator alongside it; the scorer compares whole
  rows.

The ingest leg's token bill is now ~1.4x the enriched leg's, and the
enriched leg ~1.5x the SQL legs. `trace_audit.md` ("Token audit") breaks
both down: the language reference the agent re-reads on every question is
the largest single line (about a third of each Trilogy leg's prompt tokens),
and on `ingest` the rest is discovery over thirteen one-table files, three
of which are key-only marker tables the agent has to probe empirically. The
ingest header halved the explores (80 -> 39 across the eleven questions).
