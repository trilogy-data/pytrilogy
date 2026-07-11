# DABstep agent eval

Trilogy agent eval over [DABstep](https://huggingface.co/datasets/adyen/DABstep)
(Adyen's Data Agent Benchmark for multi-step reasoning): a real-world payments
dataset where answering requires combining transaction data with domain
documentation (fee-rule matching semantics live in `manual.md`, not the schema).

Unlike TPC-DS/TPC-H there is no generator or scale factor — the data is a fixed
set of files, and scoring compares the agent's query result against a canonical
reference SQL per task (`references/query<ID>.sql`), exactly like the other
benchmarks' custom-reference path.

## Current scope: 10 harness-validation tasks

`query_prompts.json` carries the 10 dev-split tasks (the only split with
published answers). Every reference SQL is validated against those answers:

- 9/10 reproduce the published answer exactly (list answers match id-for-id;
  numerics match to float noise).
- Task 2697: the published ACI choice (`E`) reproduces under every aggregation
  interpretation tested, but the published fee value (13.57) does not
  (16 structural variants tried); the reference pins our stated definition
  (per-transaction mean matched fee, summed → `E`, 41.66) and the prompt spells
  that definition out.
- Task 70's published answer is `Not Applicable` (no fraud-fine concept exists
  in the data/docs); its reference is the literal sentinel row. Every prompt
  carries the same standing rule — return a single `'Not Applicable'` field
  when the question should not or cannot be answered — mirroring the guideline
  DABstep attaches to every task, so 70 isn't special-cased.

Question text is kept close to the original, with the answer *shape* made
explicit (result-set scoring needs a determined output), and ambiguity that
DABstep resolves via its string-answer guidelines resolved in the prompt
instead.

The full 450-task split ships with answers withheld (leaderboard-only). Scaling
this eval up means authoring canonical references per task with the methodology
validated here.

## Setup

```bash
python evals/dabstep_agent/download_data.py   # fetches data/context (~24MB) + task lists
```

`data/`, `results/`, and `.cache/` are gitignored; the task lists
(`tasks_dev.json`, `tasks_all.json`) are checked in.

## Run

```bash
python evals/dabstep_agent/run_eval.py                       # ingest category, all 10
python evals/dabstep_agent/run_eval.py --query-ids 5,49      # subset
python evals/dabstep_agent/run_eval.py --categories sql_schema,enriched   # the headline A/B
```

Same flags as the other benchmarks (`evals/common/main.py`).

## The A/B this eval is built around

DABstep's difficulty is domain knowledge (fee-matching semantics live in
`manual.md`, not the schema), so doc availability is per-category
(`spec.doc_categories`):

| Category   | Gets                                             |
|------------|--------------------------------------------------|
| sql_bare   | db + markdown docs                               |
| sql_schema | db + schema.md + markdown docs                   |
| ingest     | auto Trilogy model + markdown docs               |
| enriched   | curated Trilogy model ONLY — no markdown, no file read |

The comparison: **agent + raw docs + db** vs **agent + semantic model**. The
enriched model (`enriched_model/*.preql`, the spec's `default_enriched_dir`)
is persistent and hand-curated — the manual's content is encoded as concept
descriptions (ACI code meanings, account types, wildcard/'applies to all'
matching semantics, the fee formula) and derived concepts (`txn_date`,
`txn_month`, `intracountry`, `merchant_monthly_volume`,
`merchant_monthly_fraud_pct`). Improving it over time is part of the eval
loop; it must carry the domain knowledge on its own, but must NOT encode
per-question answers.

Other differences from the generator benchmarks:

- `database_builder` — DuckDB built from `data/context` (`db_build.py`);
  list-valued fee-rule fields are unnested into child tables
  (`fee_account_types`, `fee_merchant_category_codes`, `fee_acis`,
  `merchant_acquirers`); empty child set = rule unconstrained on that dimension.
- `doc_files` — `manual.md`, `payments-readme.md`, and `schema_notes.md`,
  installed per the category table above.
