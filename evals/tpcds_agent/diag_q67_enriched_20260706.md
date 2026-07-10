# Diagnosis: q67 FAIL in enriched leg (run 20260706-135542_enriched)

## Symptom
Scored `fail` — 100 ref rows vs 100 cand rows, "result set differs from reference".
Re-scored against a fresh DB copy: reproduces (`fail 100 100`). 97 of 100 rows differ,
all diffs are in the `sumsales` column only — structure (rollup levels, rank values,
ordering, null placement) is identical. First diff = row 0 (grand-total-partition row,
rank 72): ref `104996.99` vs cand `107048.96`. Candidate sums are consistently higher.

## Transcript error inventory
Clean run, no framework-error signatures, no rollup/having grammar trip:
1. One `Syntax error: 15 undefined concept references` — agent wrote `item.category`
   instead of `store_sales.item.category` etc. Fixed on the immediate retry using the
   error's did-you-mean suggestions. Not a loop.
2. Second write + `trilogy run` succeeded (exit 0, 100 rows). Agent submitted.
The 2026-07-06 `by rollup`-before-`having` clause order was used correctly on the first
attempt (the enriched syntax docs the agent read state the new order).

## Root cause — two independent semantic deltas, both required to fail

Candidate: `evals\tpcds_agent\results\20260706-135542_enriched\workspace\query67.preql`

### Defect A (MODEL issue): measure substituted `ext_sales_price` for `sales_price * quantity`
Task: "sum of (per-line sales price times quantity, treating null as 0)".
Agent wrote `sum(coalesce(store_sales.ext_sales_price, 0))`; reference is
`sum(coalesce(ss_sales_price*ss_quantity,0))`.

The agent was **explicitly told they are equal by the model docs** it read via explore
(workspace `raw\store_sales.preql:31`, identical text in checked-in
`tests\modeling\tpc_ds_duckdb\store_sales.preql:31`):

> `ext_sales_price numeric(15,2)::usd, # Whole-line "total price" = sales_price x quantity, ...`

Transcript line 1517: *"I should use `ext_sales_price` which is already
`sales_price * quantity` - let me verify. From the explore output: ... Perfect, that's
exactly what we need."*

The data contradicts the doc (TPC-DS nulls columns independently). Year-2000 rows
(553,970 total): 12,894 rows where `ss_ext_sales_price IS DISTINCT FROM
ss_sales_price*ss_quantity`; 9,665 have ext non-null but product null; 3,229 the
reverse; 12,942 have ext NULL outright — despite the model declaring `ext_sales_price`
**non-nullable** (no `?`), which the agent also relied on (transcript line 1546).
Same q60 family: enrichment/model guidance invisible-trap. The model doc *and* its
nullability stamp are both factually wrong for this column.

### Defect B (QUESTION/reference issue — known q73 pattern): reference silently inner-joins nullable store FK
Reference SQL comma-joins `store` (`ss_store_sk = s_store_sk`), silently dropping the
12,810 year-2000 rows with NULL `ss_store_sk` from **every** rollup level. The model
correctly declares `SS_STORE_SK: ?store.id` (nullable), so the framework emits
`LEFT OUTER JOIN store` and keeps those rows. The task never mentions excluding
unknown-store sales — and even warns "don't add, drop, or loosen filters". The
canonical `tests\modeling\tpc_ds_duckdb\query67.preql` needs an explicit
`ss.store.id is not null` (line 4) to match — proof the exclusion is not derivable
from the question text.

### Trigger matrix (reference q67 SQL shape, DB copy, 100-row comparison vs reference)
| variant | rows differing |
|---|---|
| ext measure + LEFT store (candidate shape) | 97 |
| ext measure + INNER store (fix B only) | 81 |
| price*qty measure + LEFT store (fix A only) | 43 |
| price*qty + INNER store (both fixed) | **0** |

Grand totals (year 2000): ref measure+inner store = 1,018,289,131.65;
candidate = 1,036,555,271.90 (matches candidate rank-1 row exactly).

## Framework verification
Canonical `query67.preql` built through the same engine (`generate_sql` +
execute, `"memory".` prefix stripped) matches `query67.sql` **exactly (0/100 diffs)**.
The candidate's generated SQL (transcript msg 17) is a faithful translation of what the
agent authored: correct ROLLUP over 8 dims, correct rank partition, HAVING → post-window
WHERE, nulls-first ordering. No framework defect.

## Classification
**MODEL issue (defect A, primary) + QUESTION issue (defect B), jointly sufficient; NOT framework, NOT agent error.**
- A: the model's own column description ("= sales_price x quantity") and missing `?`
  nullability on `ext_sales_price` are false in the data and directly induced the
  substitution. (Agent contributory only in the weakest sense: the task did literally
  say "sales price times quantity", but the authoritative model doc overrode it.)
- B: reference implicitly filters a nullable FK the question never states (q73 pattern).
- Fix suggestions: correct the `ext_sales_price` comment + add `?` in
  `tests\modeling\tpc_ds_duckdb\store_sales.preql:31` (propagates to eval ingest), and
  either reword the question to state the store exclusion or de-fan the reference like
  q54's treatment.
