# Diagnosis: q78 FAIL — enriched leg, run 20260706-135542

## Verdict

**AGENT error** (wrong measure columns: extended instead of per-unit). The known
`is_returned` model defect is **refuted** as the cause of this failure — it did not
manifest in this run. No framework involvement.

## Symptom

`score_query` (78, enriched workspace, tpcds_sf1 copy): `fail — result set differs
from reference`, 100 ref rows vs 100 candidate rows. Reproduced deterministically.

## Error inventory (transcript `agent_log.q78.conversation.txt`)

Exactly one runtime error in the whole session (line ~4283): a parse-time
"3 undefined concept references" for bare aliases (`store_qty`, `store_wholesale_cost`,
`store_sales_price`) in ORDER BY; the agent immediately fixed it by qualifying with
`store_agg.`. No binder / catalog / Unexpected errors; no framework-error signatures.

The agent DID consume `store_sales.is_returned = false` in the store channel (and the
web/catalog equivalents), exactly as the model doc instructs ("never returned" =
`is_returned = false`).

## Diff summary (candidate vs `tests\modeling\tpc_ds_duckdb\query78.sql` on the same DB)

- Key overlap: **100/100** (year, item, customer) — identical row set, identical order.
- `ratio`, `store_qty`, `other_qty` columns: **exact match** on every row.
- ALL 100 rows differ **only** in the four money columns, each exactly the extended
  amount vs the per-unit sum. E.g. row 1: ref `store_wholesale_cost=15.60`,
  candidate `904.80` = 15.60 x qty 58.

Cause in the candidate (`workspace\query78.preql` lines 15–16, 29–30, 43–44): the agent
summed `ext_wholesale_cost` / `ext_sales_price`. The question asks for "sum of sold
quantity, **wholesale cost**, and **per-line sales price**" — official TPC-DS q78 sums
`ss_wholesale_cost` / `ss_sales_price` (per-unit columns), and the model documents the
distinction explicitly (`sales_price` = "charged for ONE unit", `ext_sales_price` =
"Whole-line total = sales_price x quantity"; `wholesale_cost` = "Per-unit wholesale
acquisition cost"). The agent's own planning text even restated the target as
`sum(wholesale_cost), sum(sales_price)` (transcript ~line 1843) and then silently wrote
the `ext_` variants with no recorded justification.

**Proof:** re-generating the candidate with only
`ext_wholesale_cost -> wholesale_cost` and `ext_sales_price -> sales_price` substituted
yields an **EXACT MATCH** (all 100 rows, all 10 columns) against query78.sql.

## Known model defect (store `is_returned` INNER-join collapse): REFUTED here

- The proposed fix is NOT applied: `store_sales.preql:92` still binds
  `SR_TICKET_NUMBER: _returned_ticket` on the raw `memory.store_returns` datasource
  (web/catalog use the NULL-padded pattern, `web_sales.preql:101`).
- But the collapse did not occur in this plan shape: the candidate's generated SQL
  joins `store_sales LEFT OUTER JOIN store_returns` on (item, ticket), and the
  candidate's store-channel keys/quantities match the reference exactly on all 100 rows
  (reference never-returned store groups at y2000 w/ customer = 486,964; no 1,853-group
  collapse signature). The defect remains a latent risk for other query shapes but is
  not the cause of THIS failure.

## Canonical health check

`tests\modeling\tpc_ds_duckdb\query78.preql` built through the same engine (memory
catalog prefix mapped to the scoring DB) returns an **EXACT MATCH** vs query78.sql —
canonical reference pair is healthy.

## Classification

**AGENT error.** Evidence: single-substitution column fix produces an exact pass; the
model docs disambiguate per-unit vs extended in the enriched comments the agent read;
the SQL-bare leg passed with `sum(ss_wholesale_cost)/sum(ss_sales_price)`.
Secondary note (minor, not causal): the question phrase "per-line sales price" could be
tightened to "per-unit sales price" — but "wholesale cost" (no "extended") is
unambiguous and the agent got both wrong consistently.
