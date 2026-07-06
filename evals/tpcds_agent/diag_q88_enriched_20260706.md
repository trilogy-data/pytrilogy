# q88 ENRICHED fail diagnosis — 20260706-135542

## Classification: AGENT error (wrong join path: customer's *current* household demographics instead of the sale's recorded household demographics)

## Symptom
Enriched leg scorer: `fail — result set differs from reference` (1 row vs 1 row, 8 count columns, silent wrong values). Both raw-SQL legs pass (sql_bare `query88.sql` filters via `ss_hdemo_sk`).

## Error inventory (transcript `results\20260706-135542_enriched\agent_log.q88.conversation.txt`)
- No tool errors, no framework errors, no retries. Single clean write + `trilogy run` succeeded first try.
- The one and only defect is in the WHERE clause the agent authored (transcript lines 1526–1534): it addressed the hd filter as `ss.customer.household_demographic.*`.
- The agent's own exploration output (transcript lines 1247–1249) showed the correct concept with its guidance comment: `household_demographic` — "the household demographics recorded on this sale (the buyer's household at the time of purchase)". The reasoning block (lines 1484+) discusses time buckets and counting grain at length but never weighs the two hd paths; the customer-routed path is used without comment.

## Ground truth on the DB copy (sf1)
| band | candidate (customer path) | reference / canonical / counterfactual |
|---|---|---|
| 8:30–9:00 | 2336 | 2344 (-8) |
| 9:00–9:30 | 4871 | 4747 (+124) |
| 9:30–10:00 | 4346 | 4593 (-247) |
| 10:00–10:30 | 7095 | 7590 (-495) |
| 10:30–11:00 | 6501 | 7111 (-610) |
| 11:00–11:30 | 4107 | 3933 (+174) |
| 11:30–12:00 | 3974 | 4129 (-155) |
| 12:00–12:30 | 4577 | 4552 (+25) |

All 8 bands differ (mixed sign — a re-filtering of a near-independent attribute, not systematic inflation/deflation).

## Counterfactual proof (fail → pass)
One mechanical substitution in `workspace\query88.preql` — `ss.customer.household_demographic` → `ss.household_demographic` — executed against the DB copy returns
`(2344, 4747, 4593, 7590, 7111, 3933, 4129, 4552)`, byte-equal to the reference SQL result. Nothing else changed.

## Mechanism verification (raw SQL on copy)
- Reference path: `store_sales JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk`.
- Candidate path (as compiled by Trilogy, seen in the run's generated SQL: `... JOIN customer ... JOIN household_demographics ON C_CURRENT_HDEMO_SK = HD_DEMO_SK`): a hand-written raw-SQL equivalent for band 1 (hour=8, minute>=30) returns 2336 — exactly the candidate's band-1 value. Trilogy compiled precisely what was asked; the wrong numbers are 100% the path choice.
- Divergence magnitude at store 'ese': of 458,443 store_sales rows, 452,774 (98.8%) have `c_current_hdemo_sk IS DISTINCT FROM ss_hdemo_sk` (plus 10,821 NULL customer FKs vs 10,760 NULL hdemo FKs). The two attributes are essentially independent; the candidate answered a different question that happened to produce similar-magnitude counts.

## Canonical sanity check
`tests\modeling\tpc_ds_duckdb\query88.preql` (built via generate_sql with its own environment; `"memory".` schema stripped to run against the file DB) returns the identical reference tuple. Canonical model and framework are healthy.

## Known-pattern check
- Pattern 1 (implicit NULL-FK filter): NOT the cause — the candidate's hd filter itself drops NULL-hd rows on both paths; the delta is mixed-sign, not one-way inflation.
- Pattern 2 (identifier confusion): MATCHES in spirit — wrong attribute path (`customer.household_demographic` = C_CURRENT_HDEMO_SK vs sale-level `household_demographic` = SS_HDEMO_SK).
- Patterns 3/4 (measure choice, output shape): not applicable (counts correct in form; 8 columns, 1 row as asked).

## Root cause & evidence
- AGENT error. The enriched model (`workspace\raw\store_sales.preql:19,59`) exposes the correct sale-level concept with an unambiguous guidance comment; `workspace\raw\customer.preql:8,40` equally clearly labels the other as "the customer's current household demographic profile (C_CURRENT_HDEMO_SK)". The agent read both and picked the customer route without deliberation.
- Task wording nuance (not enough to reclassify): "to households where the dependent count and vehicle count satisfy..." does not say "recorded on the sale", but the sale-level concept is the natural reading and the model doc says exactly that. Reference/canonical agree.
- No FRAMEWORK involvement: generated SQL faithfully implements the authored query. No MODEL gap: both concepts exist, both documented.

## Possible mitigation
Prompt/guidance nudge: when a fact table carries its own demographic/dimension FK and the customer also carries a "current" variant, prefer the fact-level (point-of-sale) path unless the question says "current". (Same trap exists for `customer_demographic` on store_sales vs `customer.demographics`.)
