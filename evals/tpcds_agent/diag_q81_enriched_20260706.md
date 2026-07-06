# Diagnosis: q81 FAIL in enriched leg (run 20260706-135542_enriched)

## Symptom
`QueryResult(id=81, status='fail', ref_rows=100, cand_rows=100, detail='result set differs from reference')`.
Multiset diff vs `tests\modeling\tpc_ds_duckdb\query81.sql`: **13 rows only-ref / 13 rows only-cand** (LIMIT-100 crowding on a differing qualifying set). No framework errors anywhere in the transcript — the agent wrote the query in one shot (message 14), it ran cleanly, and the agent submitted. Silent wrong-rows.

## Error inventory (transcript `agent_log.q81.conversation.txt`)
- No tool errors, no retries. Agent correctly picked `cr.billing_customer` (= CR_RETURNING_CUSTOMER_SK, per model comment "use this for 'returning customer'"), `cr.return_address.state` (= CR_RETURNING_ADDR_SK), `text_id` for customer code, and all 16 output columns/order. Identifier/measure/output-shape patterns (2/3/4) do NOT apply.
- The single defect is filter placement: the candidate (`workspace\query81.preql:26-28`) puts **both** `cr.date.year = 2000` **and** `cr.billing_customer.address.state = 'GA'` in the row-grain WHERE, with the 1.2x-avg comparison in HAVING.

## Root cause (two independent, both proven by counterfactual)

### Cause 1 — AGENT / aggregate-scoping (dominant, 10 of 13 rows)
In Trilogy, a row-grain WHERE co-grains into every aggregate in the query. The generated SQL (CTE `questionable`) computes `cust_state_total` AND therefore `state_avg` over rows pre-filtered to `CA_STATE = 'GA'` on the customer's **current home address**:

```sql
INNER JOIN customer_address ... ON c.C_CURRENT_ADDR_SK = ca.CA_ADDRESS_SK
WHERE d_year = 2000 AND ca.CA_STATE = 'GA'
GROUP BY cr_returning_customer_sk, ca_state
```

So `state_avg` = average per returning-state **among GA-home customers only**. The reference (`query81.sql:1-12, 32-35`) computes `customer_total_return` with NO GA filter and applies `ca_state='GA'` only at final selection; its correlated avg is over ALL customers in the returning state. Per-customer totals are unaffected (filtering a customer by their own home state drops none of that customer's rows) — only the threshold moves, flipping 10 qualifying rows.

The task wording is unambiguous ("keep only customers whose total exceeds 1.2 times the average total within their returning-address state, **and** whose own current home address state is GA" — both are keep-filters after the average). The system guidance shown to the agent explicitly warned: `where student.state = 'TN' # filters ALL Data to the state` and "You must use inline filters if you want a where clause aggregate/window to be filtered" (transcript ~line 600-610). The `nested-aggregate-group-average` doc example the agent fetched (transcript msg 11) shows the correct pattern with the entity filter absent from WHERE. Agent error, not framework: engine did exactly what the query says.

### Cause 2 — QUESTION / implicit NULL-FK filter (known pattern 1, 3 of 13 rows)
`CR_RETURNING_ADDR_SK` is nullable (`?return_address.id`, workspace `raw\catalog_returns.preql:40`). The reference comma-joins `customer_address` in the CTE, silently dropping NULL-returning-address returns, and its correlated `ctr1.ctr_state = ctr2.ctr_state` never matches NULL. Trilogy correctly LEFT-joins (`LEFT OUTER JOIN customer_address AS cr_return_address...`) and joins the avg with `IS NOT DISTINCT FROM`, so a NULL-returning-state group exists and can qualify. The task never says to exclude returns with no recorded returning address. The canonical `tests\modeling\tpc_ds_duckdb\query81.preql:7,14` handles this with an explicit `cr.return_address.state is not null` (and a guidance comment about binding filters inside the conditional sum — exactly the trap in Cause 1).

## Counterfactual proof (scratch-only edits of the candidate, same engine, same DB copy)
| Variant | Edit | Result vs ref |
|---|---|---|
| original | — | FAIL, 13/13 diff |
| V1 | move `address.state='GA'` from WHERE to HAVING | FAIL, 3/3 diff |
| V3 | add `cr.return_address.state is not null` to WHERE only | FAIL, 10/10 diff |
| V2 | both edits | **PASS, exact multiset match (100/100)** |

Canonical `query81.preql` (built via same engine, tests env): **matches query81.sql exactly** — framework and curated model are healthy.

The passing sql_bare leg answer inner-joins `customer_address` on the returning addr and keeps the ctr CTE unfiltered by GA — matching the reference on both points, which is why raw SQL passed.

## Classification
**AGENT error (primary)** — GA current-address filter placed in row-grain WHERE contaminates `state_avg` (10 rows), despite explicit guidance and a fetched example showing the correct placement.
**QUESTION issue (secondary)** — implicit NULL-returning-address exclusion via reference inner join (3 rows), same family as q73/q76/q70.
Both fixes are required to pass; neither alone suffices.
