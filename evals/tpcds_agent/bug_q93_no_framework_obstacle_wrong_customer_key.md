# q93 — NO framework obstacle (agent picked business id, not surrogate; token bar false-positive)

Run: `evals/tpcds_agent/results/20260708-135136_enriched` — q93 status=fail, 989,173 tokens, **no error signature**.
Report reason: "result set differs from reference" (rows 100/100 match).

## Verdict
**Not a framework bug. Not a slow plan. Agent error + a guidance opportunity.**
The framework resolved everything correctly and fast (~130 ms). The failure is a wrong
*column choice*: the agent grouped/reported `ss.customer.id` (the **business** id,
`C_CUSTOMER_ID`, which requires a lookup into the `customer` table) instead of
`ss.customer.sk` (the **surrogate** `SS_CUSTOMER_SK` literally stored on the sale line).
The task explicitly forbade this: *"the customer identifier exactly as recorded on the
sale line (do not look the customer up in the customer table to translate it)."*

## Symptom in the log
- Agent's final `query93.preql` runs clean (exit 0, 134 ms, 7169 groups, 100 rows).
- The 989k tokens are **self-verification churn**, not a stuck construct: msgs 51-76 the
  agent repeatedly re-runs the same query worrying whether the null-customer "unidentified
  group" lands in the top 100 (it does not — its sum 64928.44 is non-null so it sorts after
  the 260 null-sum rows), and hit a dead end trying `offset` (Trilogy has no OFFSET, msg 66).
  Combined with the large repeated `explore` dumps re-sent every turn, cumulative tokens
  balloon over 500k with only 76 messages. The >500k heuristic is a **false positive** here.

## Proof (harness, generate_sql/execute only)
`ws = .../20260708-135136_enriched/workspace`, `eng = scoring.make_scoring_engine(...)`.

Reference SQL (query93.sql, groups by `ss_customer_sk`): 7169 groups; first rows
`(693,None),(893,None),(2146,None),(2243,None),(2301,None)`.

| Variant | rows (full) | key column | sum multiset | vs reference |
|---|---|---|---|---|
| Agent `ss.customer.id` | 7169 | `AAAAAAAAAABCBAAA`… (string) | — | **differs (label only)** |
| `ss.customer.sk`        | 7169 | `693, 893, 2146`… (int)      | == agent | **matches** |

`Counter(sum) of agent == Counter(sum) of sk` → **True**. So the customer join introduced
**no fanout** and **no value drift**; only the reported key column differs.

## Generated-SQL diff (the smoking gun)
- `ss.customer.id`:
  `FULL JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"`
  → selects `"ss_customer_customers"."C_CUSTOMER_ID"` (the forbidden customer-table translation).
- `ss.customer.sk`:
  no customer join; selects `"ss_store_sales"."SS_CUSTOMER_SK"` directly (correct — "as recorded on the sale line").

The `FULL JOIN` to `customer` did **not** leak unmatched customer rows (both variants = 7169
rows, sums identical), so the join semantics are correct here too.

## Trigger matrix (one ingredient toggled)
| toggle | result |
|---|---|
| key = `.id` (agent) | 7169 groups, string ids, sums correct → mismatches ref labels → FAIL |
| key = `.sk` (canonical) | 7169 groups, int sks, sums correct → MATCHES ref |
| reason filter on/off | orthogonal; filter resolves correctly either way |
| null-customer group | preserved as one group in both (FULL/left-anchor keeps null sk); sorts past top-100 |

Canonical `tests/modeling/tpc_ds_duckdb/query93.{sql,preql}` uses `ss.customer.sk` and matches
the reference — corroborating that `.sk` is the intended column.

## Root cause
Agent-side semantic error at `workspace/query93.preql:5` (`ss.customer.id as customer_id`).
No `trilogy/` file:line is implicated — resolution, join type, grain, and null-group handling
are all correct.

## Optional guidance follow-up (not a code fix)
"identifier exactly as recorded on the sale line / do not translate via the dimension table"
maps to the **surrogate `.sk`** stored on the fact, not the business `.id`. A hint that
`.id` triggers a dimension join (visible as `JOIN "customer"` in the plan) when the task says
"as recorded on the sale line" would steer agents to `.sk`. This is a recurring `.id` vs `.sk`
trap after the model rename (`.id`=business, `.sk`=surrogate).
