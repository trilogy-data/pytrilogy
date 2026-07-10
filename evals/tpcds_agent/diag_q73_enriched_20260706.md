# Diagnosis: q73 FAIL in enriched leg, PASS in raw-SQL legs (run 20260706-135542)

## Classification

**QUESTION issue** (under-specified: the reference answer implicitly requires the
sale's customer to be non-null via SQL inner-join semantics; the task prompt never
states that restriction). No framework bug, no model defect, no agent error.

## Symptom

- Scorer: `QueryResult(id=73, status='fail', ref_rows=1, cand_rows=82,
  detail='result set differs from reference')` (re-verified on a copy of
  `.cache/tpcds_sf1.duckdb`).
- Reference (`tests\modeling\tpc_ds_duckdb\query73.sql`) returns exactly 1 row:
  `('Robinson', 'Maribel', 'Miss', 'N', 153104, 5)`.
- Candidate returns 82 rows: that same row **plus 81 rows where all four customer
  columns are NULL** (tickets sold with no recorded customer, i.e. anonymous sales).
  The one non-null candidate row is byte-identical to the reference row.

## Agent-attempt error inventory (transcript `agent_log.q73.conversation.txt`, 2084 lines)

1. `Syntax [202]: Missing closing semicolon` — the agent omitted the trailing `;`
   on its first `trilogy file write` (msg 15). Fixed on the immediate retry.
2. Nothing else. No framework errors, no binder/catalog errors, no wrong-result
   loop. The query ran cleanly on the second attempt (82 rows) and the agent
   returned control. The agent explicitly noticed the NULL-customer rows and
   reasoned (msg 20): "customer is optional on store_sales, the join is LEFT
   OUTER. This is expected behavior - the query matches the specification exactly."

## Candidate vs reference diff

Sole divergence: treatment of sales with `SS_CUSTOMER_SK IS NULL`.

- Reference SQL joins `customer` with `WHERE ss_customer_sk = c_customer_sk`
  (comma-join = INNER), silently dropping all 81 anonymous-sale tickets.
- The passing SQL-bare candidate (`results\20260706-135542_sql_bare\workspace\query73.sql`)
  wrote `JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk` — the *default*
  SQL join, which coincidentally matches the reference's implicit filter.
- The enriched candidate (`workspace\query73.preql`) authored the question
  faithfully; Trilogy correctly rendered the nullable-FK customer enrichment as
  `LEFT OUTER JOIN customer` (nullable FK -> null-preserving join is intentional
  language semantics: "Joins do NOT drop nulls"), keeping the 81 anonymous rows.
- Everything else is equivalent: filters identical; `HD_DEP_COUNT / HD_VEHICLE_COUNT`
  is float division in DuckDB so the missing `*1.000`/CASE guard is immaterial
  (vehicle_count > 0 already holds); count grain (ticket x customer attrs) equals
  the reference's (ticket, customer_sk) grain here.

## Canonical still passes

`tests\modeling\tpc_ds_duckdb\query73.preql` built through the same engine on the
DB copy returns exactly the reference row (MATCH: True). No framework regression.

Crucially, the canonical preql only matches because it carries an **explicit**
`where store_sales.customer.id is not null` (lines 24-25) — direct proof that in
Trilogy this restriction must be authored, and it is nowhere in the task prompt.

## Root cause

`task.q73.txt` says "List the customer's last name, first name, ... count the
line items for each (ticket, customer) combination" but never states that sales
without a recorded customer are excluded. The reference encodes that exclusion
implicitly through INNER-join semantics. In the raw-SQL legs the default `JOIN`
reproduces it for free; in the enriched leg the model (correctly) declares
`SS_CUSTEMER_SK: ?customer.id  # optional — anonymous sales allowed`
(`workspace\raw\store_sales.preql:57`) and Trilogy (correctly) preserves the NULL
rows, so a faithful reading of the question yields 81 extra rows. The task
preamble ("don't add, drop, or loosen filters just to force rows") further
discourages the agent from adding an unstated `is not null` filter.

## Recommended fixes

1. QUESTION: amend the q73 prompt with e.g. "consider only sales with an
   identified (non-anonymous) customer" — mirroring the canonical preql's
   `customer.id is not null`.
2. Optional MODEL hardening: extend the `SS_CUSTOMER_SK` guidance comment to note
   that per-customer reports typically want `customer.id is not null`, since the
   TPC-DS references always inner-join customer.
3. Sweep other tasks whose references inner-join a nullable FK
   (customer/hdemo/cdemo/addr) for the same silent implicit-filter mismatch.
