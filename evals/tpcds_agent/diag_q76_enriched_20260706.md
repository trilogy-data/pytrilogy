# q76 enriched-leg failure diagnosis (run 20260706-135542_enriched)

## Symptom
`score_query` → `status='fail'`, 100 ref rows vs 100 candidate rows, "result set differs from reference".
Same query passes in the db-only raw-SQL leg (`20260706-135542_sql_bare\workspace\query76.sql`).

## Classification: QUESTION issue (reference carries an implicit filter the task never states)
Same family as the q73 sibling finding: **the reference SQL inner-joins a nullable FK dimension the
task wording never mentions, creating an implicit `sold_date IS NOT NULL` filter.**
Not a framework bug (Trilogy nullable-FK→LEFT-join semantics are intentional and the canonical
preql matches the reference), not a model defect (the `?date.id` nullable annotation is factually
correct), and not an agent error (the candidate faithfully implements the stated question and
matches the reference exactly once the unstated filter is added).

## Error inventory (transcript)
`agent_log.q76.conversation.txt`: **zero** runtime/tool errors — no Binder/Unexpected/parse
failures. The agent wrote one clean union(...) query and validated it. Pure silent-wrong-rows.

## Root cause
- Reference `tests\modeling\tpc_ds_duckdb\query76.sql` (lines 15-19 etc.) uses comma-joins
  `FROM store_sales, item, date_dim WHERE ... ss_sold_date_sk=d_date_sk` — an **INNER** join that
  silently drops every qualifying line whose `*_sold_date_sk` is NULL.
- Task text (`task.q76.txt`) only says: *"For each such line, capture the year, quarter of year,
  and item category"* — nothing requires the sale date to exist. q76 is explicitly ABOUT rows with
  missing FK references, so dropping rows with a *different* missing FK is a non-obvious implicit
  filter.
- The enriched model correctly annotates the FK nullable (`raw/store_sales.preql`:
  `SS_SOLD_DATE_SK: ?date.id  # FK to date the sale occurred (optional in source data)`; same for
  `CS_SOLD_DATE_SK`/`WS_SOLD_DATE_SK`). Trilogy therefore renders
  `LEFT OUTER JOIN "date_dim"` (see cached generated SQL, arm CTEs) while `item` (non-nullable FK)
  renders INNER — matching the reference on item but not on date.
- Null-date rows among the qualifying lines at sf=1: store 64,931 / web 89 / catalog 3,602.
  They aggregate into groups with NULL year/quarter; with `ORDER BY ... NULLS FIRST LIMIT 100`
  they land at the very top of the result, displacing 11 legitimate rows.

## Diff summary (multiset, first 100 rows)
- 11 rows only in candidate: all `('catalog','cs_ship_addr_sk', year=NULL, qoy=NULL, category ∈
  {NULL, Books, Children, Electronics, Home, Jewelry, Men, Music, Shoes, Sports, Women})`
  e.g. `(catalog, cs_ship_addr_sk, NULL, NULL, 'Books', 366, 469236.85)`.
- 11 rows only in reference: the tail rows those NULL groups pushed past the limit
  (e.g. `(catalog, cs_ship_addr_sk, 2000, 2, 'Books', 13, 19685.20)` family).
- Everything else identical (candidate rows 12-100 == reference rows 1-89).

## Verification
1. Harness scoring against fresh DB copy reproduces: `fail / result set differs` (100 vs 100).
2. Canonical `tests\modeling\tpc_ds_duckdb\query76.preql` through the same engine/DB:
   **CANONICAL MATCHES REF: True** — no regression; note the canonical author had to add explicit
   `and ss.date.id is not null` (and per-channel equivalents) to reproduce the reference's implicit
   inner-join drop. This is the smoking gun that the filter is required knowledge absent from the task.
3. Minimal fix test: candidate + `and <channel>.date.id is not null` in each union arm →
   **FIXED CANDIDATE MATCHES REF: True** (exact multiset match, 100/100).

## Why the SQL leg passed
The raw-SQL agent wrote `JOIN date_dim` / `JOIN item` (inner) out of SQL habit — the idiomatic SQL
join accidentally encodes the reference's implicit filter. In Trilogy the nullable annotation makes
the equivalent traversal null-preserving, exposing the task/reference gap.

## Recommendation
Amend the task wording (e.g. "consider only lines with a recorded sale date") or relax the
reference to LEFT JOIN date_dim, mirroring the q73 remediation. No framework or model change needed.
