# q70 enriched-leg failure diagnosis (run 20260706-135542_enriched)

## Classification

**QUESTION issue** (task/reference mismatch — the q73 pattern: reference implicitly inner-joins the
nullable FK `ss_store_sk`, and additionally carries a top-5-states `IN` filter, neither of which the
task text mentions). NOT a framework bug (canonical preql still passes byte-for-value; generated SQL
faithfully implements the authored query). NOT an agent error (the agent answered the task wording
exactly, and even called out the NULL-state group as legitimate data).

## Symptom

`report.json`: `{"id": 70, "status": "fail", "ref_rows": 3, "cand_rows": 5, "detail": "result set
differs from reference"}`. Reproduced deterministically via `common.scoring.score_query` against a
copy of `evals/tpcds_agent/.cache/tpcds_sf1.duckdb`.

## Row-level diff

Reference (`tests/modeling/tpc_ds_duckdb/query70.sql`), 3 rows — all totals identical because at sf1
every one of the 12 stores is TN / Williamson County:

| total_sum | s_state | s_county | lochierarchy | rank |
|---|---|---|---|---|
| -444194870.30 | NULL | NULL | 2 | 1 |
| -444194870.30 | TN | NULL | 1 | 1 |
| -444194870.30 | TN | Williamson County | 0 | 1 |

Candidate (`workspace/query70.preql`), 5 rows:

| state | county | total | level | rank |
|---|---|---|---|---|
| NULL | NULL | **-449428398.62** | 2 | 1 |  <- grand total shifted by NULL-store bucket
| NULL | NULL | -5233528.32 | 1 | 1 |       <- EXTRA: NULL-state subtotal
| TN | NULL | -444194870.30 | 1 | **2** |    <- rank 1 -> 2 (NULL bucket outranks it: -5.2M > -444M)
| NULL | NULL | -5233528.32 | 0 | 1 |       <- EXTRA: NULL-state/county detail row
| TN | Williamson County | -444194870.30 | 0 | 1 |

Arithmetic proof: -449,428,398.62 = -444,194,870.30 + -5,233,528.32. Probe:
`store_sales JOIN date_dim ... WHERE d_year=2000 AND ss_store_sk IS NULL` -> **12,810 rows,
sum(ss_net_profit) = -5,233,528.32**. The entire diff is the NULL-`ss_store_sk` sales bucket.

## Root cause

1. `ss_store_sk` is a nullable FK. Per Trilogy's intentional semantics ("joins do NOT drop nulls"),
   the enriched model renders `LEFT OUTER JOIN "store"` (visible in the candidate's generated SQL,
   transcript message 13), preserving 12,810 year-2000 sales with no store. The reference SQL uses a
   comma/INNER join (`s_store_sk = ss_store_sk`), silently discarding them.
2. The reference also restricts to `s_state IN (top-5 states by profit)`. At sf1 with INNER joins
   this is a no-op (only TN exists) — which is exactly why the raw-SQL leg passed without it
   (`results/20260706-135542_sql_schema/workspace/query70.sql` has neither clause and matches). But
   in Trilogy the IN filter is load-bearing: NULL state fails `IN`, so the canonical
   `tests/modeling/tpc_ds_duckdb/query70.preql` (which keeps the top-5 rowset filter,
   `ss.store.state in top_states.ts_state`) drops the NULL bucket and still returns the exact 3
   reference rows (verified this run against the same DB copy).
3. The enriched task text (`task.q70.txt`) mentions **neither** constraint: "For store sales whose
   sold date falls in the year 2000, report total net profit rolled up by store state and store
   county..." — no top-5-states clause, no "sales made at a known store". A faithful Trilogy answer
   therefore keeps the NULL-store bucket and cannot match the reference.

`year = 2000` vs `month_seq 1200-1211` is NOT a factor: verified identical totals
(-444,194,870.30 both ways; d_month_seq for d_year=2000 is exactly 1200..1211).

## Transcript error inventory (agent_log.q70.conversation.txt)

- msg 7: one parse error — agent wrote two-arg `grouping(ss.store.state, ss.store.county)`;
  Trilogy's `grouping()` is single-arg. Self-corrected in one step to
  `grouping(state) + grouping(county)` after consulting `agent-info syntax example rollup`.
- No trip on the 2026-07-06 grammar change (`by rollup` before `having`): the agent placed
  `by rollup (...)` correctly before `order by` on its first structural attempt (no `having` used).
- No framework-error signatures (no binder/catalog/Unexpected errors); query ran cleanly first time
  after the grouping() fix. Agent explicitly noticed the NULL-state group: "there are store sales in
  year 2000 where the store state is NULL. That's fine, the data is what it is."

## Recommended fix

Amend the enriched question text for q70 to carry the reference's implicit constraints, e.g. add
"consider only sales attributable to a store, and only stores in the top 5 states by total net
profit over the period" (the canonical .preql's top_states rowset shows the intended Trilogy shape).
Alternative: score the enriched leg against a NULL-tolerant reference, but that diverges from the
canonical .sql and hides the same trap for other rollup questions over nullable FKs.
