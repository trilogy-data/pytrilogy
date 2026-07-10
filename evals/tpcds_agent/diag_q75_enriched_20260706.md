# q75 enriched-leg failure diagnosis — run 20260706-135542_enriched

## Classification: AGENT error (proven; two independent semantic mistakes). NOT framework, NOT model, NOT question.

## Symptom
`QueryResult(id=75, status='fail', ref_rows=100, cand_rows=100, detail='result set differs from reference')`.
Re-scored against a copy of `.cache/tpcds_sf1.duckdb`: 40 of 100 rows differ from
`tests/modeling/tpc_ds_duckdb/query75.sql`. No errors in the transcript
(`agent_log.q75.conversation.txt`) — the query ran cleanly first try and produced
silently wrong sums; ordering keys (qty_diff/amt_diff) shift so wrong rows enter the top-100.

Example diff (year pair 2001/2002, brand 1001001, class 5, manuf 172):
- REF : curr_cnt 4567, cnt_diff -1165, amt_diff -86618.26
- CAND: curr_cnt 4544, cnt_diff -1188, amt_diff -86618.26  (qty moved, amt identical → NULL-qty lines)

## The two agent mistakes (workspace/query75.preql)

### 1. Skipped the required duplicate-line removal
Question: "Combine all three channels' per-line records into one single set, **remove
duplicate lines from that combined set**, then aggregate…" — this is TPC-DS q75's
`UNION` (distinct) over the 7-tuple (year, brand_id, class_id, category_id,
manufact_id, net_qty, net_amt).

Candidate comment (query75.preql:4-5): "*all_sales already combines all three channels;
its grain <item.id, channel, order_id> means no duplicates*". Transcript lines 1422-1426
show the agent explicitly considering dedup and reasoning it away via the model grain.
Wrong altitude: the dedup applies to the **projected record** after order/channel identity
is dropped, where duplicates absolutely exist (Books, all years: 445,282 raw line records
→ 431,675 distinct 7-tuples). The agent-info guidance in its own context even documents the
`dedup-before-aggregate` rowset pattern (transcript ~line 881-883) — and the canonical
`tests/modeling/tpc_ds_duckdb/query75.preql` implements exactly that (`rowset deduped <- …`).

### 2. Coalesced the SALES measures to 0 (unrequested)
Question: net qty = "sold quantity minus any matched return quantity, **with missing
returns treated as zero**" — coalesce the RETURN side only. Reference keeps the sale side
bare (`cs_quantity - COALESCE(cr_return_quantity,0)`), so a NULL-quantity sale line yields
NULL and drops out of SUM.

Candidate (query75.preql:6-7): `coalesce(s.quantity, 0) - coalesce(s.return_quantity, 0)`
(same for ext_sales_price/return_amount). NULL-qty sale lines with a matched return now
contribute a **negative** net qty instead of nothing. Store channel alone has 2,445
NULL-qty / 2,445 NULL-price Books lines in 2001-2002 (catalog 132/128, web 3/3).
This is the DOMINANT contributor (see matrix). The model doc itself warns against
uninstructed coalescing (all_sales.preql:52 GOTCHA on return_quantity).

## Proof: decomposition matrix (SQL variants of the reference, run on the same DB)
| variant | UNION mode | sales-side coalesce | == ref | == candidate | sym-diff vs ref |
|---|---|---|---|---|---|
| A | UNION (dedup) | no  | **yes** | no | 0 |
| B | UNION ALL     | no  | no | no | 10 |
| C | UNION ALL     | yes | no | **yes (exact)** | 80 |
| D | UNION (dedup) | yes | no | no | 78 |

Variant C reproduces the candidate's 100 rows **exactly** → the framework rendered the
authored Trilogy faithfully; the entire failure is the two authored semantic deltas.
Mistake 2 (coalesce) dominates; mistake 1 (no dedup) accounts for the residual 10.

## Rule-outs
- FRAMEWORK: canonical `query75.preql` (rowset-dedup + left join + having form, incl. the
  previously-fixed filter+window construct family) regenerated and re-run → 100 rows,
  byte-set identical to `query75.sql`. No recurrence.
- MODEL: `raw.all_sales` returns-unification produced correct per-line
  `coalesce(return_*, 0)` semantics (variant-C exact match proves the FULL-join/pseudonym
  plumbing is right). Store `is_returned` open defect NOT implicated — candidate never
  touches `is_returned`; net measures avoid the INNER-collapse path. No surrogate-vs-text
  id issue (all four output ids are the numeric `*_id` properties, matching ref columns).
- QUESTION: the same task text in the SQL-bare leg produced a PASSING answer
  (`results/20260706-135542_sql_bare/workspace/query75.sql`) that gets BOTH points right:
  `SELECT DISTINCT year, brand_id, … net_qty, net_amt` (its comment: "Remove duplicate
  lines from the combined set") and bare `ss_quantity - COALESCE(sr_return_quantity, 0)`.
  Wording was sufficient.

## Error inventory (transcript)
None. Zero tool errors, zero planner/binder errors, single clean `trilogy run`. The
`union join` on the four attribute keys + both-sides-not-null filter correctly emulated
the inner join (verified in generated SQL, transcript lines ~1793-1820).

## Takeaway for prompting/guidance
Enriched-model agents rationalize "remove duplicate lines" as a no-op because the model
grain is unique — but q75's dedup is over the projected measure tuple. The
`dedup-before-aggregate` agent-info example exists but did not fire. Also worth a guidance
line: "treat missing X as zero" means coalesce ONLY X; never coalesce sale-side measures
unprompted (NULL quantities are meant to drop out of sums).
