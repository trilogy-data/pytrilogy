# Diagnosis: q64 FAIL in enriched leg (run 20260706-135542_enriched)

## Classification: AGENT error (proven by counterfactual). No framework bug, no model defect, no question defect.

## Symptom
`report.json` q64: status=fail, ref_rows=2, cand_rows=2, "result set differs from reference".
Scored against a copy of `.cache/tpcds_sf1.duckdb` via `evals/common/scoring.py` — reproduced exactly.

Row-level diff: both rows match the reference on **every** grouping key, both years, both counts
(1/1), and both coupon sums (1488.38 / 338.38 and 0.00 / 0.00). Only the four wholesale-cost and
list-price sums differ — each by an **exact integer factor**:

| row | col | ref | cand | ratio |
|---|---|---|---|---|
| eing | ws_1999 | 95.24 | 1904.80 | ×20 |
| eing | lp_1999 | 140.95 | 2819.00 | ×20 |
| eing | ws_2000 | 52.89 | 317.34 | ×6 |
| eing | lp_2000 | 97.31 | 583.86 | ×6 |
| ese | ws_1999 | 7.73 | 262.82 | ×34 |
| ese | lp_1999 | 14.14 | 480.76 | ×34 |
| ese | ws_2000 | 94.87 | 5312.72 | ×56 |
| ese | lp_2000 | 166.02 | 9297.12 | ×56 |

The ratios are the group quantities: the candidate summed the **line-extended** measures.

## Root cause
`workspace/query64.preql` lines 41-42 (and 73-74) use
`sum(ss.ext_wholesale_cost)` / `sum(ss.ext_list_price)`.
The task says "count the number of sale lines and **sum wholesale cost, list price, and coupon
amount**" (plain, per-unit) and the reference (`tests/modeling/tpc_ds_duckdb/query64.sql` lines
28-29) sums `ss_wholesale_cost` / `ss_list_price`. The same task sentence uses "cumulative
**extended** list price" for the catalog gate, so the per-unit vs extended contrast is explicit
in the question wording.

The model documents both concepts unambiguously (`workspace/raw/store_sales.preql`):
line 40 `wholesale_cost … # Per-unit wholesale acquisition cost.` vs
line 32 `ext_wholesale_cost … # Line-extended wholesale cost.` (same for list_price, lines 30/33).
The agent read both descriptions (transcript ~lines 1171, 1199) and picked the ext_ variants in
its first draft (~line 2879) with zero deliberation, carrying them through unchanged. Coupon has
no ext_ variant, which is why the coupon sums match — corroborating the diagnosis.

**Counterfactual proof**: replacing only `sum(ss.ext_wholesale_cost)`→`sum(ss.wholesale_cost)` and
`sum(ss.ext_list_price)`→`sum(ss.list_price)` in the candidate (nothing else) yields an exact
multiset match with the reference (2 rows, all 21 columns).

## Error inventory (mid-flight "cannot merge all", transcript ~2712 and ~3003)
Both reproduce identically at current head (`DisconnectedConceptsException`) and are **by-design,
correct** rejections, not framework bugs:
1. (~2712) `select cs.item.id, sum(cs.ext_list_price)…, sum(cr.refunded_cash+…)…` over two
   *independent* imports (`raw/catalog_sales:cs`, `raw/catalog_returns:cr`) with no join/merge —
   `cs.item.id` and `cr.item.id` are unrelated concepts. Error message suggested join/merge;
   agent recovered on the next attempt with `union join`.
2. (~3003) same shape via two `auto` aggregates compared in a WHERE — same legitimate disconnect,
   same clean recovery (rowset + `union join`).
Neither is a recurrence of the fixed q64 issues (transitive-key fan-out, coalescing subordinate
keys, find_nullable_concepts KeyError) — none of those signatures appear in the transcript.

## Deltas proven immaterial (candidate vs reference, exact match after the counterfactual)
- Catalog gate over `union join` (all cs lines) vs reference's inner join (matched lines only) —
  the passing sql_bare leg (`results/20260706-135542_sql_bare/workspace/query64.sql`) also sums
  over all catalog_sales lines and passed.
- Self-pairing on `ss.item.text_id` vs reference `i_item_sk` (q60 pattern checked — immaterial here).
- Candidate omits reference's inner joins to promotion / household_demographics / income_band on
  unmentioned nullable FKs (q73 pattern checked — sql_bare leg also omits them and passed at sf=1).

## Canonical sanity (step 3)
`tests/modeling/tpc_ds_duckdb/query64.preql` executed against the same DB copy (memory-catalog
harness, views over attached file) returns 2 rows and **matches** `query64.sql` exactly. No
canonical regression.
