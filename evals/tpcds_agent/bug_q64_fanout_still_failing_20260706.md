# q64 FAIL / token sink (865k tok, run `20260706-222300`) — NOT a framework bug

## Verdict
**AGENT modeling error, proven by counterfactual. No framework bug, no model defect, no
regression of a prior fix.** The engine compiled every construct correctly; the correct idiom
works and reproduces the reference exactly. Two *independent* agent errors compound here; one is
new this run (the 100-row fanout), one is the previously-documented value slip.

## Symptom
`report.json` q64: `status=fail, ref_rows=2, cand_rows=100` (`cand_rows=100` = the `limit 100` cap;
true count is **247**). Reference `PRAGMA tpcds(64)`/`query64.sql` = **2 rows** (re-confirmed on this
workspace's `tpcds.duckdb`). A ~50–120× fanout, silent (no error).

## Root cause #1 (NEW, dominant — the fanout)
`workspace/query64.preql` rowset `matched_lines` (L68–71) filters store_lines with **two
INDEPENDENT `in` semijoins**:
```
where store_lines.tkt in store_ret.ret_tkt
  and store_lines.item_id in store_ret.ret_item_id
```
This admits any line whose ticket was returned (for *some* item) **and** whose item was returned
(on *some* ticket) — not lines where the *same* `(item,ticket)` pair was returned. The reference
correlates them (`ss_item_sk=sr_item_sk AND ss_ticket_number=sr_ticket_number`).

Magnitude (measured on `tpcds.duckdb`, base tables):
- correlated `(item,ticket)` return match: **287,867** lines
- candidate's independent `ticket∈R AND item∈R`: **2,083,051** lines — **7.2× looser**.

Those extra lines survive into `agg_sales` (grain = product/store/all-addresses/first-sales-yr/
first-ship-yr = same grain as reference `cross_sales`), giving ~906 aggregate rows vs the
reference's handful. The final year self-pair `union join y1999.item_id=y2000.item_id and
store_name and store_zip` (keyed on only 3 of the grain's ~14 dims — exactly as the reference
self-joins) then partial-cross-products those extra rows → **247**.

### Counterfactual (single-line fix)
Replace the two independent `in`s with one **correlated composite tuple membership**:
```
where (store_lines.item_id, store_lines.tkt) in (store_ret.ret_item_id, store_ret.ret_tkt)
```
→ candidate drops **247 → 2 rows**, nothing else changed. The framework supports and correctly
compiles composite-tuple membership (q87 family); the agent simply didn't use it.

## Root cause #2 (pre-existing value slip — orthogonal, would fail even at 2 rows)
Candidate sums `ss.ext_wholesale_cost`/`ss.ext_list_price` (line-extended) (L55–56) where the
reference sums per-unit `ss_wholesale_cost`/`ss_list_price`. Same error diagnosed in
`diag_q64_enriched_20260706.md` and `bug_q64_customer_address_crossrowset_enrichment_fanout.md`.
Applying **both** fixes (composite membership + `ext_*`→per-unit) makes the candidate reproduce the
reference **exactly** (2 rows: `…95.24/140.95/1488.38…52.89/97.31/338.38` and
`…7.73/14.14/0.00…94.87/166.02/0.00`). Coupon (no `ext_` variant) already matched, corroborating.

## Proof there is NO framework fanout
Every intermediate is 1:1 with the data (no join multiplies rows):
| stage | rows |
|---|---|
| `store_lines` distinct(item,tkt) | 1255 |
| `store_lines` row-grain (item,tkt,+dims) | 1255 (= distinct → no join fanout) |
| `matched_lines` distinct(item,tkt) | 907 |
| `agg_sales` (full grain) | 906 (≈ matched_lines → 1 group/line) |
| `y1999` full grain / `y2000` full grain | 216 / (small) |
| final join (as-is) | 247 |
| final join, composite-membership fix | **2** |

The generated final SQL (`INNER JOIN yellow on item=item AND store=store AND zip=zip`, L452 of the
dump) is the correct 3-key join. The one odd line — `LEFT OUTER JOIN "vast" on 1=1` (L453) — is a
harmless cross-join onto a **single-row** constant CTE `vast = SELECT 1999 AS year_1999, 2000 AS
year_2000` (the two literal `1999 as year_1999`/`2000 as year_2000` output columns); it multiplies
by 1 and is not the fanout.

## Trigger matrix (final self-pair, no-limit)
| variant | rows |
|---|---|
| as-is (`union join`, independent membership) | 247 |
| final `union join` → `full join` | 247 |
| final `union join` → `left join` | 247 |
| composite-tuple membership (only change) | **2** |
| composite membership + `ext_*`→per-unit | **2, exact reference match** |

Join **type** is irrelevant (union/full/left all 247) — confirming the fanout is grain/membership,
not a coalescing-join defect.

## Prior-report status on the CURRENT engine
- `bug_q64_subset_join_subordinate_key_lost_across_rowset_boundary.md` — **still FIXED**; no
  `Missing source reference` sentinel anywhere in `agent_log.q64.conversation.txt`; the agent freely
  uses `union join` for both the catalog qualify and the year self-pair and it renders/executes.
- `bug_q64_customer_address_crossrowset_enrichment_fanout.md` (framework bug #2, transitive
  `customer.address` enrichment fan-out) — **does NOT apply**: it only bites the *canonical*
  "aggregate id keys, enrich text later via cross-rowset join" strategy. This agent enriches all
  descriptive text INSIDE the row-grain `store_lines` rowset (grouped in `agg_sales`), so there is
  no post-hoc enrichment join to fan out. Same dodge as every prior run.
- The `20260706` "unexpected error" regression reports — no `Unexpected error`/`Binder` signature
  in this run's log; not applicable.

## Classification
- Fanout (247/100 rows): **AGENT error** — independent semijoins instead of correlated
  `(item,ticket)` membership. Framework compiled both; correct idiom → 2 rows.
- Value inflation: **AGENT error** — `ext_*` vs per-unit columns (pre-documented).
- Token sink (865k): the agent could not self-validate (no reference) and its query returned a
  plausible-looking 100-row set, so it churned. No framework wall.

## Guidance-defect candidate (optional, not a bug)
"Customers who bought the SAME item on a returned ticket" is a *correlated-pair* match. The agent
reached for two `in`s (the intuitive but wrong decomposition). A syntax-example / prompt note that
same-pair matching needs composite-tuple membership `(a,b) in (x,y)` (not two independent `in`s)
would likely have averted both the fanout and the churn. This is language ergonomics, not a defect.

## Files
- Candidate: `evals/tpcds_agent/results/20260706-222300/workspace/query64.preql` (L68–71 membership;
  L55–56 ext_ columns)
- Reference: `tests/modeling/tpc_ds_duckdb/query64.{sql,preql}` (both → 2 rows on this DB)
- Repro scripts: `scratchpad/q64probe{2,4,5,6}.py`
