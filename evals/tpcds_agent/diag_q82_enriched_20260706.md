# Diagnosis: q82 FAIL in enriched leg (20260706-135542_enriched)

## Symptom
Scorer: `fail`, ref_rows=2, cand_rows=4, "result set differs from reference" (reproduced
against a copy of `.cache/tpcds_sf1.duckdb`: `QueryResult(id=82, status='fail', ref_rows=2, cand_rows=4)`).
Raw-SQL leg passes (`results\20260706-135542_sql_bare\workspace\query82.sql` uses per-sk EXISTS probes).

## Classification: QUESTION issue (implicit SCD-version co-occurrence in the reference), with the
agent's `text_id`-level join being the documented-correct per-item reading. NOT a framework bug.

## Error inventory (transcript `agent_log.q82.conversation.txt`)
One error total, at ~line 1329:

```
Resolution error in query82.preql: Discovery error: cannot merge all concepts into one
connected query (statement at line 4). The requested concepts split into 2 disconnected
subgraphs: {inv.date.date, inv.item.text_id, inv.quantity_on_hand}; {current_price,
description, item_code, ss.item.current_price, ss.item.manufacturer_id, ss.item.text_id}.
Are you missing a join or merge statement to relate them?
```

Trigger (attempt 1, msg 9): cross-model equality **in a WHERE clause** —
`where ... inv.item.text_id = ss.item.text_id` — between two separately-imported models
(`raw.store_sales as ss`, `raw.inventory as inv`). Reproduced verbatim via `generate_sql`
(`DisconnectedConceptsException`). Verdict: **legitimate disconnected-import guardrail**, same
family as the two seen in sibling probes. WHERE equality is a filter, not a join declaration by
design; the message names both subgraphs and points at join/merge; the agent recovered in one
step (fetched `agent-info syntax example scoped-join`, rewrote with rowsets + scoped join).
Not framework, not the cause of the wrong answer.

## Final candidate and the 4-vs-2 diff
`workspace/query82.preql`: rowset `sales_items` (item filtered on price 62–92 + mfg in
(129,270,821,423), keyed `ss.item.text_id`), rowset `inv_items` (inventory snapshot 100–500
units in 2000-05-25..2000-07-24, keyed `inv.item.text_id`), then
`union join sales_items.item_code = inv_items.item_code` + both-sides `is not null` (intersection).

Candidate rows (4): AAAAAAAAAEBCAAAA (63.76), AAAAAAAAECMCAAAA (67.28),
AAAAAAAALIHCAAAA (86.90), AAAAAAAAMGFDAAAA (70.59).
Reference rows (2): AAAAAAAAECMCAAAA, AAAAAAAALIHCAAAA.

Extra rows are **SCD siblings** — `item` is an SCD with several `i_item_sk` per `i_item_id`.
Per-version facts for the extras (item_id, sk, price, mfg, inventory-qualified-per-sk):

```
AAAAAAAAAEBCAAAA  8512  0.83  82   False
AAAAAAAAAEBCAAAA  8513  4.76  82   True   <- inventory qualifies HERE
AAAAAAAAAEBCAAAA  8514 63.76 129   False  <- price/mfg qualify HERE
AAAAAAAAMGFDAAAA 13676  9.58 270   True   <- inventory qualifies HERE
AAAAAAAAMGFDAAAA 13677 70.59 270   False  <- price qualifies HERE
```

The reference (`tests\modeling\tpc_ds_duckdb\query82.sql`) conjoins ALL predicates on ONE item
row (`inv_item_sk = i_item_sk AND i_current_price BETWEEN ... AND ss_item_sk = i_item_sk`), i.e.
the price-qualified SCD **version** must itself have the qualifying inventory. The candidate
intersects the two qualification sets at the **business-item** (`text_id`) level, so a sibling
version's inventory snapshot qualifies the item. All 4 candidate items DO appear in store_sales,
so the store-sales leg is not the differentiator.

## Counterfactual proof (flips fail -> pass)
Identical query with the ONLY change being the join/filters keyed on the surrogate
(`ss.item.id as item_sk` / `inv.item.id as item_sk`, `union join sales_items.item_sk =
inv_items.item_sk`) returns exactly the 2 reference rows. One-line identity change = pass.

## Why QUESTION issue and not AGENT error
- Task wording: "items whose current price is between 62 and 92 ... that had at least one
  inventory snapshot ... and that appear in store sales" — identity is "item code"; nothing says
  all conditions must hold on the SAME SCD version row. A business item has one *current* price;
  the extra items' current-priced versions genuinely satisfy price+mfg, and the item (an older
  version sk) genuinely had a qualifying snapshot. 4 rows is the faithful business-level answer.
- The enriched model actively directs this reading — `workspace/raw/item.preql:7`:
  "Surrogate key (one row per SCD version of an item; SEVERAL ids share one text_id). Do NOT
  group/report per-item by this — it splits an item across versions. Use text_id." and :10
  "use this for per-item results, not the surrogate id".
- The reference's per-sk conjunction is a TPC-DS template artifact (it even applies
  "current price" to non-current versions). Same family as q73/q76/q70 implicit-filter findings:
  reference imposes row semantics the task never states. (Inverse of q60, where the OUTPUT
  identifier itself was wrong — here output columns are correct.)

## Canonical check
`tests\modeling\tpc_ds_duckdb\query82.preql` (joins on `.id`, all filters in the inventory
model) generates and matches `query82.sql` exactly (2 rows) — canonical healthy.

## Latent (non-binding) observations
1. Neither the candidate NOR the canonical generated SQL scans `store_sales` at all. The
   candidate's `sales_items` rowset compiles to a bare `item` scan (dim attrs via the `ss.`
   namespace do not imply fact presence); the canonical's `store_sales.item.id is not null`
   collapses through the subset join into a vacuous `I_ITEM_SK is not null` on the
   inventory-side item join. Non-binding at sf=1: the per-sk qualified set WITHOUT the
   store-sales condition equals the reference set (verified). Would bite at other scale
   factors / data; expressing fact presence needs a fact-derived concept
   (e.g. `count(store_sales.id) > 0`), not a dim-key null test.
2. Framework behaved correctly throughout: the one rejection was a clear guardrail, the scoped
   union join + presence probes (`_virt_presence_*`) rendered valid SQL returning exactly what
   was authored.
