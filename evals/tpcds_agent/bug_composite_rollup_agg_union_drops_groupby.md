# Bug: composite rollup aggregate over a union model + derived-key projection → "column must appear in GROUP BY"

**Status:** OPEN (found 2026-06-08, enriched eval q80 — exhausted at 75 calls / ~240 events).
**Severity:** high — `generate_sql` succeeds, execution fails. Trilogy emits invalid SQL (violates
the "never emit syntactically invalid SQL" rule). The agent had no way to fix it from the query side
and burned the whole iteration budget rewriting around it.

## Symptom

```
(_duckdb.BinderException) Binder Error: column "…channel_dim_text_id" must appear in the GROUP BY
clause or must be part of an aggregate function.
```
The rollup is generated correctly in one CTE (`GROUP BY ROLLUP (2, 1)`), but the operand of a
**composite** rollup aggregate is computed in a *separate* CTE that re-projects the rollup key as a
plain column **without** carrying the ROLLUP grouping → the final assembly references it ungrouped.

## Minimal trigger (THREE conditions, all required)

1. A **composite** rollup aggregate — `sum(a) - sum(b) by rollup k1, k2` (the two operand sums get
   split across CTEs).
2. **At least one other** rollup aggregate in the same SELECT (forces a multi-CTE join assembly).
3. The projected dimensions are **derived/CASE expressions of the rollup keys**, not raw passthrough
   (e.g. `case when chan='STORE' then 'store channel' … end`, `concat('store', text_id)`).

Drop any one and the SQL is valid:
- raw-key projection (instead of CASE) → OK
- all-simple aggregates (no composite `sum-sum`) → OK
- a single composite aggregate alone → OK

Confirmed against the real `raw.all_sales` model (union-conformed store/catalog/web) AND a synthetic
2-arm union below.

## Self-contained repro

`sales2.preql`:
```trilogy
key chan enum<string>['A','B'];
key oid int;
property <oid, chan>.txt string;
property <oid, chan>.amt float;
property <oid, chan>.prof float;
property <oid, chan>.loss float;

datasource arm_a (raw(''' 'A' '''): chan, oid: oid, txt: txt, amt: amt, prof: prof, loss: loss)
grain (oid, chan) complete where chan = 'A' address arm_a;
datasource arm_b (raw(''' 'B' '''): chan, oid: oid, txt: txt, amt: amt, prof: prof, loss: loss)
grain (oid, chan) complete where chan = 'B' address arm_b;
```
Query (raises BinderException on execute; `generate_sql` succeeds):
```trilogy
import sales2 as s;
select
  case when s.chan = 'A' then 'aa' else 'bb' end as channel,   -- derived projection of rollup key
  concat('x', s.txt) as outlet,                                -- derived projection of rollup key
  sum(s.amt) by rollup s.chan, s.txt as sales,                 -- other rollup aggregate
  sum(s.prof) - sum(coalesce(s.loss, 0)) by rollup s.chan, s.txt as profit  -- composite rollup agg
limit 100;
```
Tables: `arm_a(oid,txt,amt,prof,loss)`, `arm_b(...)`. Raw real-model repro preserved at
`results/20260608-031455_enriched/workspace/` (model + `_worker_0/tpcds.duckdb`); the full q80 agent
thrash is in `agent_log.q80.jsonl` (8× the GROUP BY binder error).

## Root cause (suspected)

The composite aggregate `sum(prof) - sum(coalesce(loss,0)) by rollup …` splits its two operand sums
across CTEs (one lands in the rollup CTE as `_virt_agg_sum_…`, the other in a sibling CTE — `macho`
in the real model). The sibling/assembly CTE re-projects the rollup grouping key as a plain column
but does NOT inherit the `GROUP BY ROLLUP`, so the key is selected outside any aggregate/group. The
derived-key (CASE) projection is what forces the extra assembly layer where the regrouping is lost;
raw passthrough collapses to the single rollup CTE and stays valid.

Same "rollup grouping lost across a split CTE" family as the fixed grouping_id / B3 order-by-rollup
bugs, but here it's driven by **composite-aggregate operand splitting under a union model**, not by
grouping()/grouping_id wrappers.

## Companion (q49, same eval — NOT a framework bug)

q49 exhausted on heterogeneous *exploration thrash*, not one wall: wrong concept names
(`total_return_amt`→`ws.return_amount`), wrong namespace (`wr.item.id`→`ws.item.id`), and
concept/rowset definitions placed below the `where` ("cannot sit inside a query. Move this statement
above your where"). Each error appears once — the agent kept trying different things and never
converged. This is model-discoverability / idiom guidance (cf. the q13/q96 import-description fixes),
not a codegen bug.
