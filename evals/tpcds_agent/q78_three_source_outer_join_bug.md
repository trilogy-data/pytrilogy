# BUG: 3-source scoped OUTER join — anchor re-joined on a MEASURE column, key dropped → silent NULL anchor columns

## Severity
High — produces **silently wrong results** (no error). A LEFT join whose anchor side comes back all-NULL. Surfaced as the q78 agent thrash (66 iterations, 3.37M tokens, the single most expensive query in the enriched suite) because the agent could not understand why its "store" columns were NULL.

## Symptom
A scoped LEFT join from one anchor rowset to **two or more** other rowsets returns rows where the **anchor's own columns are NULL** (specifically the key columns not shared in the broken re-join, and the anchor's measures). The anchor side of a LEFT join must always be preserved/non-null on returned rows.

## Minimal repro
Engine built from `evals/tpcds_agent/results/20260623-145720/workspace` (enriched models in `raw/`):

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with store_agg as
where ss.date.year = 2000 and ss.customer.id is not null
select ss.item.id as item_id, ss.customer.id as cust_id, sum(ss.quantity) as store_qty;

with catalog_agg as
where cs.date.year = 2000 and cs.bill_customer.id is not null
select cs.item.id as item_id, cs.bill_customer.id as cust_id, sum(cs.quantity) as cat_qty;

with web_agg as
where ws.date.year = 2000 and ws.billing_customer.id is not null
select ws.item.id as item_id, ws.billing_customer.id as cust_id, sum(ws.quantity) as web_qty;

select
    store_agg.item_id, store_agg.cust_id, store_agg.store_qty,
    coalesce(catalog_agg.cat_qty,0) + coalesce(web_agg.web_qty,0) as other_qty
left join store_agg.item_id = catalog_agg.item_id = web_agg.item_id
left join store_agg.cust_id = catalog_agg.cust_id = web_agg.cust_id
having coalesce(catalog_agg.cat_qty,0) + coalesce(web_agg.web_qty,0) > 0
limit 15;
```

Result: every row has `store_agg.cust_id = NULL` and `store_agg.store_qty = NULL` (the anchor), while the catalog/web-derived `other_qty` is populated.

## Scope (which forms trigger it)
- **2-source** scoped LEFT join (store→catalog only): **CORRECT.** Anchor preserved, `coalesce(catalog.key, store.key)` projection, clean `LEFT OUTER JOIN`.
- **3-source LEFT, chained** (`a = b = c`): **BROKEN** (above).
- **3-source LEFT, star** (`store→cat` and `store→web` as separate 2-way clauses, no `a=b=c`): **BROKEN identically** — so the trigger is **3+ OUTER sources**, NOT the `a=b=c` chaining.
- **3-source INNER**: SQL is **clean** (`web INNER JOIN store …, INNER JOIN catalog …` on full key), returns 0 rows — plausibly a genuinely empty 3-channel (item,customer) intersection at sf=1. The INNER path is unaffected.

So the defect is specific to the **OUTER (LEFT/FULL) 3+-source consolidation**.

## The malformed SQL (chained-LEFT repro)
```sql
-- consolidation degrades the LEFT joins into FULL joins:
FROM "late"  -- web_agg
  FULL JOIN "cooperative"  -- catalog_agg
    on late.web_cust = cooperative.cat_cust AND late.web_item = cooperative.cat_item AND late.web_year = cooperative.cat_year
  FULL JOIN "vacuous"  -- store_agg (the intended anchor!)
    on cooperative.* = vacuous.* AND late.* = vacuous.*   -- store demoted to a FULL-joined side
-- ...producing "macho", then a SPURIOUS re-join of the anchor:
SELECT macho.store_year, macho.store_item, vacuous.store_cust, macho.store_qty, macho.other_qty
FROM "macho"
  LEFT OUTER JOIN "vacuous"   -- store_agg AGAIN
    on macho.store_item = vacuous.store_item
   AND macho.store_qty  = vacuous.store_qty    -- <<< JOINS ON A SUM MEASURE
   AND macho.store_year = vacuous.store_year   -- <<< cust_id DROPPED from the key
WHERE macho.other_qty > 0
```

Two distinct defects in the OUTER consolidation:
1. **LEFT anchor demoted to FULL.** The intended preserved side (`store_agg`) is pulled into the FULL-join chain instead of anchoring it.
2. **Spurious anchor re-join keyed on a measure + partial key.** The final `LEFT OUTER JOIN vacuous` re-attaches `store_agg` to recover its columns, but builds the ON-clause from `{item_id, store_qty(MEASURE), year}` and **omits `cust_id`**. Joining on the aggregated `store_qty` value can't match → `store_cust_id` and `store_qty` resolve to NULL.

## Suspected code area
The 2026-06-22 scoped-OUTER rework (see memory `project_outer_scoped_join_two_rowset_distinct_base.md`):
- `trilogy/core/processing/nodes/merge_node.py` — `_key_equivalence_classes` + FULL-key dedup (builds the consolidated OUTER join + the final dedup re-join).
- Whatever selects the dedup re-join key is including the rowset's **measure** outputs and dropping a key column when there are ≥3 sources. It also is not preserving LEFT-anchor semantics for 3+ sources.

The 2-source path was the focus of that work and is correct; the ≥3-source case looks uncovered/regressed.

## Reproduction harness
`/scratchpad/repro78b.py` (chained), `repro78c.py` (star), `repro78d.py` (INNER), `repro78.py` (2-source, correct). Pattern:
```python
import sys; sys.path.insert(0,'evals'); from common import scoring
ws=Path('evals/tpcds_agent/results/20260623-145720/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
sql=eng.generate_sql(body)[-1]; rows=list(eng.execute_raw_sql(sql).fetchall())
```

## Expected fix
3+-source scoped OUTER join should: keep the declared anchor as the preserved (LEFT) side, and the final projection of anchor columns must come through the join keyed on the **full key set** (no measure columns, no dropped keys) — or, if consolidation genuinely can't preserve anchor identity, raise rather than emit silently-wrong SQL.

## Note for agent guidance (separate from the bug)
q78's intent (store rows, with summed catalog+web "other" quantities) is a store-anchored LEFT join to two aggregates. Even once the bug is fixed, this is an awkward idiom; `all_sales` with a channel filter may be the better-modeled path. But the framework must not emit wrong SQL here regardless.
