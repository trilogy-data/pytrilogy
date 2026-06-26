# Bug: scoped join onto a rowset + a second composite fact join → invalid SQL referencing a stale CTE alias (q64)

**Status:** OPEN — confirmed, 100% deterministic with the exact query (below). Minimal trigger not
fully isolated (needs the multi-dimension-join complexity); load-bearing element identified.
**Surfaced by:** TPC-DS q64 enriched eval (run `20260626-135713`). The agent flagged it itself:
*"likely a Trilogy bug or the catalog_item_agg rowset has an aliasing problem."*
**Severity:** HIGH — generates SQL DuckDB rejects.

## Symptom

```
(_duckdb.BinderException) Binder Error: Referenced table "cooperative" not found!
Candidate tables: "late"
LINE 86:  "cooperative"."item_id" as "catalog_item_agg_item_id",
WITH cheerful as (
SELECT "cs_item_items"."I_ITEM_ID" as "_catalog_item_agg_item_id",
       sum("cs_catalog_sales"."CS_EXT_LIST_PRICE") as "_catalog_item_agg_cat_ex..." ...
```

The rowset `catalog_item_agg` is materialized as a CTE (`cheerful`) projecting
`_catalog_item_agg_item_id` (leading underscore), but a downstream projection references
`"cooperative"."item_id"` — a **different, non-existent CTE alias with the wrong column name**. So
the rowset CTE is referenced inconsistently (two aliases / two column names) when the query has
multiple scoped joins.

## Reproducing query (exact, 100% deterministic)

```trilogy
import raw.store_sales as ss;
import raw.physical_returns as pr;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

with catalog_item_agg as
select
  cs.item.text_id as item_id,
  sum(cs.ext_list_price) as cat_ext_list_price,
  sum(cr.refunded_cash + cr.reversed_charge + cr.store_credit) as cat_refund
left join cs.order_number = cr.order_number and cs.item.id = cr.item.id
;

where ss.item.text_id in catalog_item_agg.item_id
  and catalog_item_agg.cat_ext_list_price > 2 * catalog_item_agg.cat_refund
  and ss.item.color in ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')
  and ss.item.current_price between 65 and 74
  and ss.customer_demographic.marital_status != ss.customer.demographics.marital_status
  and ss.date.year in (1999, 2000)
select
  ss.item.product_name, ss.item.text_id as item_id,
  ss.store.name as store_name, ss.store.zip as store_zip,
  ss.sale_address.street_number as sale_street_number, ss.sale_address.street_name as sale_street_name,
  ss.sale_address.city as sale_city, ss.sale_address.zip as sale_zip,
  ss.customer.address.street_number as cust_street_number, ss.customer.address.street_name as cust_street_name,
  ss.customer.address.city as cust_city, ss.customer.address.zip as cust_zip,
  ss.date.year as sale_year,
  ss.customer.first_sales_date.year as first_sales_year,
  ss.customer.first_shipto_date.year as first_shipto_year,
  count(ss.line_item) as line_count,
  sum(ss.ext_wholesale_cost) as wholesale_cost_sum,
  sum(ss.ext_list_price) as list_price_sum,
  sum(ss.coupon_amt) as coupon_amt_sum
inner join ss.ticket_number = pr.ticket_number and ss.item.id = pr.item.id
inner join ss.item.text_id = catalog_item_agg.item_id
order by ss.item.product_name, ss.store.name
limit 100;
```

## Bisection (what's load-bearing vs not)

- **Removing the second scoped join** `inner join ss.ticket_number = pr.ticket_number and ss.item.id = pr.item.id` (+ its `pr` import) → **PASSES.** So the **composite multi-key join to a second fact (`pr`), coexisting with the rowset scoped join, is required.**
- Removing the marital-status filter, the `customer.address.*` columns, the `sale_address.*` columns, or the `first_sales/first_shipto_date` columns → still **ERRORS** (none of these are it).
- **Insufficient on their own** (each of these built up from scratch *passes*): the rowset scoped join alone; rowset join + membership `in`; rowset join + the `cat_list > 2*cat_refund` measure comparison; the `as item_id` alias colliding with the rowset's `item_id` output; even the two scoped joins (ss↔pr composite + ss↔rowset) + membership in a *small* select.

So the trigger is the **combination**: scoped join onto a rowset **+** a composite multi-key scoped
join to another fact **+** the wider dimension-join graph (store / sale_address / customer.address /
customer date-dims). Under that, the rowset CTE is emitted/referenced under a stale alias.

## Likely fix area

Same family as the (fixed) q14 multi-key scoped-join-onto-rowset BinderException
(`project_q14_multikey_join_and_grouping_having_binder_bugs.md`) — but a manifestation that fix
doesn't cover: when **multiple** scoped joins (a rowset join + a composite fact join) coexist with a
broad dimension-join graph, the rowset CTE's alias/column-name resolution diverges (a downstream
projection points at `cooperative.item_id` instead of the rowset CTE's `_catalog_item_agg_item_id`).
Inspect the CTE aliasing / `get_alias` path for a rowset referenced as a join target when there are
≥2 scoped joins and the rowset is also consumed via membership + measure comparison.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-135713/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('q64_exact.preql').read())[-1]
eng.execute_raw_sql(sql)   # BinderException: Referenced table "cooperative" not found
```
