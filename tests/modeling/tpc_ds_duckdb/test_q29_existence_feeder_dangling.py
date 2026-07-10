"""Regression for the q29 dangling existence-source BinderException.

A three-fact query (store_sales x store_returns x catalog_sales) with a composite
`(a, b) in (rowset.x, rowset.y)` membership filter whose rowset (``cs_agg``)
ALSO carries an unused measure column. The membership rowset is an existence
feeder — reachable only through the subselect, never joined — but its incidental
extra column made the merge misclassify it as a joined row source. The row
source_map then pointed the coalesced grain key (``cs_agg.iid``) at that feeder
CTE, which the renderer emitted in the SELECT list without ever joining it:
``Referenced table "..." not found`` at bind time.
"""

from trilogy.constants import CONFIG

QUERY = """
import store_sales as ss;
import store_returns as sr;
import catalog_sales as cs;

with cs_agg as
where cs.sold_date.year in (1999, 2000, 2001)
select
  cs.billing_customer.id as cid,
  cs.item.id as iid,
  sum(cs.quantity) as cs_qty
;

with sr_data as
where sr.return_date.year = 1999
  and sr.return_date.month_of_year >= 9
select
  sr.ticket_number,
  sr.item.sk as item_sk,
  sr.billing_customer.id as cid,
  sr.return_quantity as sr_qty
;

where ss.date.year = 1999
  and ss.date.month_of_year = 9
  and (ss.customer.id, ss.item.id) in (cs_agg.cid, cs_agg.iid)
  and (ss.ticket_number, ss.item.sk, ss.customer.id)
      in (sr_data.ticket_number, sr_data.item_sk, sr_data.cid)
select
  ss.item.id as item_code,
  ss.item.desc as item_description,
  ss.store.id as store_code,
  ss.store.name as store_name,
  sum(ss.quantity) as store_ticket_quantity,
  sum(sr_data.sr_qty) as store_return_quantity,
  sum(cs_agg.cs_qty) as catalog_ticket_quantity
union join ss.ticket_number = sr_data.ticket_number
  and ss.item.sk = sr_data.item_sk
  and ss.customer.id = sr_data.cid
union join ss.customer.id = cs_agg.cid
  and ss.item.id = cs_agg.iid
order by 1, 2, 3, 4
limit 100;
"""


def test_q29_existence_feeder_not_a_dangling_row_source(engine_sf001):
    # Pre-fix this generated a CTE that SELECTed a column from the membership
    # rowset CTE without joining it -> DuckDB "Referenced table ... not found".
    # EXPLAIN binds the query (surfacing the dangling reference) without
    # materializing the FULL-JOIN plan, so the guard stays cheap under
    # full-suite memory pressure.
    sql = engine_sf001.generate_sql(QUERY)[-1]
    engine_sf001.execute_raw_sql("EXPLAIN " + sql)


def test_q29_existence_feeder_no_datasource_build_cache(engine_sf001):
    # The build cache reshapes the CTE tree, which decides where predicate
    # pushdown lands the subselect. With it off the membership pushed into a
    # parent that never had the feeder promoted onto it.
    prior = CONFIG.generation.datasource_build_cache
    CONFIG.generation.datasource_build_cache = False
    try:
        sql = engine_sf001.generate_sql(QUERY)[-1]
    finally:
        CONFIG.generation.datasource_build_cache = prior
    engine_sf001.execute_raw_sql("EXPLAIN " + sql)
