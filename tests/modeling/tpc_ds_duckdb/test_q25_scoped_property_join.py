"""q25 authored with scoped joins on the customer BUSINESS key (`.id`).

The `.id` members live on the customer dimension (facts bind `.sk`), so
enforcing `subset join ss.customer.id = cs.billing_customer.id` requires the
per-side dimension hop. Historically the equality silently vanished and
catalog_sale_net_profit became an item-level total. Pins: every branch that
joins catalog_sales pairs the two customer-dimension scans on C_CUSTOMER_ID.
"""

import re
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent

QUERY = """import store_sales as ss;
import catalog_sales as cs;

where
  ss.date.year = 2001
  and ss.date.month_of_year = 4
  and ss.is_returned
  and ss.return_date.year = 2001
  and ss.return_date.month_of_year between 4 and 10
  and cs.sold_date.year = 2001
  and cs.sold_date.month_of_year between 4 and 10
select
  ss.item.id as item_code,
  ss.item.desc as item_desc,
  ss.store.id as store_code,
  ss.store.name as store_name,
  sum(ss.net_profit) as store_sale_net_profit,
  sum(ss.return_net_loss) as store_return_net_loss,
  sum(cs.net_profit) as catalog_sale_net_profit
subset join ss.customer.id = cs.billing_customer.id
subset join ss.item.sk = cs.item.sk
order by item_code, item_desc, store_code, store_name
limit 100;"""


def test_q25_property_join_pairs_customer_id():
    env = Environment(working_path=working_path)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    statements = engine.parse_text(QUERY)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql

    id_pairings = re.findall(
        r'"C_CUSTOMER_ID"\s*(?:=|is not distinct from)\s*"?\w*"?\."C_CUSTOMER_ID"',
        sql,
    )
    assert id_pairings, f"customer id pairing missing from SQL:\n{sql}"
    # every catalog_sales entry must be customer-gated: each CTE scanning or
    # consuming catalog data at row grain carries an id pairing
    catalog_ctes = [
        chunk
        for chunk in sql.split("\n\n")
        if '"catalog_sales"' in chunk and "JOIN" in chunk
    ]
    for chunk in catalog_ctes:
        assert "C_CUSTOMER_ID" in chunk, f"catalog branch missing pairing:\n{chunk}"
