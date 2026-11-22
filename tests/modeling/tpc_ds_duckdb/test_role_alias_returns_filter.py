"""Two role-playing date columns of `all_sales`, renamed, under a WHERE on a
return column (tpc-ds agent q05 probe, 2026-11-21). Renaming must not change
the plan's join axis: the aliased query returns the bare query's rows."""

import pytest

TEMPLATE = """import all_sales as s;
where {where}
select s.sale_date.{part}{a}, s.return_date.{part}{b}{extra};"""


@pytest.mark.parametrize(
    "where,part,extra",
    [
        ("s.is_returned is not null", "date", ""),
        ("s.is_returned is not null", "year", ""),
        ("s.return_amount is not null", "date", ""),
        ("s.is_returned is not null", "year", ", s.channel"),
    ],
)
def test_role_aliases_beside_return_filter(engine_sf001, where, part, extra):
    def run(a: str, b: str) -> list:
        query = TEMPLATE.format(where=where, part=part, a=a, b=b, extra=extra)
        rows = engine_sf001.execute_text(query)[-1].fetchall()
        return sorted(rows, key=lambda r: tuple((v is None, v) for v in r))

    assert run(" as sold", " as returned") == run("", "")


CHAINED_INTERSECT = """import store_sales as ss;
import catalog_sales as cs;
import web_sales as ws;

with sc as intersect(
    (where ss.sale_date.year = 2000 and ss.customer.sk is not null
     select ss.customer.last_name as last_name, ss.sale_date.date as sale_date),
    (where cs.sale_date.year = 2000 and cs.billing_customer.sk is not null
     select cs.billing_customer.last_name as last_name, cs.sale_date.date as sale_date)
) -> (last_name, sale_date);

with scw as intersect(
    (select sc.last_name as last_name, sc.sale_date as sale_date),
    (where ws.sale_date.year = 2000 and ws.billing_customer.sk is not null
     select ws.billing_customer.last_name as last_name, ws.sale_date.date as sale_date)
) -> (last_name, sale_date);

select count(scw.sale_date) as n;"""

CHAINED_INTERSECT_SQL = """
with s as (select c_last_name, d_date from memory.store_sales
    join memory.date_dim on ss_sold_date_sk = d_date_sk
    join memory.customer on ss_customer_sk = c_customer_sk where d_year = 2000),
c as (select c_last_name, d_date from memory.catalog_sales
    join memory.date_dim on cs_sold_date_sk = d_date_sk
    join memory.customer on cs_bill_customer_sk = c_customer_sk where d_year = 2000),
w as (select c_last_name, d_date from memory.web_sales
    join memory.date_dim on ws_sold_date_sk = d_date_sk
    join memory.customer on ws_bill_customer_sk = c_customer_sk where d_year = 2000)
select count(d_date) from (select * from s intersect select * from c intersect select * from w)
"""


def test_chained_intersect_of_renamed_arms(engine_sf001):
    rows = engine_sf001.execute_text(CHAINED_INTERSECT)[-1].fetchall()
    assert rows == engine_sf001.execute_raw_sql(CHAINED_INTERSECT_SQL).fetchall()
