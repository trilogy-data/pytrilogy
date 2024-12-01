from trilogy.core.models import Environment
from pathlib import Path
from datetime import datetime

working_path = Path(__file__).parent


def test_demo(engine):
    query = """
import store_sales as store_sales;
with ranked_states as
select 
    store_sales.customer.first_name,
    store_sales.customer.state,
    rank store_sales.customer.first_name 
        over store_sales.customer.state 
        order by  sum(store_sales.sales_price) by store_sales.customer.first_name, store_sales.customer.state desc 
    -> sales_rank;

select 
    ranked_states.store_sales.customer.first_name,
    avg(cast(ranked_states.sales_rank as int))-> avg_sales_rank
order by 
    avg_sales_rank desc
limit 10
;"""

    results = engine.execute_text(query)[0].fetchall()

    assert results[0].avg_sales_rank != 1.0


def test_copy_perf():
    env, imports = Environment(working_path=working_path).parse(
        """
import call_center as call_center;
import catalog_returns as catalog_returns;
import catalog_sales as catalog_sales;
import customer_demographic as customer_demographic;
import customer as customer;
import inventory as inventory;
import item as item;
import promotion as promotion;
import store_returns as store_returns;
import store_sales as store_sales;
import store as store;
import time as time;
import date as date;
import warehouse as warehouse;
import web_sales as web_sales;
"""
    )

    start = datetime.now()
    _ = env.duplicate()
    end = datetime.now()
    duration = end - start
    dumped = env.model_dump_json()
    assert duration.total_seconds() < 2.75, f"{len(dumped)}, {duration}"


def test_merge_comparison(engine):

    x = """
import store_sales as store_sales;
import web_sales as web_sales;
    SELECT
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count
HAVING
    store_order_count>0
MERGE
SELECT
    web_sales.date.year,
    count(web_sales.order_number) as web_order_count
HAVING
    web_order_count>0 
ALIGN 
    report_date: store_sales.date.year, web_sales.date.year
ORDER BY
    report_date asc;"""

    y = """
import store_sales as store_sales;
import web_sales as web_sales;
import date as date; 
MERGE store_sales.date.* into ~date.*;
MERGE web_sales.date.* into ~date.*;

SELECT
    date.year,
    count(web_sales.order_number) as web_order_count,
    count(store_sales.ticket_number) as store_order_count
HAVING
    web_order_count>0 or store_order_count>0
ORDER BY 
    date.year asc
LIMIT 100;"""

    r1 = engine.execute_text(x)[0].fetchall()
    r2 = list(engine.execute_text(y)[0].fetchall())

    for idx, row in enumerate(r1):
        r2_row = r2[idx]
        assert row.web_order_count == r2_row.web_order_count
        assert row.store_order_count == r2_row.store_order_count


def test_having_nested(engine):

    y = """
import store_sales as store_sales;
import web_sales as web_sales;

SELECT
    store_sales.date.year,
    count_distinct(store_sales.ticket_number) as store_order_count
having
    store_order_count > 1000
MERGE
SELECT
    web_sales.date.year,
    count_distinct(web_sales.order_number) as web_order_count

ALIGN 
    report_date: store_sales.date.year, web_sales.date.year
ORDER BY
    report_date asc;"""

    r1 = engine.execute_query(y).fetchall()
    for row in r1:
        assert row.store_order_count is None or row.store_order_count > 1000, row
