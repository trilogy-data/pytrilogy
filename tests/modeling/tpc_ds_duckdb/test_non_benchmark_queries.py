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
    assert duration.total_seconds() < 1, f"{len(dumped)}, {duration}"
