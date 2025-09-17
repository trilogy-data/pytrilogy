from datetime import datetime, timedelta
from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_utility import calculate_effective_parent_grain, check_if_group_required
from trilogy.core.processing.concept_strategies_v3 import search_concepts, generate_graph, History
from trilogy.core.models.build import BuildGrain

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
    assert duration.total_seconds() < 2, f"{len(dumped)}, {duration}"


def test_generate_queries_perf():

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
    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    test_queries = """
select
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count;

select
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count;

    select
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count;


"""
    start = datetime.now()
    dialect.parse_text(test_queries)
    end = datetime.now()

    assert end - start < timedelta(seconds=4), f"Duration: {end - start}"


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

    r1 = list(engine.execute_text(x)[0].fetchall())
    assert r1[0].web_order_count == 11951

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

    r2 = list(engine.execute_text(y)[0].fetchall())
    # assert 1 == 0
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


def test_website_demo(engine):
    query = """import store_sales as store_sales;
select 
    store_sales.customer.id, 
    store_sales.customer.full_name,
    store_sales.ticket_number, 
limit 5;    
"""
    engine.execute_query(query).fetchall()


def test_where_clause_inputs(engine):
    y = """import store_sales as store_sales;
import catalog_sales as catalog_sales;

merge catalog_sales.bill_customer.id into store_sales.customer.id;
merge catalog_sales.item.id into store_sales.item.id;

SELECT 
    store_sales.item.name,
    store_sales.item.desc,
    store_sales.store.text_id,
    store_sales.store.name,
    sum(store_sales.net_profit) AS store_sales_profit ,
    sum(store_sales.return_net_loss) AS store_returns_loss ,
    sum(catalog_sales.net_profit) AS catalog_sales_profit
WHERE 
    store_sales.is_returned and store_sales.date.year=2001 and store_sales.date.month_of_year=4
    and store_sales.return_date.year=2001 and store_sales.return_date.month_of_year between 4 and 10
    and catalog_sales.date.year=2001 and catalog_sales.date.month_of_year between 4 and 10
    and store_sales.return_customer.id = store_sales.customer.id
ORDER BY 
    store_sales.item.name asc,
    store_sales.item.desc asc,
    store_sales.store.text_id asc,
    store_sales.store.name asc
LIMIT 100;"""
    r1 = engine.parse_text(y)[-1]
    found = False
    for cte in r1.ctes:
        if cte.condition:
            found = True
            assert "store_sales.is_returned" in [
                x.address for x in cte.condition.row_arguments
            ], [x.address for x in cte.condition.row_arguments]
    assert found


def test_constant_extra(engine):
    query = """import store_sales as store_sales;

where store_sales.date.year = 2001
select 
    count(store_sales.customer.id)->ccount, 
    1 as test,
limit 5;    
"""
    engine.execute_query(query).fetchall()

def test_merge_grain_discovery(engine:Executor):
    
    engine.parse_text(
        """import store_sales as store_sales;"""
    )
    environment = engine.environment
    build_environment = environment.materialize_for_select()
    graph = generate_graph(build_environment)

    target_concepts = [
        build_environment.concepts["store_sales.ticket_number"],
        build_environment.concepts["store_sales.date.year"],
        build_environment.concepts["store_sales.item.id"],
    ]
    node = search_concepts(
        mandatory_list = target_concepts,
        history = History(base_environment=environment),
        environment = build_environment,
        g = graph,
        depth = 0, 
        accept_partial = False

    )
    grain = calculate_effective_parent_grain(node)
    for x in node.parents:
        print(x)
    assert grain.components == BuildGrain(components={'store_sales.ticket_number', 'store_sales.item.id'}).components

    assert check_if_group_required(
        downstream_concepts=target_concepts,
        parents=[node.resolve()],
        environment=build_environment
    ).required == False