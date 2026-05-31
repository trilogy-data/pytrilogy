import re
from datetime import datetime
from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.constants import CONFIG
from trilogy.core.models.build import BuildAggregateWrapper, BuildGrain
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import (
    History,
    generate_graph,
    search_concepts,
)
from trilogy.core.processing.discovery_utility import (
    calculate_effective_parent_grain,
    check_if_group_required,
)
from trilogy.core.query_processor import process_query
from trilogy.parser import parse_text

working_path = Path(__file__).parent


def test_demo(engine):
    query = """
import physical_sales as physical_sales;
with ranked_states as
select 
    physical_sales.billing_customer.first_name,
    physical_sales.billing_customer.address.state,
    rank physical_sales.billing_customer.first_name 
        over physical_sales.billing_customer.address.state 
        order by  sum(physical_sales.sales_price) by physical_sales.billing_customer.first_name, physical_sales.billing_customer.address.state desc 
    -> sales_rank;

select 
    ranked_states.physical_sales.billing_customer.first_name,
    avg(cast(ranked_states.sales_rank as int))-> avg_sales_rank
order by 
    avg_sales_rank desc
limit 10
;"""

    results = engine.execute_text(query)[0].fetchall()

    assert results[0].avg_sales_rank != 1.0


def test_copy_perf():
    env, imports = Environment(working_path=working_path).parse("""
import call_center as call_center;
import catalog_returns as catalog_returns;
import catalog_sales as catalog_sales;
import customer_demographic as customer_demographic;
import customer as customer;
import inventory as inventory;
import item as item;
import promotion as promotion;
import physical_returns as physical_returns;
import physical_sales as physical_sales;
import store as store;
import time as time;
import date as date;
import warehouse as warehouse;
import web_sales as web_sales;
""")

    start = datetime.now()
    _ = env.duplicate()
    end = datetime.now()
    duration = end - start
    dumped = env.to_dict()
    assert duration.total_seconds() < 2, f"{len(dumped)}, {duration}"


def test_generate_queries_perf():

    env, _ = Environment(working_path=working_path).parse("""
import call_center as call_center;
import catalog_returns as catalog_returns;
import catalog_sales as catalog_sales;
import customer_demographic as customer_demographic;
import customer as customer;
import inventory as inventory;
import item as item;
import promotion as promotion;
import physical_returns as physical_returns;
import physical_sales as physical_sales;
import store as store;
import time as time;
import date as date;
import warehouse as warehouse;
import web_sales as web_sales;
""")

    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    test_queries = """
select
    physical_sales.date.year,
    count(physical_sales.ticket_number) as store_order_count;

select
    web_sales.date.year,
    count(web_sales.order_number) as web_order_count;

select
    catalog_sales.date.year,
    count(catalog_sales.order_number) as catalog_order_count;


"""
    durations = []
    for _ in range(5):
        start = datetime.now()
        dialect.parse_text(test_queries)
        end = datetime.now()
        durations.append((end - start).total_seconds())
    # 0.4037
    avg_duration = sum(durations) / len(durations)
    print(f"Parse times: {durations}")
    print(f"Average parse time: {avg_duration:.4f}s")
    assert avg_duration < 2.0, f"Average duration: {avg_duration:.4f}s"


def test_merge_comparison(engine):

    x = """
import physical_sales as physical_sales;
import web_sales as web_sales;




    SELECT
    physical_sales.date.year,
    count(physical_sales.ticket_number) as store_order_count
HAVING
    store_order_count>0
MERGE
SELECT
    web_sales.date.year,
    count(web_sales.order_number) as web_order_count
HAVING
    web_order_count>0 
ALIGN 
    report_date: physical_sales.date.year, web_sales.date.year
ORDER BY
    report_date asc;"""

    r1 = list(engine.execute_text(x)[0].fetchall())
    assert r1[0].web_order_count == 11951

    y = """
import physical_sales as physical_sales;
import web_sales as web_sales;
import date as date; 

MERGE physical_sales.date.* into ~date.*;
MERGE web_sales.date.* into ~date.*;

datasource filtered_cache (
    store_sales_date_month_of_year: ~physical_sales.date.month_of_year,
    store_sales_date_year: ~physical_sales.date.year,
    store_sales_ext_sales_price: ~physical_sales.ext_sales_price,
    store_sales_item_brand_id: ~physical_sales.item.brand_id,
    store_sales_item_brand_name: ~physical_sales.item.brand_name,
    store_sales_item_id: ~physical_sales.item.id,
    store_sales_item_manufacturer_id: ~physical_sales.item.manufacturer_id,
    store_sales_ticket_number: ~physical_sales.ticket_number
)
complete where physical_sales.date.month_of_year = 11 and physical_sales.item.manufacturer_id = 128
address filtered_cache;


SELECT
    date.year,
    count(web_sales.order_number) as web_order_count,
    count(physical_sales.ticket_number) as store_order_count
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
import physical_sales as physical_sales;
import web_sales as web_sales;

SELECT
    physical_sales.date.year,
    count_distinct(physical_sales.ticket_number) as store_order_count
having
    store_order_count > 1000
MERGE
SELECT
    web_sales.date.year,
    count_distinct(web_sales.order_number) as web_order_count

ALIGN 
    report_date: physical_sales.date.year, web_sales.date.year
ORDER BY
    report_date asc;"""

    r1 = engine.execute_query(y).fetchall()
    for row in r1:
        assert row.store_order_count is None or row.store_order_count > 1000, row


def test_website_demo(engine):
    query = """import physical_sales as physical_sales;
select 
    physical_sales.billing_customer.id, 
    physical_sales.billing_customer.full_name,
    physical_sales.ticket_number, 
limit 5;    
"""
    engine.execute_query(query).fetchall()


def test_where_clause_inputs(engine):
    y = """import physical_sales as physical_sales;
import catalog_sales as catalog_sales;

merge catalog_sales.bill_customer.id into physical_sales.billing_customer.id;
merge catalog_sales.item.id into physical_sales.item.id;

SELECT 
    physical_sales.item.product_name,
    physical_sales.item.desc,
    physical_sales.store.text_id,
    physical_sales.store.name,
    sum(physical_sales.net_profit) AS store_sales_profit ,
    sum(physical_sales.return_net_loss) AS store_returns_loss ,
    sum(catalog_sales.net_profit) AS catalog_sales_profit
WHERE 
    physical_sales.is_returned and physical_sales.date.year=2001 and physical_sales.date.month_of_year=4
    and physical_sales.return_date.year=2001 and physical_sales.return_date.month_of_year between 4 and 10
    and catalog_sales.date.year=2001 and catalog_sales.date.month_of_year between 4 and 10
    and physical_sales.return_customer.id = physical_sales.billing_customer.id
ORDER BY 
    physical_sales.item.product_name asc,
    physical_sales.item.desc asc,
    physical_sales.store.text_id asc,
    physical_sales.store.name asc
LIMIT 100;"""
    r1 = engine.parse_text(y)[-1]
    found = False
    for cte in r1.ctes:
        if cte.condition and "physical_sales.is_returned" in [
            x.address for x in cte.condition.row_arguments
        ]:
            found = True
            break
    assert found


def test_constant_extra(engine):
    query = """import physical_sales as physical_sales;

where physical_sales.date.year = 2001
select 
    count(physical_sales.billing_customer.id)->ccount, 
    1 as test,
limit 5;    
"""
    engine.execute_query(query).fetchall()


def test_merge_grain_discovery(engine: Executor):

    engine.parse_text("""import physical_sales as physical_sales;""")
    environment = engine.environment
    build_environment = environment.materialize_for_select()
    graph = generate_graph(build_environment)

    target_concepts = [
        build_environment.concepts["physical_sales.ticket_number"],
        build_environment.concepts["physical_sales.date.year"],
        build_environment.concepts["physical_sales.item.id"],
    ]
    node = search_concepts(
        mandatory_list=target_concepts,
        history=History(base_environment=environment),
        environment=build_environment,
        g=graph,
        depth=0,
        accept_partial=False,
    )
    grain = calculate_effective_parent_grain(node.resolve())
    assert (
        grain.components
        == BuildGrain(
            components={"physical_sales.ticket_number", "physical_sales.item.id"}
        ).components
    ), grain.components

    assert not check_if_group_required(
        downstream_concepts=target_concepts,
        parents=[node.resolve()],
        environment=build_environment,
    ).required


def test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate():
    query = """
    import all_sales as sales;

    def weekday_sum(weekday) -> sum(
        sales.ext_sales_price ? sales.date.day_of_week = weekday
    ) by sales.date.week_seq;

    SELECT
        sales.date.week_seq,
        @weekday_sum(0) / @weekday_sum(1) as sun_over_mon
    ORDER BY sales.date.week_seq asc
    LIMIT 5;
    """

    env = Environment(working_path=working_path)
    _, statements = parse_text(query, env)
    processed = process_query(env, statements[-1])
    grouped_cte = processed.ctes[-1]

    aggregate_outputs = [
        c
        for c in grouped_cte.output_columns
        if isinstance(c.lineage, BuildAggregateWrapper)
    ]
    assert len(aggregate_outputs) == 2
    assert [c.address for c in grouped_cte.group_concepts] == ["sales.date.week_seq"]


def test_def_body_can_call_another_custom_function():
    """Regression: FunctionCallWrapper.with_reference_replacement used to raise
    NotImplementedError, breaking any `def` whose body wrapped another @-call."""
    query = """
    import all_sales as sales;

    def weekday_sum(weekday) -> sum(
        sales.ext_sales_price ? sales.date.day_of_week = weekday
    ) by sales.date.week_seq;

    def doubled_weekday_sum(weekday) -> @weekday_sum(weekday) * 2;

    SELECT
        sales.date.week_seq,
        @doubled_weekday_sum(0) as sun_doubled
    ORDER BY sales.date.week_seq asc
    LIMIT 5;
    """

    env = Environment(working_path=working_path)
    _, statements = parse_text(query, env)
    process_query(env, statements[-1])


def test_two_merge_aggregate_compacts_inline_window_query():
    query = """
    import catalog_sales as catalog_sales;
    import web_sales as web_sales;
    import date as date;

    merge catalog_sales.date.* into ~date.*;
    merge web_sales.date.* into ~date.*;

    auto relevent_week_seq <- filter date.week_seq where date.year in (2001, 2002);

    def weekday_sales(weekday) ->
        (SUM(CASE WHEN date.day_of_week = weekday THEN web_sales.ext_sales_price ELSE 0.0 END) by date.week_seq +
        SUM(CASE WHEN date.day_of_week = weekday THEN catalog_sales.ext_sales_price ELSE 0.0 END) by date.week_seq)
    ;

    def round_lag(sales)-> round(sales / (lead 53 sales by date.week_seq asc), 2);

    WHERE
        date.week_seq in relevent_week_seq
    SELECT
        date.week_seq,
        @round_lag(@weekday_sales(0)) as sunday_increase,
        @round_lag(@weekday_sales(1)) as monday_increase,
        @round_lag(@weekday_sales(2)) as tuesday_increase,
        @round_lag(@weekday_sales(3)) as wednesday_increase,
        @round_lag(@weekday_sales(4)) as thursday_increase,
        @round_lag(@weekday_sales(5)) as friday_increase,
        @round_lag(@weekday_sales(6)) as saturday_increase
    having sunday_increase is not null
    ORDER BY date.week_seq asc NULLS FIRST
    LIMIT 100;
    """

    original = CONFIG.optimizations.merge_aggregate
    try:
        CONFIG.optimizations.merge_aggregate = False
        off_env = Environment(working_path=working_path)
        _, off_statements = parse_text(query, off_env)
        off_processed = process_query(off_env, off_statements[-1])
        off_generated = Dialects.DUCK_DB.default_executor(environment=off_env).generate_sql(query)[-1]
        CONFIG.optimizations.merge_aggregate = True
        on_env = Environment(working_path=working_path)
        _, on_statements = parse_text(query, on_env)
        on_processed = process_query(on_env, on_statements[-1])
    finally:
        CONFIG.optimizations.merge_aggregate = original

    assert len(off_processed.ctes) == 9, off_generated
    assert len(on_processed.ctes) == 5


def test_rowset_arithmetic_argument_keeps_precedence():
    query = (working_path / "query02-one.preql").read_text()
    env = Environment(working_path=working_path)
    sql = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(query)[-1]

    assert re.search(r"round\(\( .*? \+ .*? \) / \(lead", sql, re.S), sql
