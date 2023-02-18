# from preql.compiler import compile
from os.path import dirname, join

import networkx as nx
import pytest

from preql.core.env_processor import generate_graph
from preql.core.models import Select, QueryDatasource, CTE
from preql.core.query_processor import (
    get_datasource_by_concept_and_grain,
    datasource_to_ctes,
    get_query_datasources,
)
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse


@pytest.mark.adventureworks
def test_parsing(environment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)


@pytest.mark.adventureworks_execution
def test_finance_queries(adventureworks_engine, environment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        sql = generator.compile_statement(statement)
        results = adventureworks_engine.execute_query(statement)


@pytest.mark.adventureworks
def test_query_datasources(environment):
    from preql.constants import logger
    from logging import StreamHandler
    from preql.core.models import Grain

    logger.addHandler(StreamHandler())

    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    assert (
        str(environment.datasources["internet_sales.fact_internet_sales"].grain)
        == "Grain<internet_sales.order_line_number,internet_sales.order_number>"
    )

    test: Select = statements[-1]  # multipart join

    for item in test.grain.components:
        print(item)
    print('-----')
    environment_graph = generate_graph(environment)

    # assert a group up to the first name works
    customer_datasource = get_datasource_by_concept_and_grain(
       environment.concepts['customer.first_name'], Grain(components = [environment.concepts['customer.first_name']]), environment, environment_graph
    )

    assert customer_datasource.identifier == "customers_at_customer_first_name"

    # assert a join before the group by works
    customer_datasource = get_datasource_by_concept_and_grain(
        environment.concepts['internet_sales.order_number'], Grain(components = [environment.concepts['internet_sales.order_number'], environment.concepts['customer.first_name']]), environment, environment_graph
    )

    assert customer_datasource.identifier == "customers_fact_internet_sales_at_internet_sales_order_number_customer_first_name"

    # assert a group up to the first name works
    customer_datasource = get_datasource_by_concept_and_grain(
        environment.concepts['customer.first_name'], Grain(components = [environment.concepts['customer.first_name'],environment.concepts['customer.last_name']]), environment, environment_graph
    )

    assert customer_datasource.identifier == "customers_at_customer_first_name_customer_last_name"


    concepts, datasources = get_query_datasources(
        environment=environment, graph=environment_graph, statement=test
    )

    assert "ds~internet_sales.fact_internet_sales" in environment_graph.nodes
    assert (
        "c~internet_sales.total_sales_amount@Grain<Abstract>" in environment_graph.nodes
    )
    # for val in list(environment_graph.neighbors(datasource_to_node(fact_internet_sales))):
    #     print(val)
    # assert concept_to_node(sales.with_grain) in list(environment_graph.neighbors(datasource_to_node(fact_internet_sales)))
    # assert (concept_to_node(sales),concept_to_node(total_sales), ) in environment_graph.edges()

    default_fact = 'fact_internet_sales_at_internet_sales_order_line_number_internet_sales_order_number'
    for concept in test.output_components:
        datasource = get_datasource_by_concept_and_grain(
            concept, test.grain, environment, environment_graph
        )

        if concept.name == "customer_id":
            assert datasource.identifier == "customers<customer_id>"
        elif concept.address == "sales_territory.key":
            assert datasource.identifier == "sales_territories<key>"
        elif concept.name == "order_number":
            assert (
                datasource.identifier
                == default_fact
            )
        elif concept.name == "order_line_number":
            assert (
                datasource.identifier
                == default_fact
            )
        elif concept.name == "total_sales_amount":
            assert (
                datasource.identifier
                == 'customers_fact_internet_sales_at_internet_sales_order_number_internet_sales_order_line_number_customer_first_name'
            )
        elif concept.name == "region":
            assert datasource.identifier == "sales_territories_at_sales_territory_key"
        elif concept.name == "first_name":
            assert datasource.identifier == "customers_at_customer_customer_id_at_internet_sales_order_number_internet_sales_order_line_number_customer_first_name"
        else:
            raise ValueError(concept)
    assert set([datasource.identifier for datasource in datasources.values()]) == {'customers_at_customer_customer_id_at_internet_sales_order_number_internet_sales_order_line_number_customer_first_name',
                                                                                   'customers_fact_internet_sales_at_internet_sales_order_number_internet_sales_order_line_number_customer_first_name',
                                                                                   'fact_internet_sales_at_internet_sales_order_line_number_internet_sales_order_number'}


    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)

    assert len(ctes) == 6
    base_cte: CTE = [
        cte
        for cte in ctes
        if cte.name.startswith(
            "cte_fact_internet_sales_at_internet_sales_order_line_number_internet_sales_order_number"
        )
    ][0]
    assert len(base_cte.output_columns) == 2

    # the CTE has all grain components
    assert base_cte.group_to_grain == False


@pytest.mark.adventureworks
def test_two_properties(environment):
    from preql.constants import logger
    from logging import StreamHandler
    from preql.core.models import Grain

    logger.addHandler(StreamHandler())

    with open(
            join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    test: Select = statements[-3]

    for item in test.grain.components:
        print(item)
    print('-----')
    environment_graph = generate_graph(environment)

    # assert a group up to the first name works
    customer_datasource = get_datasource_by_concept_and_grain(
        environment.concepts['customer.first_name'], test.grain, environment, environment_graph
    )

    # assert customer_datasource.identifier == "customers_fact_internet_sales_order_dates_at_dates_order_date_customer_first_name"

    order_date_datasource = get_datasource_by_concept_and_grain(
        environment.concepts['dates.order_date'], test.grain, environment, environment_graph
    )

    # assert order_date_datasource.identifier == "customers_fact_internet_sales_order_dates_at_dates_order_date_customer_first_name"


    concepts, datasources = get_query_datasources(
        environment=environment, graph=environment_graph, statement=test
    )

    generator = SqlServerDialect()
    sql2 = generator.generate_queries(environment, [test])
    sql = generator.compile_statement(sql2[0])
    print(sql)

@pytest.mark.adventureworks_execution
def test_online_sales_queries(adventureworks_engine, environment):
    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        sql = generator.compile_statement(statement)
        results = adventureworks_engine.execute_query(statement).fetchall()
