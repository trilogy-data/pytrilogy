# from preql.compiler import compile
from os.path import dirname, join

import networkx as nx
import pytest

from preql.core.env_processor import generate_graph
from preql.core.models import Select, QueryDatasource, CTE, Grain
from preql.core.query_processor import (
    get_datasource_by_concept_and_grain,
    datasource_to_ctes,
    get_query_datasources,
)
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse
from preql.core.query_processor import process_query


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

    environment_graph = generate_graph(environment)
    concepts, datasources = get_query_datasources(
        environment=environment, graph=environment_graph, statement=test
    )
    total_sales = environment.concepts["internet_sales.total_sales_amount"]
    sales = environment.concepts["internet_sales.sales_amount"]
    fact_internet_sales = environment.datasources["internet_sales.fact_internet_sales"]

    path = nx.shortest_path(
        environment_graph,
        "ds~internet_sales.fact_internet_sales",
        "c~internet_sales.order_quantity@Grain<internet_sales.order_line_number,internet_sales.order_number>",
    )
    assert "ds~internet_sales.fact_internet_sales" in environment_graph.nodes
    assert (
        "c~internet_sales.total_sales_amount@Grain<Abstract>" in environment_graph.nodes
    )
    path = nx.shortest_path(
        environment_graph,
        "ds~internet_sales.fact_internet_sales",
        "c~internet_sales.total_sales_amount@Grain<Abstract>",
    )
    # for val in list(environment_graph.neighbors(datasource_to_node(fact_internet_sales))):
    #     print(val)
    # assert concept_to_node(sales.with_grain) in list(environment_graph.neighbors(datasource_to_node(fact_internet_sales)))
    # assert (concept_to_node(sales),concept_to_node(total_sales), ) in environment_graph.edges()

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
                == "fact_internet_sales_at_order_line_number_order_number"
            )
        elif concept.name == "order_line_number":
            assert (
                datasource.identifier
                == "fact_internet_sales_at_order_line_number_order_number"
            )
        elif concept.name == "total_sales_amount":
            assert (
                datasource.identifier
                == "fact_internet_sales_at_order_line_number_order_number"
            )
        elif concept.name == "region":
            assert datasource.identifier == "sales_territories_at_key"
        elif concept.name == "first_name":
            assert datasource.identifier == "customers_at_customer_id"
        else:
            raise ValueError(concept)
    assert set([datasource.identifier for datasource in datasources.values()]) == {
        "customers_at_customer_id",
        "fact_internet_sales_at_order_line_number_order_number",
        "sales_territories_at_key",
    }

    joined_datasource: QueryDatasource = [
        ds for ds in datasources.values() if ds.identifier == "customers_at_customer_id"
    ][0]
    assert set([c.name for c in joined_datasource.input_concepts]) == {
        "customer_id",
        "first_name",
    }
    assert set([c.name for c in joined_datasource.output_concepts]) == {
        "customer_id",
        "first_name",
    }

    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)

    assert len(ctes) == 3
    #
    for cte in ctes:
        print(cte.name)
    base_cte: CTE = [
        cte
        for cte in ctes
        if cte.name.startswith(
            "cte_fact_internet_sales_at_order_line_number_order_number_"
        )
    ][0]
    assert len(base_cte.output_columns) == 5

    # the CTE has all grain components
    assert base_cte.group_to_grain == False


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
        print(sql)
        results = adventureworks_engine.execute_query(statement)
