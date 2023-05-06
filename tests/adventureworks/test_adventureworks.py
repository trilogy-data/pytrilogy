# from preql.compiler import compile
from os.path import dirname, join

import pytest
from preql import Executor
from preql.core.env_processor import generate_graph
from preql.core.models import Select, QueryDatasource, Grain, Environment
from preql.core.processing.concept_strategies_v2 import (
    source_concepts,
    source_query_concepts,
)
from preql.core.query_processor import datasource_to_ctes, get_query_datasources
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse


@pytest.mark.adventureworks
def test_parsing(environment: Environment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    SqlServerDialect()
    environment, statements = parse(file, environment=environment)


@pytest.mark.adventureworks_execution
def test_finance_queries(adventureworks_engine: Executor, environment: Environment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        sql_str = generator.compile_statement(statement)
        print(sql_str)
        results = adventureworks_engine.execute_query(statement)
        assert list(results)[0] == ("Canadian Division", 8, 292174782.71999985)


@pytest.mark.adventureworks
def test_query_datasources(environment: Environment):
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
    from preql.hooks.query_debugger import print_recursive_nodes

    # assert a group up to the first name works

    # source query concepts includes extra group by to grain
    customer_node = source_query_concepts(
        [environment.concepts["customer.first_name"]],
        [environment.concepts["customer.first_name"]],
        environment,
        environment_graph,
    )
    print_recursive_nodes(customer_node)
    customer_datasource = customer_node.resolve()

    assert (
        customer_datasource.identifier
        == "customers_at_customer_customer_id_at_customer_first_name"
    )

    # assert a join before the group by works
    t_grain = Grain(
        components=[
            environment.concepts["internet_sales.order_number"],
            environment.concepts["customer.first_name"],
        ]
    )
    customer_datasource = source_concepts(
        [environment.concepts["internet_sales.order_number"]],
        t_grain.components_copy,
        environment,
        environment_graph,
    ).resolve()

    assert (
        "customers_join_fact_internet_sales_at_customer_customer_id_internet_sales_order_line_number_internet_sales_order_number"
        in customer_datasource.identifier
    )

    # assert a group up to the first name works
    customer_datasource = source_query_concepts(
        [environment.concepts["customer.first_name"]],
        [
            environment.concepts["customer.first_name"],
            environment.concepts["customer.last_name"],
        ],
        environment,
        environment_graph,
    ).resolve()

    assert (
        customer_datasource.identifier
        == "customers_at_customer_customer_id_at_customer_first_name_customer_last_name"
    )

    datasource = get_query_datasources(
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

    default_fact = "customers_join_fact_internet_sales_at_customer_customer_id_internet_sales_order_line_number_internet_sales_order_number"
    for concept in test.output_components:
        datasource = source_concepts(
            [concept], test.grain.components_copy, environment, environment_graph
        ).resolve()

        if concept.name == "customer_id":
            assert datasource.identifier == "customers<customer_id>"
        elif concept.address == "sales_territory.key":
            assert datasource.identifier == "sales_territories<key>"
        elif concept.name == "order_number":
            assert datasource.identifier == default_fact
        elif concept.name == "order_line_number":
            assert datasource.identifier == default_fact
        elif concept.name == "total_sales_amount":
            assert (
                datasource.identifier
                == "customers_join_fact_internet_sales_at_customer_customer_id_internet_sales_order_line_number_internet_sales_order_number_at_internet_sales_order_number_internet_sales_order_line_number_customer_first_name"  # noqa: E501
            )
        elif concept.name == "region":
            assert datasource.identifier == "sales_territories_at_sales_territory_key"
        elif concept.name == "first_name":
            assert datasource.identifier.startswith(
                "customers_join_fact_internet_sales_at_customer_customer_id_internet_sales_order_line_number"
            )
        else:
            raise ValueError(concept)

    cte = datasource_to_ctes(datasource)[0]

    assert {c.address for c in cte.output_columns} == {
        "customer.first_name",
        "internet_sales.order_line_number",
        "internet_sales.order_number",
        "internet_sales.total_sales_amount",
    }
    assert len(cte.output_columns) == 4


def recurse_datasource(parent: QueryDatasource, depth=0):
    for x in parent.datasources:
        if isinstance(x, QueryDatasource):
            recurse_datasource(x, depth + 1)


@pytest.mark.adventureworks
def test_two_properties(environment: Environment):
    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    test: Select = statements[-3]

    environment_graph = generate_graph(environment)

    # assert a group up to the first name works
    customer_datasource = source_concepts(
        [environment.concepts["customer.first_name"]],
        test.grain.components_copy,
        environment,
        environment_graph,
    ).resolve()

    recurse_datasource(customer_datasource)

    expected_identifier = "customers_join_order_dates_join_fact_internet_sales_at_customer_customer_id_dates_order_key_internet_sales_order_line_number_internet_sales_order_number"
    assert customer_datasource.identifier == expected_identifier

    order_date_datasource = source_concepts(
        [environment.concepts["dates.order_date"]],
        test.grain.components_copy,
        environment,
        environment_graph,
    ).resolve()

    assert order_date_datasource.identifier == expected_identifier

    get_query_datasources(
        environment=environment, graph=environment_graph, statement=test
    )

    generator = SqlServerDialect()
    sql2 = generator.generate_queries(environment, [test])
    generator.compile_statement(sql2[0])


@pytest.mark.adventureworks_execution
def test_online_sales_queries(adventureworks_engine: Executor, environment: Environment):
    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        sqls = generator.compile_statement(statement)
        adventureworks_engine.execute_query(statement).fetchall()
