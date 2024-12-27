# from trilogy.compiler import compile
from os.path import dirname, join

import pytest

from trilogy import Executor
from trilogy.core.env_processor import generate_graph
from trilogy.core.author_models import SelectStatement
from trilogy.core.execute_models import (
    BoundConcept,
    BoundEnvironment,
    Grain,
    ProcessedQuery,
    ProcessedQueryPersist,
    QueryDatasource,
)
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.nodes import MergeNode, SelectNode
from trilogy.core.query_processor import datasource_to_cte, get_query_datasources
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.parser import parse


@pytest.mark.adventureworks
def test_parsing(environment: BoundEnvironment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    SqlServerDialect()
    environment, statements = parse(file, environment=environment)


@pytest.mark.adventureworks_execution
def test_finance_queries(adventureworks_engine: Executor, environment: BoundEnvironment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        if not isinstance(statement, (ProcessedQuery, ProcessedQueryPersist)):
            continue
        generator.compile_statement(statement)
        results = adventureworks_engine.execute_query(statement)
        assert list(results)[0] == ("Canadian Division", 8, 292174782.71999985)


@pytest.mark.adventureworks
def test_query_datasources(environment: BoundEnvironment):
    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    assert (
        str(environment.datasources["internet_sales.fact_internet_sales"].grain)
        == "Grain<internet_sales.order_line_number,internet_sales.order_number>"
    )

    test: SelectStatement = statements[-1]  # multipart join

    environment_graph = generate_graph(environment)
    from trilogy.hooks.query_debugger import print_recursive_nodes

    # assert a group up to the first name works

    # source query concepts includes extra group by to grain
    customer_node = search_concepts(
        [
            environment.concepts[
                "internet_sales.customer.first_name"
            ].with_default_grain()
        ],
        environment=environment,
        g=environment_graph,
        depth=0,
    )
    print_recursive_nodes(customer_node)
    customer_datasource = customer_node.resolve()

    assert (
        customer_datasource.safe_identifier
        == "internet_sales_customer_customers_at_internet_sales_customer_customer_id_at_internet_sales_customer_first_name"
    )

    # assert a join before the group by works
    t_grain = Grain(
        components=[
            environment.concepts["internet_sales.order_number"],
            environment.concepts["internet_sales.customer.first_name"],
        ]
    )
    customer_datasource = search_concepts(
        [environment.concepts["internet_sales.order_number"]]
        + [environment.concepts[x] for x in t_grain.components],
        environment=environment,
        g=environment_graph,
        depth=0,
    ).resolve()

    # assert a group up to the first name works
    customer_datasource = search_concepts(
        [environment.concepts["internet_sales.customer.first_name"]],
        environment=environment,
        g=environment_graph,
        depth=0,
    ).resolve()

    assert (
        customer_datasource.safe_identifier
        == "internet_sales_customer_customers_at_internet_sales_customer_customer_id_at_internet_sales_customer_first_name"
    )

    datasource = get_query_datasources(environment=environment, statement=test)

    cte = datasource_to_cte(datasource, {})

    assert {c.address for c in cte.output_columns} == {
        "internet_sales.customer.first_name",
        "internet_sales.order_line_number",
        "internet_sales.order_number",
        "internet_sales.total_sales_amount",
    }
    assert len(cte.output_columns) == 4


def recurse_datasource(parent: QueryDatasource, depth=0):
    for x in parent.datasources:
        if isinstance(x, QueryDatasource):
            recurse_datasource(x, depth + 1)


def list_to_address(clist: list[BoundConcept]) -> set[str]:
    return set([c.address for c in clist])


@pytest.mark.adventureworks
def test_two_properties(environment: BoundEnvironment):
    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    test: SelectStatement = statements[-3]

    environment_graph = generate_graph(environment)

    # assert a group up to the first name works
    _customer_datasource = search_concepts(
        [environment.concepts["internet_sales.customer.first_name"]]
        + [environment.concepts[x] for x in test.grain.components],
        environment=environment,
        g=environment_graph,
        depth=0,
    )

    assert _customer_datasource
    customer_datasource = _customer_datasource.resolve()

    assert list_to_address(customer_datasource.output_concepts).issuperset(
        list_to_address(
            [environment.concepts["internet_sales.customer.first_name"]]
            + [environment.concepts[x] for x in test.grain.components],
        )
    )

    order_date_datasource = search_concepts(
        [environment.concepts["internet_sales.dates.order_date"]]
        + [environment.concepts[x] for x in test.grain.components],
        environment=environment,
        g=environment_graph,
        depth=0,
    ).resolve()

    assert list_to_address(order_date_datasource.output_concepts).issuperset(
        list_to_address(
            [environment.concepts["internet_sales.dates.order_date"]]
            + [environment.concepts[x] for x in test.grain.components],
        )
    )

    get_query_datasources(environment=environment, statement=test)


@pytest.mark.adventureworks
def test_grain(environment: BoundEnvironment):
    from trilogy.core.processing.concept_strategies_v3 import search_concepts

    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    environment_graph = generate_graph(environment)
    test = search_concepts(
        [
            environment.concepts["dates.order_date"],
            environment.concepts["dates.order_key"],
        ],
        environment=environment,
        depth=0,
        g=environment_graph,
    )
    assert isinstance(test, SelectNode)
    assert len(test.parents) == 0
    assert (
        test.grain.components
        == Grain(components=[environment.concepts["dates.order_key"]]).components
    )
    assert (
        environment.datasources["dates.order_dates"].grain.components
        == Grain(components=[environment.concepts["dates.order_key"]]).components
    )
    resolved = test.resolve()
    assert resolved.grain == Grain(components=[environment.concepts["dates.order_key"]])
    assert test.grain == resolved.grain
    assert resolved.group_required is False


@pytest.mark.adventureworks
def test_group_to_grain(environment: BoundEnvironment):
    from trilogy.core.processing.concept_strategies_v3 import search_concepts

    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    environment_graph = generate_graph(environment)
    assert (
        len(
            environment.concepts[
                "internet_sales.total_sales_amount_debug"
            ].grain.components
        )
        == 2
    )
    test = search_concepts(
        [
            environment.concepts["internet_sales.total_sales_amount_debug"],
            environment.concepts["internet_sales.dates.order_date"],
        ],
        environment=environment,
        depth=0,
        g=environment_graph,
    )
    assert isinstance(test, MergeNode)
    assert test.whole_grain is True
    assert len(test.parents) == 2
    resolved = test.resolve()
    expected_grain = Grain(
        components=[
            environment.concepts["internet_sales.dates.order_key"],
            environment.concepts["internet_sales.order_line_number"],
            environment.concepts["internet_sales.order_number"],
        ]
    )
    assert resolved.grain.components == expected_grain.components, [resolved.grain.components, expected_grain.components]
    assert resolved.force_group is False
    assert resolved.group_required is False


@pytest.mark.adventureworks
def test_two_properties_query(environment: BoundEnvironment):
    from trilogy.core.processing.concept_strategies_v3 import search_concepts
    from trilogy.core.processing.node_generators import gen_group_node

    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    environment, statements = parse(file, environment=environment)
    assert "local.total_sales_amount_debug_2" in set(list(environment.concepts.keys()))
    environment_graph = generate_graph(environment)
    assert (
        len(
            environment.concepts[
                "internet_sales.total_sales_amount_debug"
            ].grain.components
        )
        == 2
    )
    test = gen_group_node(
        environment.concepts["total_sales_amount_debug_2"],
        local_optional=[environment.concepts["internet_sales.dates.order_date"]],
        environment=environment,
        depth=0,
        g=environment_graph,
        source_concepts=search_concepts,
    )

    test: SelectStatement = statements[-3]
    generator = SqlServerDialect()
    sql2 = generator.generate_queries(environment, [test])
    generator.compile_statement(sql2[0])


@pytest.mark.adventureworks_execution
def test_online_sales_queries(
    adventureworks_engine: Executor, environment: BoundEnvironment
):
    with open(
        join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        generator.compile_statement(statement)
        adventureworks_engine.execute_query(statement).fetchall()
