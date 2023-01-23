from preql.core.models import Select, Grain, QueryDatasource, CTE
from preql.core.query_processor import (
    get_datasource_by_concept_and_grain,
    datasource_to_ctes,
    get_query_datasources,
    process_query,
)


def test_select_output(test_environment, test_environment_graph):
    product = test_environment.concepts["product_id"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph

    for item in product.grain.components:
        print(item)
    datasource = get_datasource_by_concept_and_grain(
        product,
        grain=product.grain,
        environment=test_environment,
        g=test_environment_graph,
    )

    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products"
    }


def test_basic_aggregate(test_environment, test_environment_graph):
    product = test_environment.concepts["product_id"]
    total_revenue = test_environment.concepts["total_revenue"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        total_revenue,
        grain=Grain(components=[product]),
        environment=test_environment,
        g=test_environment_graph,
    )
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "revenue"
    }


def test_join_aggregate(test_environment, test_environment_graph):
    category_id = test_environment.concepts["category_id"]
    total_revenue = test_environment.concepts["total_revenue"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        total_revenue,
        grain=Grain(components=[category_id]),
        environment=test_environment,
        g=test_environment_graph,
    )
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "revenue",
        "products",
    }


def test_query_aggregation(test_environment, test_environment_graph):
    select = Select(selection=[test_environment.concepts["total_revenue"]])
    concepts, datasources = get_query_datasources(
        environment=test_environment, graph=test_environment_graph, statement=select
    )

    assert set([datasource.identifier for datasource in datasources.values()]) == {
        "revenue_at_abstract"
    }
    check = list(datasources.values())[0]
    assert len(check.input_concepts) == 1
    assert check.input_concepts[0].name == "revenue"
    assert len(check.output_concepts) == 1
    assert check.output_concepts[0].name == "total_revenue"
    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)

    assert len(ctes) == 1

    for cte in ctes:
        print(cte.output_columns)
        assert len(cte.output_columns) == 1
        assert cte.output_columns[0].name == "total_revenue"


def test_query_datasources(test_environment, test_environment_graph):
    select = Select(
        selection=[
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )
    concepts, datasources = get_query_datasources(
        environment=test_environment, graph=test_environment_graph, statement=select
    )
    assert set([datasource.identifier for datasource in datasources.values()]) == {
        "products_revenue_at_category_id",
        "category_at_category_id",
    }

    joined_datasource: QueryDatasource = [
        ds
        for ds in datasources.values()
        if ds.identifier == "products_revenue_at_category_id"
    ][0]
    assert set([c.name for c in joined_datasource.input_concepts]) == {
        "product_id",
        "category_id",
        "revenue",
    }
    assert set([c.name for c in joined_datasource.output_concepts]) == {
        "total_revenue",
        "category_id",
    }

    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)

    assert len(ctes) == 4
    join_ctes = [
        cte
        for cte in ctes
        if cte.name == "cte_products_revenue_at_category_id_8908257907118235"
    ]
    assert join_ctes
    join_cte: CTE = join_ctes[0]
    assert len(join_cte.source.datasources) == 2
    assert set([c.name for c in join_cte.related_columns]) == {
        "product_id",
        "category_id",
        "revenue",
    }
    assert set([c.name for c in join_cte.output_columns]) == {
        "total_revenue",
        "category_id",
    }

    for cte in ctes:
        assert len(cte.output_columns) > 0
        if "default.revenue" in cte.source_map.keys() and "revenue" not in cte.name:
            raise ValueError


def test_full_query(test_environment, test_environment_graph):
    select = Select(
        selection=[
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )

    processed = process_query(statement=select, environment=test_environment)

    assert set(c.name for c in processed.output_columns) == {
        "category_id",
        "category_name",
        "total_revenue",
    }
