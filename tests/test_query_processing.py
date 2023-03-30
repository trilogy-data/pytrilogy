from preql.core.models import Select, QueryDatasource, CTE
from preql.core.processing.concept_strategies_v2 import source_concepts
from preql.core.query_processor import (
    datasource_to_ctes,
    process_query,
    get_query_datasources_v2
)
from preql.hooks.query_debugger import print_recursive_nodes, print_recursive_resolved


def test_direct_select(test_environment, test_environment_graph):
    product = test_environment.concepts["product_id"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = source_concepts(
        [product],
        product.grain.components_copy,
        environment=test_environment,
        g=test_environment_graph,
    ).resolve()

    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products"
    }


def test_get_datasource_from_window_function(test_environment, test_environment_graph):
    # test without grouping
    product_rank = test_environment.concepts["product_revenue_rank"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    # assert product_rank.grain.components[0] == test_environment.concepts['name']
    datasource = source_concepts(
        [product_rank],
        product_rank.grain.components_copy,
        environment=test_environment,
        g=test_environment_graph,
    ).resolve()
    assert product_rank in datasource.output_concepts
    # assert datasource.grain == product_rank.grain
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "revenue_at_local_order_id_at_local_product_id"
    }

    product_rank_by_category = test_environment.concepts[
        "product_revenue_rank_by_category"
    ]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = source_concepts(
        [product_rank_by_category],
        product_rank_by_category.grain.components_copy,
        environment=test_environment,
        g=test_environment_graph,
    ).resolve()
    assert product_rank_by_category in datasource.output_concepts
    # assert datasource.grain == product_rank_by_category.grain

    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products_join_revenue_at_local_product_id_local_order_id_at_local_product_id_local_category_id"
    }


def test_get_datasource_for_filter(test_environment, test_environment_graph):
    product = test_environment.concepts["products_with_revenue_over_50"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph

    assert {n.name for n in product.sources} == {
        "total_revenue",
        "revenue",
        "product_id",
    }
    datasource = source_concepts(
        [product],
        product.grain.components_copy,
        environment=test_environment,
        g=test_environment_graph,
    )
    print_recursive_nodes(datasource)
    datasource = datasource.resolve()
    print_recursive_resolved(datasource)
    assert isinstance(datasource, QueryDatasource)
    assert datasource.output_concepts == [product]
    # assert set([datasource.name for datasource in datasource.datasources]) == {
    #     "products"
    # }


def test_select_output(test_environment, test_environment_graph):
    product = test_environment.concepts["product_id"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph

    datasource = source_concepts(
        [product],
        product.grain.components_copy,
        environment=test_environment,
        g=test_environment_graph,
    ).resolve()

    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products"
    }


def test_basic_aggregate(test_environment, test_environment_graph):
    product = test_environment.concepts["product_id"]
    total_revenue = test_environment.concepts["total_revenue"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = source_concepts(
        [total_revenue],
        [product],
        environment=test_environment,
        g=test_environment_graph,
    )
    print_recursive_nodes(datasource)
    datasource = datasource.resolve()
    print_recursive_resolved(datasource)
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "revenue_at_local_order_id"
    }


def test_join_aggregate(test_environment, test_environment_graph):
    category_id = test_environment.concepts["category_id"]
    total_revenue = test_environment.concepts["total_revenue"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = source_concepts(
        [total_revenue],
        [category_id],
        environment=test_environment,
        g=test_environment_graph,
    ).resolve()
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products_join_revenue_at_local_product_id_local_order_id"
    }


def test_query_aggregation(test_environment, test_environment_graph):
    select = Select(selection=[test_environment.concepts["total_revenue"]])
    concepts, datasources = get_query_datasources_v2(
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
        "products_join_revenue_at_local_category_id",
        "category_at_local_category_id",
    }

    joined_datasource: QueryDatasource = [
        ds
        for ds in datasources.values()
        if ds.identifier == "products_join_revenue_at_local_category_id"
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

    assert len(ctes) == 2

    join_ctes = [
        cte
        for cte in ctes
        if cte.name.startswith("cte_products_join_revenue_at_local_category_i")
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
