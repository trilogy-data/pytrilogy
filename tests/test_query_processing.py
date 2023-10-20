from preql.core.models import Select, QueryDatasource
from preql.core.processing.concept_strategies_v2 import source_concepts
from preql.core.query_processor import process_query, get_query_datasources


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
        "products_at_local_product_id_join_revenue_at_local_order_id_at_local_product_id_local_order_id_at_local_product_id_local_category_id"
    }


def test_get_datasource_for_filter(test_environment, test_environment_graph):
    hi_rev_product = test_environment.concepts["products_with_revenue_over_50"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph

    assert {n.name for n in hi_rev_product.sources} == {
        "total_revenue",
        "revenue",
        "product_id",
    }
    datasource = source_concepts(
        [hi_rev_product],
        hi_rev_product.grain.components_copy,
        environment=test_environment,
        g=test_environment_graph,
    )
    datasource = datasource.resolve()
    assert isinstance(datasource, QueryDatasource)
    assert hi_rev_product in datasource.output_concepts


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
    datasource = datasource.resolve()
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
        "products_at_local_product_id_join_revenue_at_local_order_id_at_local_product_id_local_order_id"
    }


def test_query_aggregation(test_environment, test_environment_graph):
    select = Select(selection=[test_environment.concepts["total_revenue"]])
    datasource = get_query_datasources(
        environment=test_environment, graph=test_environment_graph, statement=select
    )

    assert {datasource.identifier} == {"revenue_at_local_order_id_at_abstract"}
    check = datasource
    assert len(check.input_concepts) == 1
    assert check.input_concepts[0].name == "revenue"
    assert len(check.output_concepts) == 1
    assert check.output_concepts[0].name == "total_revenue"


def test_query_datasources(test_environment, test_environment_graph):
    select = Select(
        selection=[
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )
    get_query_datasources(
        environment=test_environment, graph=test_environment_graph, statement=select
    )


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
