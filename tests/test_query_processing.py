from trilogy.core.enums import (
    SourceType,
)
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.query_processor import get_query_datasources, process_query
from trilogy.core.statements.author import SelectStatement


def test_direct_select(test_environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    product = test_environment.concepts["product_id"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    # test_environment = test_environment.materialize_for_select({})
    datasource = search_concepts(
        [product] + [test_environment.concepts[c] for c in product.grain.components],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    ).resolve()

    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products"
    }


def test_get_datasource_from_window_function(
    test_environment: Environment, test_environment_graph
):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    product_rank = test_environment.concepts["product_revenue_rank"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    # assert product_rank.grain.components[0] == test_environment.concepts['name']
    node = search_concepts(
        [product_rank]
        + [test_environment.concepts[c] for c in product_rank.grain.components],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    )

    # raise SyntaxError(product_rank.grain)
    datasource = node.resolve()
    assert product_rank in datasource.output_concepts
    # assert datasource.grain == product_rank.grain
    assert isinstance(datasource, QueryDatasource)
    assert datasource.grain.components == product_rank.grain.components

    product_rank_by_category = test_environment.concepts[
        "product_revenue_rank_by_category"
    ]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = search_concepts(
        [product_rank_by_category]
        + [
            test_environment.concepts[c]
            for c in product_rank_by_category.grain.components
        ],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    ).resolve()
    assert product_rank_by_category in datasource.output_concepts
    # assert datasource.grain == product_rank_by_category.grain

    assert isinstance(datasource, QueryDatasource)


def test_get_datasource_for_filter(
    test_environment: Environment, test_environment_graph
):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    hi_rev_product = test_environment.concepts["products_with_revenue_over_50"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph

    assert {n.name for n in hi_rev_product.sources} == {
        "total_revenue",
        "revenue",
        "product_id",
    }
    datasource = search_concepts(
        [hi_rev_product]
        + [test_environment.concepts[c] for c in hi_rev_product.grain.components],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    )
    datasource = datasource.resolve()
    assert isinstance(datasource, QueryDatasource)
    assert hi_rev_product in datasource.output_concepts


def test_select_output(test_environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    product = test_environment.concepts["product_id"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph

    datasource = search_concepts(
        [product] + [test_environment.concepts[c] for c in product.grain.components],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    ).resolve()

    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {
        "products"
    }


def test_basic_aggregate(test_environment: Environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    product = test_environment.concepts["product_id"]
    total_revenue = test_environment.concepts["total_revenue"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = search_concepts(
        [total_revenue.with_grain(product), product],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    )
    datasource = datasource.resolve()
    assert isinstance(datasource, QueryDatasource)
    assert datasource.grain == BuildGrain(components=[product])


def test_join_aggregate(test_environment: Environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    category_id = test_environment.concepts["category_id"]
    total_revenue = test_environment.concepts["total_revenue"]
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = search_concepts(
        [
            total_revenue.with_grain(BuildGrain(components={"local.category_id"})),
            category_id,
        ],
        history=history,
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
    ).resolve()
    assert isinstance(datasource, QueryDatasource)
    assert datasource.source_type == SourceType.GROUP
    assert len(set([datasource.name for datasource in datasource.datasources])) == 1
    assert datasource.grain.components == {"local.category_id"}


def test_query_aggregation(test_environment, test_environment_graph):
    select = SelectStatement(selection=[test_environment.concepts["total_revenue"]])
    datasource = get_query_datasources(environment=test_environment, statement=select)

    assert {datasource.identifier} == {"revenue_at_local_order_id_at_abstract"}
    check = datasource
    assert len(check.input_concepts) == 2
    assert check.input_concepts[0].name == "revenue"
    assert len(check.output_concepts) == 1
    assert check.output_concepts[0].name == "total_revenue"


def test_query_datasources(test_environment, test_environment_graph):
    select = SelectStatement(
        selection=[
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )
    get_query_datasources(environment=test_environment, statement=select)


def test_full_query(test_environment, test_environment_graph):
    select = SelectStatement(
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
