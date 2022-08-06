from preql.core.models import Concept, Select, Environment, Grain, Datasource, QueryDatasource, SelectItem
from preql.core.query_processor import get_datasource_by_concept_and_grain, ReferenceGraph, datasource_to_ctes, get_query_datasources
from typing import List

def test_select_output(test_environment, test_environment_graph):
    product = test_environment.concepts['product_id']
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        product, grain = product.grain,
        environment = test_environment,
        g = test_environment_graph,
    )
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {'products',}


def test_basic_aggregate(test_environment, test_environment_graph):
    product = test_environment.concepts['product_id']
    total_revenue = test_environment.concepts['total_revenue']
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        total_revenue,
        grain = Grain([product]),
        environment = test_environment,
        g = test_environment_graph,
    )
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {'revenue', }


def test_join_aggregate(test_environment, test_environment_graph):
    category_id = test_environment.concepts['category_id']
    total_revenue = test_environment.concepts['total_revenue']
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        total_revenue,
        grain = Grain([category_id]),
        environment = test_environment,
        g = test_environment_graph,
    )
    assert isinstance(datasource, QueryDatasource)
    assert set([datasource.name for datasource in datasource.datasources]) == {'revenue', 'products',}


def test_query_aggregation(test_environment, test_environment_graph):
    select = Select(selection=[test_environment.concepts['total_revenue']])
    concepts, datasources = get_query_datasources(environment=test_environment, graph=test_environment_graph,
                                                  statement = select)

    assert set([datasource.identifier for datasource in datasources.values()]) == {'revenue<>',}
    check = list(datasources.values())[0]
    assert len(check.input_concepts) == 1
    assert check.input_concepts[0].name == 'revenue'
    assert len(check.output_concepts) == 1
    assert check.output_concepts[0].name == 'total_revenue'
    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)


    assert len(ctes) == 1

    for cte in ctes:
        print(cte.output_columns)
        assert len(cte.output_columns)==1
        assert cte.output_columns[0].name == 'total_revenue'



def test_query_datasources(test_environment, test_environment_graph):
    select = Select(selection=[test_environment.concepts['category_id'],
                               test_environment.concepts['total_revenue']])
    concepts, datasources = get_query_datasources(environment=test_environment, graph=test_environment_graph,
                                                 statement = select)
    assert set([datasource.identifier for datasource in datasources.values()]) == {'revenue_join_products<category_id>', 'category<category_id>',
                                                                                  }

    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)


    assert len(ctes) == 4
    from preql.dialect.sql_server import render_concept_sql
    for cte in ctes:
        assert len(cte.output_columns)>0
        print(cte.name)
        # if 'default.revenue' in cte.source_map.keys():
        #     assert 'default.total_revenue' not in cte.source_map.keys()
        print(cte.source_map)
        for concept in cte.output_columns:
            print(render_concept_sql(concept, cte))