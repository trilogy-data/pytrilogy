from preql.core.models import Concept, Select, Environment, Grain, Datasource, QueryDatasource, SelectItem, CTE
from preql.core.query_processor import get_datasource_by_concept_and_grain, ReferenceGraph, datasource_to_ctes, get_query_datasources, process_query
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
                               test_environment.concepts['category_name'],
                               test_environment.concepts['total_revenue']])
    concepts, datasources = get_query_datasources(environment=test_environment, graph=test_environment_graph,
                                                 statement = select)
    assert set([datasource.identifier for datasource in datasources.values()]) == {'products_join_revenue<category_id>', 'category<category_id>',
                                                                                  }

    joined_datasource:QueryDatasource = [ds for ds in datasources.values() if ds.identifier =='products_join_revenue<category_id>' ][0]
    assert set([c.name for c in joined_datasource.input_concepts]) == {'product_id','category_id', 'revenue'}
    assert set([c.name for c in joined_datasource.output_concepts]) == {'total_revenue','category_id'}

    ctes = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)


    assert len(ctes) == 4

    join_ctes = [cte for cte in ctes if cte.name == 'cte_products_join_revenuecategory_id_4899990339111153']
    assert join_ctes
    join_cte:CTE = join_ctes[0]
    assert len(join_cte.source.datasources) == 2
    assert set([c.name for c in join_cte.related_columns]) == {'product_id','category_id', 'revenue'}
    assert set([c.name for c in join_cte.output_columns]) == {'total_revenue','category_id'}


    from preql.dialect.sql_server import render_concept_sql
    for cte in ctes:
        assert len(cte.output_columns)>0
        print(cte.name)
        # if 'default.revenue' in cte.source_map.keys():
        #     assert 'default.total_revenue' not in cte.source_map.keys()
        print(cte.source_map)
        print(cte.output_columns)
        if 'default.revenue' in cte.source_map.keys() and 'revenue' not in cte.name:
            raise ValueError


def test_full_query(test_environment, test_environment_graph):
    select = Select(selection=[test_environment.concepts['category_id'],
                               test_environment.concepts['category_name'],
                               test_environment.concepts['total_revenue']])

    processed = process_query(statement=select, environment=test_environment)

    assert set(c.name for c in processed.output_columns) == {'category_id', 'category_name', 'total_revenue'}
