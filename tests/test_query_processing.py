from preql.core.models import Concept, Select, Environment, Grain, Datasource, QueryDatasource
from preql.core.query_processor import get_datasource_by_concept_and_grain, ReferenceGraph, node_to_cte, datasource_to_cte
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



def test_query_datasources(test_environment, test_environment_graph):
