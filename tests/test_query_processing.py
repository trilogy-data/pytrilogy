from preql.core.models import Concept, Select, Environment, Grain
from preql.core.query_processor import get_datasource_by_concept_and_grain, ReferenceGraph

def test_select_output(test_environment, test_environment_graph):
    product = test_environment.concepts['product_id']
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        product, grain = product.grain,
        environment = test_environment,
        g = test_environment_graph,
        query_graph = ReferenceGraph()
    )
    assert datasource.name == 'products'


def test_basic_aggregate(test_environment, test_environment_graph):
    product = test_environment.concepts['product_id']
    total_revenue = test_environment.concepts['total_revenue']
    #        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
    datasource = get_datasource_by_concept_and_grain(
        total_revenue, grain = Grain([product]),
        environment = test_environment,
        g = test_environment_graph,
        query_graph = ReferenceGraph()
    )
    assert datasource.name == 'products'