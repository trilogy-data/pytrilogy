from preql.core.processing.node_generators import gen_unnest_node
from preql.core.processing.concept_strategies_v2 import source_concepts
from preql.core.models import Environment


def test_gen_unnest_node_parents(test_environment: Environment, test_environment_graph):
    pass


def test_gen_unnest_node(test_environment, test_environment_graph):
    gen_unnest_node(
        concept=test_environment.concepts["unnest_literal_array"],
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=source_concepts,
    )
