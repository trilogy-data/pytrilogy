from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import search_concepts, History
from trilogy.core.processing.node_generators import gen_unnest_node


def test_gen_unnest_node_parents(test_environment: Environment, test_environment_graph):
    pass


def test_gen_unnest_node(test_environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    gen_unnest_node(
        concept=test_environment.concepts["unnest_literal_array"],
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history
    )
