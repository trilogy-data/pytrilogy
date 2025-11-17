from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators import gen_synonym_node


def test_gen_pseudonym_node(test_environment: Environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment_build = test_environment.materialize_for_select()
    test_concepts = [
        test_environment_build.alias_origin_lookup["local.alt_order_id"],
        test_environment_build.concepts["revenue"],
    ]
    gen_synonym_node(
        all_concepts=test_concepts,
        environment=test_environment_build,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )
