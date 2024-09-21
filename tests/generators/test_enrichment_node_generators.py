from trilogy.core.processing.node_generators.common import gen_property_enrichment_node
from trilogy.core.processing.node_generators.group_node import gen_group_node
from trilogy.core.processing.concept_strategies_v3 import search_concepts


def address_set(concepts):
    return set([c.address for c in concepts])


def test_gen_property_enrichment_node(test_environment, test_environment_graph):

    prod = test_environment.concepts["category_id"]
    prod_r = test_environment.concepts["total_revenue"]
    gnode = gen_group_node(
        concept=prod_r,
        local_optional=[prod],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )

    node = gen_property_enrichment_node(
        base_node=gnode,
        extra_properties=[test_environment.concepts["category_name"]],
        environment=test_environment,
        g=test_environment_graph,
        depth=1,
        source_concepts=search_concepts,
        log_lambda=lambda x: x,
    )

    assert address_set(node.output_concepts) == address_set(
        [
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )
