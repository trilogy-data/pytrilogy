from preql.core.processing.node_generators import gen_filter_node
from preql.core.processing.concept_strategies_v3 import search_concepts
from preql.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)
from preql.core.models import Environment
from preql.core.enums import PurposeLineage


def test_gen_filter_node_parents(test_environment: Environment, test_environment_graph):
    comp = test_environment.concepts["products_with_revenue_over_50"]
    assert comp.derivation == PurposeLineage.FILTER

    assert comp.lineage
    assert test_environment.concepts["product_id"] in comp.lineage.concept_arguments
    # assert test_environment.concepts["total_revenue"] in comp.lineage.concept_arguments
    filtered, parents = resolve_filter_parent_concepts(comp)
    # parents should be both the value and the category
    assert filtered == test_environment.concepts["product_id"]
    assert len(parents) == 2
    assert test_environment.concepts["product_id"] in parents
    # assert test_environment.concepts["total_revenue"] in parents


def test_gen_filter_node(test_environment, test_environment_graph):
    # from preql.core.models import AggregateWrapper

    _ = gen_filter_node(
        concept=test_environment.concepts["products_with_revenue_over_50"],
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )
