from trilogy.core.enums import ComparisonOperator, PurposeLineage
from trilogy.core.models import Comparison, BoundEnvironment, WhereClause
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.node_generators import gen_filter_node
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)


def test_gen_filter_node_parents(test_environment: BoundEnvironment, test_environment_graph):
    comp = test_environment.concepts["products_with_revenue_over_50"]
    assert comp.derivation == PurposeLineage.FILTER

    assert comp.lineage
    assert test_environment.concepts["product_id"] in comp.lineage.concept_arguments
    # assert test_environment.concepts["total_revenue"] in comp.lineage.concept_arguments
    filtered, row_parents, existence_parents = resolve_filter_parent_concepts(
        comp, environment=test_environment
    )
    # parents should be both the value and the category
    assert filtered == test_environment.concepts["product_id"]
    assert len(row_parents) == 2
    assert test_environment.concepts["product_id"] in row_parents
    # assert test_environment.concepts["total_revenue"] in parents


def test_gen_filter_node(test_environment, test_environment_graph):
    # from trilogy.core.models import AggregateWrapper

    _ = gen_filter_node(
        concept=test_environment.concepts["products_with_revenue_over_50"],
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )


def test_gen_filter_node_same_concept(test_environment, test_environment_graph):
    conditional = Comparison(
        left=test_environment.concepts["category_name"],
        operator=ComparisonOperator.LIKE,
        right="%abc%",
    )
    node = gen_filter_node(
        concept=test_environment.concepts["product_id"].with_filter(
            conditional, test_environment
        ),
        local_optional=[
            test_environment.concepts["category_id"].with_filter(
                conditional, test_environment
            )
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )
    assert node.conditions == conditional


def test_gen_filter_node_include_all(test_environment, test_environment_graph):
    conditional = Comparison(
        left=test_environment.concepts["category_name"],
        operator=ComparisonOperator.LIKE,
        right="%abc%",
    )
    node = gen_filter_node(
        concept=test_environment.concepts["product_id"].with_filter(
            conditional, test_environment
        ),
        local_optional=[
            test_environment.concepts["category_id"].with_filter(
                conditional, test_environment
            )
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        conditions=WhereClause(conditional=conditional),
    )
    assert (
        node.conditions == conditional
    ), f"{str(node.conditions)} vs {str(conditional)}"
