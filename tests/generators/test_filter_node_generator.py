from trilogy.core.enums import ComparisonOperator, Derivation
from trilogy.core.models.author import Comparison
from trilogy.core.models.build import BuildWhereClause, Factory
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators import gen_filter_node
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)


def test_gen_filter_node_parents(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    comp = test_environment.concepts["products_with_revenue_over_50"]
    assert comp.derivation == Derivation.FILTER

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
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    _ = gen_filter_node(
        concept=test_environment.concepts["products_with_revenue_over_50"],
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )


def test_gen_filter_node_same_concept(test_environment, test_environment_graph):
    history = History(base_environment=test_environment)
    factory = Factory(environment=test_environment)
    conditional = Comparison(
        left=test_environment.concepts["category_name"],
        operator=ComparisonOperator.LIKE,
        right="%abc%",
    )
    f_product_id = test_environment.concepts["product_id"].with_filter(conditional)
    f_concept_id = test_environment.concepts["category_id"].with_filter(conditional)
    og_test_environment = test_environment
    test_environment.add_concept(f_product_id)
    test_environment.add_concept(f_concept_id)
    test_environment = test_environment.materialize_for_select()

    node = gen_filter_node(
        concept=test_environment.concepts[f_product_id.address],
        local_optional=[
            test_environment.concepts[f_concept_id.address],
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )
    assert node.conditions == factory.build(conditional)


def test_gen_filter_node_include_all(test_environment, test_environment_graph):
    history = History(base_environment=test_environment)
    factory = Factory(environment=test_environment)
    conditional = Comparison(
        left=test_environment.concepts["category_name"],
        operator=ComparisonOperator.LIKE,
        right="%abc%",
    )
    f_product_id = test_environment.concepts["product_id"].with_filter(conditional)
    f_concept_id = test_environment.concepts["category_id"].with_filter(conditional)
    og_test_environment = test_environment
    test_environment.add_concept(f_product_id)
    test_environment.add_concept(f_concept_id)
    build_conditional = factory.build(conditional)
    test_environment = test_environment.materialize_for_select()
    node = gen_filter_node(
        concept=test_environment.concepts[f_product_id.address],
        local_optional=[
            test_environment.concepts[f_concept_id.address],
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        conditions=BuildWhereClause(conditional=build_conditional),
        history=history,
    )
    assert (
        node.conditions == build_conditional
    ), f"{str(node.conditions)} vs {str(conditional)}"
