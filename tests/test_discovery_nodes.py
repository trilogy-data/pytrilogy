from trilogy.core.processing.concept_strategies_v3 import (
    GroupNode,
    search_concepts,
)
from trilogy.core.processing.node_generators import gen_group_node
from trilogy.core.models import Environment, Grain
from trilogy.core.constants import INTERNAL_NAMESPACE, ALL_ROWS_CONCEPT


def test_group_node(test_environment, test_environment_graph):
    total_revenue = test_environment.concepts["total_revenue"]
    revenue = test_environment.concepts["revenue"]
    category = test_environment.concepts["category_name"]
    group_node = GroupNode(
        output_concepts=[total_revenue, category],
        input_concepts=[category, revenue],
        environment=test_environment,
        g=test_environment_graph,
        parents=[
            search_concepts(
                [category, revenue],
                environment=test_environment,
                g=test_environment_graph,
                depth=0,
            )
        ],
    )
    group_node.resolve()


def test_group_node_property(test_environment: Environment, test_environment_graph):
    sum_name_length = test_environment.concepts["category_name_length_sum"]

    group_node = gen_group_node(
        sum_name_length,
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        source_concepts=search_concepts,
        depth=0,
    )
    input_concept_names = {
        x.name
        for x in group_node.parents[0].output_concepts
        if x not in group_node.parents[0].hidden_concepts
    }
    assert input_concept_names == {"category_name_length", "category_id"}
    final = group_node.resolve()
    assert len(final.datasources) == 1
    assert final.datasources[0].group_required is False


def test_group_node_property_all(test_environment: Environment, test_environment_graph):
    sum_name_length = test_environment.concepts["category_name_length_sum"]
    all_rows = test_environment.concepts[f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"]
    sum_name_length_all_rows = sum_name_length.with_grain(Grain(components=[all_rows]))
    group_node = gen_group_node(
        sum_name_length_all_rows,
        local_optional=[],
        environment=test_environment,
        g=test_environment_graph,
        source_concepts=search_concepts,
        depth=0,
    )
    input_concept_names = {
        x.name
        for x in group_node.parents[0].output_concepts
        if x not in group_node.parents[0].hidden_concepts
    }
    assert input_concept_names == {"category_name_length", "category_id"}
    final = group_node.resolve()
    assert len(final.datasources) == 1
    assert final.datasources[0].group_required is False
