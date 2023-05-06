from preql.core.processing.concept_strategies_v2 import GroupNode


def test_group_node(test_environment, test_environment_graph):
    revenue = test_environment.concepts["total_revenue"]
    category = test_environment.concepts["category_name"]
    group_node = GroupNode(
        mandatory_concepts=[revenue, category],
        optional_concepts=[],
        environment=test_environment,
        g=test_environment_graph,
    )

    group_node.resolve()
