from preql.core.processing.concept_strategies_v2 import GroupNode, gen_select_node


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
            gen_select_node(
                concept=revenue,
                local_optional=[category],
                environment=test_environment,
                g=test_environment_graph,
                depth=0,
            )
        ],
    )
    group_node.resolve()
