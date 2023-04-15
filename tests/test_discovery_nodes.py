from preql.core.processing.concept_strategies_v2 import GroupNode


def test_group_node(test_environment):
    revenue = test_environment.concepts['total_revenue']
    product = test_environment.concepts['product_name']
    group_node = GroupNode(required_concepts = [revenue, product], environment=test_environment)

    resolved = group_node.resolve()