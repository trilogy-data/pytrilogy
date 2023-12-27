from preql.core.models import Environment


def test_gen_select_node_parents(test_environment: Environment, test_environment_graph):
    test_environment.concepts["category_top_50_revenue_products"]
    test_environment.concepts["category_id"]


# def test_gen_select_node_from_join(test_environment:Environment, test_environment_graph):
#     # ensure that when we get a select node requiring a join
#     #
#     cat_count = test_environment.concepts["category_top_50_revenue_products"]
#     cat = test_environment.concepts['category_id']

#     gnode = gen_select_node(
#         concept=cat_count,
#         local_optional=[cat],
#         environment=test_environment,
#         g=test_environment_graph,
#         depth=0,
#         source_concepts=source_concepts,
#     )

#     # check the node we got
#     assert gnode.partial_concepts == []
#     assert gnode.all_concepts == [cat_count, cat]
#     output_resolved = gnode.resolve()
#     assert output_resolved.output_concepts == [cat_count, cat]
#     assert len(gnode.parents) == 1

#     # now go upstream
#     parent_group_node = gnode.parents[0]
#     assert isinstance(parent_group_node, MergeNode)
#     resolved = parent_group_node.resolve()
#     assert len(resolved.datasources) == 2
#     assert set(resolved.source_map.keys()) == {'local.category_top_50_revenue_products', 'local.category_id'}
#     assert len(resolved.source_map['local.category_top_50_revenue_products']) == 1
#     assert list(resolved.source_map['local.category_top_50_revenue_products'])[0].partial_concepts == [cat]
