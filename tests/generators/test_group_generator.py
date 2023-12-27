from preql.core.processing.node_generators import gen_group_node
from preql.core.processing.nodes import GroupNode
from preql.core.processing.concept_strategies_v2 import source_concepts
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from preql.core.processing.nodes import MergeNode
from preql.core.models import Environment
from preql.core.enums import PurposeLineage


def test_gen_group_node_parents(test_environment: Environment, test_environment_graph):
    comp = test_environment.concepts["category_top_50_revenue_products"]
    assert comp.derivation == PurposeLineage.AGGREGATE

    assert test_environment.concepts["category_id"] in comp.lineage.concept_arguments
    assert comp.grain.components == [test_environment.concepts["category_id"]]
    assert comp.lineage.by == [test_environment.concepts["category_id"]]
    parents = resolve_function_parent_concepts(comp)
    # parents should be both the value and the category
    assert len(parents) == 2
    assert test_environment.concepts["category_id"] in parents


def test_gen_group_node_basic(test_environment, test_environment_graph):
    # from preql.core.models import AggregateWrapper
    prod = test_environment.concepts["product_id"]
    rev = test_environment.concepts["revenue"]
    prod_r = test_environment.concepts["total_revenue"]
    gnode = gen_group_node(
        concept=prod_r,
        local_optional=[prod],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=source_concepts,
    )
    assert isinstance(gnode, GroupNode)
    assert {x.address for x in gnode.input_concepts} == {rev.address, prod.address}
    assert {x.address for x in gnode.output_concepts} == {prod_r.address, prod.address}
    assert gnode.resolve().source_map.keys() == {
        prod_r.address,
        prod.address,
        rev.address,
    }
    assert len(gnode.parents) == 1
    parent = gnode.parents[0]
    assert isinstance(parent, MergeNode)
    assert len(parent.all_concepts) == 2


def test_gen_group_node(test_environment, test_environment_graph):
    # from preql.core.models import AggregateWrapper
    cat = test_environment.concepts["category_id"]
    test_environment.concepts["category_top_50_revenue_products"]
    immediate_aggregate_input = test_environment.concepts[
        "products_with_revenue_over_50"
    ]
    gnode = gen_group_node(
        concept=test_environment.concepts["category_top_50_revenue_products"],
        local_optional=[cat],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=source_concepts,
    )
    assert len(gnode.parents) == 1
    parent = gnode.parents[0]
    assert isinstance(parent, MergeNode)
    assert len(parent.all_concepts) == 2
    assert cat in parent.all_concepts
    assert immediate_aggregate_input in parent.all_concepts

    # check that the parent is a merge node
    resolved_parent = parent.resolve()
    assert len(resolved_parent.joins) == 1
    join = resolved_parent.joins[0]
    assert join.left_datasource.output_concepts == [cat]
    assert (
        list(join.left_datasource.datasources)[0].identifier
        == test_environment.datasources["category"].identifier
    )
    assert join.left_datasource.partial_concepts == []
    # check that the parent merge node is using the right sources given partial flags
    assert cat in join.right_datasource.partial_concepts
    assert {x.identifier for x in resolved_parent.source_map[cat.address]} == {
        join.left_datasource.identifier
    }
