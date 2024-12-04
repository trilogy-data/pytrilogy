from trilogy.core.enums import FunctionType, Purpose, PurposeLineage
from trilogy.core.models import AggregateWrapper, DataType, Environment, Function
from trilogy.core.processing.concept_strategies_v3 import search_concepts
from trilogy.core.processing.node_generators import gen_group_node
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import GroupNode, MergeNode
from trilogy.parsing.common import agg_wrapper_to_concept, function_to_concept


def test_gen_group_node_parents(test_environment: Environment, test_environment_graph):
    comp = test_environment.concepts["category_top_50_revenue_products"]
    assert comp.derivation == PurposeLineage.AGGREGATE
    assert comp.lineage
    assert test_environment.concepts["category_id"] in comp.lineage.concept_arguments
    assert comp.grain.components == [test_environment.concepts["category_id"]]
    assert isinstance(comp.lineage, AggregateWrapper)
    assert comp.lineage.by == [test_environment.concepts["category_id"]]
    parents = resolve_function_parent_concepts(comp)
    # parents should be both the value and the category
    assert len(parents) == 2
    assert test_environment.concepts["category_id"] in parents


def test_gen_group_node_basic(test_environment, test_environment_graph):
    # from trilogy.core.models import AggregateWrapper
    prod = test_environment.concepts["product_id"]
    test_environment.concepts["revenue"]
    prod_r = test_environment.concepts["total_revenue"]
    gnode = gen_group_node(
        concept=prod_r,
        local_optional=[prod],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )
    assert isinstance(gnode, (GroupNode, MergeNode))
    assert {x.address for x in gnode.output_concepts} == {prod_r.address, prod.address}


def test_gen_group_node(test_environment: Environment, test_environment_graph):
    # from trilogy.core.models import AggregateWrapper
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    cat = test_environment.concepts["category_id"]
    test_environment.concepts["category_top_50_revenue_products"]
    immediate_aggregate_input = test_environment.concepts[
        "products_with_revenue_over_50"
    ]
    gnode = gen_group_node(
        concept=test_environment.concepts["category_top_50_revenue_products"],
        local_optional=[],
        # local_optional=[cat],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
    )
    assert len(gnode.parents) == 1
    parent = gnode.parents[0]
    assert isinstance(parent, MergeNode)
    assert len(parent.all_concepts) == 2
    assert cat in parent.all_concepts
    assert immediate_aggregate_input in parent.all_concepts
    assert cat not in gnode.partial_concepts
    assert cat in gnode.all_concepts

    # check that the parent is a merge node
    parent.resolve()


def test_proper_parents(test_environment):
    base = Function(
        operator=FunctionType.COUNT,
        arguments=[test_environment.concepts["category_name"]],
        output_purpose=Purpose.PROPERTY,
        output_datatype=DataType.INTEGER,
    )

    resolved = resolve_function_parent_concepts(
        function_to_concept(base, name="base_agg", namespace="local"),
    )
    assert len(resolved) == 2
    assert test_environment.concepts["category_id"] in resolved
    resolved = resolve_function_parent_concepts(
        agg_wrapper_to_concept(
            AggregateWrapper(
                function=base,
                by=[test_environment.concepts["category_name"]],
            ),
            name="agg_to_alt_grain",
            namespace="local",
        )
    )

    assert len(resolved) == 2
    assert test_environment.concepts["category_name"] in resolved
    assert test_environment.concepts["category_id"] in resolved
