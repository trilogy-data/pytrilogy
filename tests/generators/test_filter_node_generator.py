from trilogy.core.enums import ComparisonOperator, Derivation, FunctionType, Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.author import Comparison, Concept, Function
from trilogy.core.models.build import BuildWhereClause, Factory
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators import gen_filter_node
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)
from trilogy.core.processing.node_generators.filter_node import build_parent_concepts
from trilogy.core.processing.nodes import MergeNode
from trilogy.hooks import DebuggingHook


def test_gen_filter_node_parents(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    comp = test_environment.concepts["products_with_revenue_over_50"]
    assert comp.derivation == Derivation.FILTER

    assert comp.lineage
    assert test_environment.concepts["product_id"] in comp.lineage.concept_arguments
    # assert test_environment.concepts["total_revenue"] in comp.lineage.concept_arguments
    row_parents, _existence_parents = resolve_filter_parent_concepts(
        comp, environment=test_environment
    )
    # parents should be both the value and the category
    assert row_parents[0] == test_environment.concepts["product_id"]
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
    DebuggingHook()
    history = History(base_environment=test_environment)
    factory = Factory(environment=test_environment)
    conditional = Comparison(
        left=test_environment.concepts["category_name"],
        operator=ComparisonOperator.LIKE,
        right="%abc%",
    )
    f_product_id = test_environment.concepts["product_id"].with_filter(conditional)
    f_concept_id = test_environment.concepts["category_id"].with_filter(conditional)
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
    ), f"{node.conditions!s} vs {conditional!s}"


def test_gen_filter_node_includes_equivalent_optional(
    test_environment, test_environment_graph
):
    history = History(base_environment=test_environment)
    conditional = Comparison(
        left=test_environment.concepts["revenue"],
        operator=ComparisonOperator.GT,
        right=5,
    )
    filtered_order = test_environment.concepts["order_id"].with_filter(conditional)
    test_environment.add_concept(filtered_order)
    test_environment = test_environment.materialize_for_select()

    node = gen_filter_node(
        concept=test_environment.concepts[filtered_order.address],
        local_optional=[
            test_environment.alias_origin_lookup["local.alt_order_id"],
        ],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )

    assert node is not None
    assert not isinstance(node, MergeNode)
    assert "local.alt_order_id" in [x.address for x in node.output_concepts]


def test_gen_filter_node_includes_adjacent_sibling_filter_content(test_environment):
    history = History(base_environment=test_environment)
    other_sales_price = Concept(
        name="other_sales_price",
        datatype=DataType.FLOAT,
        purpose=Purpose.PROPERTY,
        lineage=Function(
            arguments=[test_environment.concepts["revenue"], 2.0],
            output_datatype=DataType.FLOAT,
            output_purpose=Purpose.PROPERTY,
            operator=FunctionType.MULTIPLY,
        ),
    )
    conditional = Comparison(
        left=test_environment.concepts["category_name"],
        operator=ComparisonOperator.LIKE,
        right="%abc%",
    )
    filtered_revenue = test_environment.concepts["revenue"].with_filter(conditional)
    filtered_other_sales_price = other_sales_price.with_filter(conditional)
    test_environment.add_concept(other_sales_price)
    test_environment.add_concept(filtered_revenue)
    test_environment.add_concept(filtered_other_sales_price)
    build_environment = test_environment.materialize_for_select()
    build_filtered_other = build_environment.concepts[
        filtered_other_sales_price.address
    ]

    parent_plan = build_parent_concepts(
        concept=build_environment.concepts[filtered_revenue.address],
        local_optional=[build_filtered_other],
        environment=build_environment,
    )

    assert parent_plan.optimized_pushdown is True
    assert build_filtered_other in parent_plan.same_filter_optional

    node = gen_filter_node(
        concept=build_environment.concepts[filtered_revenue.address],
        local_optional=[
            build_filtered_other,
        ],
        environment=build_environment,
        g=generate_graph(build_environment),
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )

    assert node is not None
    # requested outputs lead; the tail is the retained row-parent grain
    # (_row_grain_outputs) so a downstream fuse joins on it, not on
    # whatever incidental columns survive narrowing
    assert [x.address for x in node.output_concepts] == [
        filtered_revenue.address,
        filtered_other_sales_price.address,
        "local.category_name",
        "local.revenue",
        "local.other_sales_price",
    ]
