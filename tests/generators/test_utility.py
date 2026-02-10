from networkx import Graph

from trilogy import parse
from trilogy.core.enums import JoinType
from trilogy.core.processing.node_generators.common import (
    # resolve_join_order_v2,
    NodeJoin,
    StrategyNode,
    resolve_join_order,
)
from trilogy.core.processing.utility import (
    JoinOrderOutput,
    ensure_content_preservation,
    resolve_join_order_v2,
)


def test_resolve_join_order():
    env, _ = parse("""
key order_id int;
key product_id int;
property product_id.price float;       
                """)
    env = env.materialize_for_select()
    test_case = []
    x = resolve_join_order(test_case)
    assert x == []
    orders = env.concepts["order_id"]
    price = env.concepts["price"]
    product = env.concepts["product_id"]
    left = StrategyNode(
        input_concepts=[orders], output_concepts=[orders], environment=env
    )
    right = StrategyNode(
        input_concepts=[orders, product],
        output_concepts=[orders, product],
        environment=env,
    )

    right_two = StrategyNode(
        input_concepts=[product, price],
        output_concepts=[product, price],
        environment=env,
    )

    first_join = NodeJoin(
        left_node=left,
        right_node=right,
        join_type=JoinType.INNER,
        concepts=[orders],
    )
    second_join = NodeJoin(
        left_node=right,
        right_node=right_two,
        join_type=JoinType.INNER,
        concepts=[product],
    )
    second_join_inverse = NodeJoin(
        left_node=right_two,
        right_node=right,
        join_type=JoinType.INNER,
        concepts=[product],
    )
    test_case = [second_join, first_join]
    x = resolve_join_order(test_case)
    assert x == [first_join, second_join]

    test_case = [second_join, second_join_inverse]
    try:
        x = resolve_join_order(test_case)
        raise ValueError("test should not get here")
    except Exception as e:
        assert isinstance(e, SyntaxError)


def test_resolve_join_order_v2():
    g = Graph()

    g.add_edge("ds~orders", "c~order_id")
    g.add_edge("ds~orders", "c~product_id")
    g.add_edge("ds~orders", "c~customer_id")
    g.add_edge("ds~products", "c~product_id")
    g.add_edge("ds~products", "c~price")
    g.add_edge("ds~customer", "c~customer_id")
    g.add_edge("ds~customer", "c~customer_name")
    g.add_edge("ds~customer_address", "c~customer_id")
    g.add_edge("ds~customer_address", "c~address")
    g.add_edge("ds~customer_address", "c~city")

    partials = {
        "ds~orders": ["c~customer_id", "c~product_id"],
        "ds~customer_address": ["c~customer_id"],
    }

    output = resolve_join_order_v2(g, partials, {})

    assert output == [
        JoinOrderOutput(
            right="ds~orders",
            type=JoinType.LEFT_OUTER,
            keys={"ds~products": {"c~product_id"}},
        ),
        JoinOrderOutput(
            right="ds~customer",
            type=JoinType.FULL,
            keys={"ds~orders": {"c~customer_id"}},
        ),
        JoinOrderOutput(
            right="ds~customer_address",
            type=JoinType.LEFT_OUTER,
            keys={"ds~customer": {"c~customer_id"}},
        ),
    ]


def test_empty_joins_list():
    """Test that an empty list doesn't cause any errors."""
    joins = []
    ensure_content_preservation(joins)
    assert joins == []


def test_single_join_unchanged():
    """Test that a single join remains unchanged regardless of type."""
    join = JoinOrderOutput(
        right="table_b", type=JoinType.INNER, keys={"table_a": {"id"}}
    )
    joins = [join]
    ensure_content_preservation(joins)
    assert join.type == JoinType.INNER


def test_full_join_always_skipped():
    """Test that FULL joins are never modified."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.FULL, keys={"table_b": {"id"}}
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join2.type == JoinType.FULL


def test_no_prior_null_introducing_joins():
    """Test that join types remain unchanged when no prior joins introduce nulls."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.INNER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.INNER, keys={"table_b": {"id"}}
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.INNER
    assert join2.type == JoinType.INNER


def test_prior_left_outer_promotes_to_left_outer():
    """Test that a prior LEFT_OUTER join promotes current join to LEFT_OUTER."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.INNER, keys={"table_b": {"id"}}
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.LEFT_OUTER


def test_prior_right_outer_promotes_to_right_outer():
    """Test that a prior RIGHT_OUTER join promotes current join to RIGHT_OUTER."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.RIGHT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c",
        type=JoinType.INNER,
        keys={"table_a": {"id"}},  # table_a is in join1.lefts
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.RIGHT_OUTER
    assert join2.type == JoinType.RIGHT_OUTER


def test_prior_full_join_promotes_to_left_outer():
    """Test that a prior FULL join promotes current join to LEFT_OUTER (acts as LEFT_OUTER)."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.FULL, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.INNER, keys={"table_b": {"id"}}
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.FULL
    assert join2.type == JoinType.LEFT_OUTER


def test_both_prior_conditions_promote_to_full():
    """Test that both prior left and right conditions promote to FULL join."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.RIGHT_OUTER, keys={"table_d": {"id"}}
    )
    join3 = JoinOrderOutput(
        right="table_e",
        type=JoinType.INNER,
        keys={"table_b": {"id"}, "table_d": {"id"}},
    )
    joins = [join1, join2, join3]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.RIGHT_OUTER
    assert join3.type == JoinType.FULL


def test_complex_chain_with_multiple_promotions():
    """Test a complex chain of joins with multiple type promotions."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.INNER, keys={"table_b": {"id"}}
    )
    join3 = JoinOrderOutput(
        right="table_d", type=JoinType.INNER, keys={"table_c": {"id"}}
    )
    joins = [join1, join2, join3]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.LEFT_OUTER  # promoted due to join1
    assert join3.type == JoinType.LEFT_OUTER  # promoted due to join2


def test_no_overlap_in_keys_no_promotion():
    """Test that joins with no overlapping keys don't affect each other."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_d",
        type=JoinType.INNER,
        keys={"table_c": {"id"}},  # No overlap with join1
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.INNER  # Should remain unchanged


def test_multiple_keys_in_join():
    """Test joins with multiple keys in the keys dictionary."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id", "name"}}
    )
    join2 = JoinOrderOutput(
        right="table_c",
        type=JoinType.INNER,
        keys={"table_b": {"id"}, "table_d": {"other_id"}},
    )
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.LEFT_OUTER


def test_right_outer_with_multiple_predecessors():
    """Test RIGHT_OUTER logic with multiple predecessor joins."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.INNER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.RIGHT_OUTER, keys={"table_b": {"id"}}
    )
    join3 = JoinOrderOutput(
        right="table_d",
        type=JoinType.INNER,
        keys={"table_a": {"id"}},  # table_a is in join2.lefts (via join1)
    )
    joins = [join1, join2, join3]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.INNER
    assert join2.type == JoinType.RIGHT_OUTER
    assert join3.type == JoinType.INNER


def test_left_outer_already_correct_type():
    """Test that a join that already has the correct type is not modified."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c",
        type=JoinType.LEFT_OUTER,  # Already the correct type
        keys={"table_b": {"id"}},
    )
    joins = [join1, join2]

    # Store original references to ensure they're the same objects
    original_join2 = join2

    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.LEFT_OUTER
    assert join2 is original_join2  # Same object reference


def test_edge_case_empty_keys():
    """Test behavior with empty keys dictionary."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={}  # Empty keys
    )
    join2 = JoinOrderOutput(right="table_c", type=JoinType.INNER, keys={})  # Empty keys
    joins = [join1, join2]
    ensure_content_preservation(joins)
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.INNER  # No overlap, so no change


def test_modification_continues_processing():
    """Test that the continue statement works correctly after modification."""
    join1 = JoinOrderOutput(
        right="table_b", type=JoinType.LEFT_OUTER, keys={"table_a": {"id"}}
    )
    join2 = JoinOrderOutput(
        right="table_c", type=JoinType.INNER, keys={"table_b": {"id"}}
    )
    join3 = JoinOrderOutput(
        right="table_d", type=JoinType.INNER, keys={"table_c": {"id"}}
    )
    joins = [join1, join2, join3]
    ensure_content_preservation(joins)

    # All should be promoted to LEFT_OUTER due to cascade effect
    assert join1.type == JoinType.LEFT_OUTER
    assert join2.type == JoinType.LEFT_OUTER
    assert join3.type == JoinType.LEFT_OUTER


def test_resolve_join_order_v2_multi_partial():
    """When two datasources share a concept as partial,
    they should both be kept as left join sources for
    a dimension table (enabling COALESCE)."""
    g = Graph()
    g.add_edge("ds~fact1", "c~shared_id")
    g.add_edge("ds~fact1", "c~fact1_val")
    g.add_edge("ds~fact2", "c~shared_id")
    g.add_edge("ds~fact2", "c~fact2_val")
    g.add_edge("ds~dim", "c~shared_id")
    g.add_edge("ds~dim", "c~dim_name")

    partials = {
        "ds~fact1": ["c~shared_id"],
        "ds~fact2": ["c~shared_id"],
    }

    output = resolve_join_order_v2(g, partials, {})

    # Both fact tables should join to dim; fact tables join first
    # because multi_partial scoring boosts them.
    assert len(output) == 2
    # dim should be the last join (right side)
    assert output[-1].right == "ds~dim"
    # The dim join should have two left sources (both facts)
    dim_join = output[-1]
    assert len(dim_join.keys) == 2
