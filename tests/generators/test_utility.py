from networkx import Graph

from trilogy import parse
from trilogy.core.enums import JoinType
from trilogy.core.env_processor import generate_graph
from trilogy.core.processing.node_generators.common import (
    # resolve_join_order_v2,
    NodeJoin,
    StrategyNode,
    resolve_join_order,
)
from trilogy.core.processing.utility import JoinOrderOutput, resolve_join_order_v2


def test_resolve_join_order():
    # empty

    env, _ = parse(
        """
key order_id int;
key product_id int;
property product_id.price float;       
                """
    )
    g = generate_graph(env)
    test_case = []
    x = resolve_join_order(test_case)
    assert x == []
    orders = env.concepts["order_id"]
    price = env.concepts["price"]
    product = env.concepts["product_id"]
    left = StrategyNode(
        input_concepts=[orders], output_concepts=[orders], environment=env, g=g
    )
    right = StrategyNode(
        input_concepts=[orders, product],
        output_concepts=[orders, product],
        environment=env,
        g=g,
    )

    right_two = StrategyNode(
        input_concepts=[product, price],
        output_concepts=[product, price],
        environment=env,
        g=g,
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

    output = resolve_join_order_v2(g, partials)

    assert output == [
        JoinOrderOutput(
            right="ds~customer_address",
            type=JoinType.LEFT_OUTER,
            keys={"ds~customer": {"c~customer_id"}},
        ),
        JoinOrderOutput(
            right="ds~orders",
            type=JoinType.LEFT_OUTER,
            keys={"ds~customer": {"c~customer_id"}},
        ),
        JoinOrderOutput(
            right="ds~products",
            type=JoinType.FULL,
            keys={"ds~orders": {"c~product_id"}},
        ),
    ]
