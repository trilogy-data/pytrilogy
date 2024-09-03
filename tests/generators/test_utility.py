from trilogy.core.processing.node_generators.common import (
    resolve_join_order,
    NodeJoin,
    StrategyNode,
)
from trilogy import parse
from trilogy.core.env_processor import generate_graph
from trilogy.core.enums import JoinType


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
    price = env.concepts['price']
    product = env.concepts['product_id']
    left = StrategyNode(
        input_concepts=[orders], output_concepts=[orders], environment=env, g=g
    )
    right = StrategyNode(
        input_concepts=[orders, product], output_concepts=[orders, product], environment=env, g=g
    )

    right_two = StrategyNode(
        input_concepts=[product, price ], output_concepts=[product, price ], environment=env, g=g
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
    test_case = [
        second_join,
        first_join

    ]
    x = resolve_join_order(test_case)  
    assert x == [first_join, second_join]


    test_case = [
        second_join,
        second_join_inverse

    ]
    try:
        x = resolve_join_order(test_case)
        raise ValueError('test should not get here')  
    except Exception as e:
        assert isinstance(e, SyntaxError)
