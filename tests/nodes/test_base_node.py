from trilogy.core.processing.nodes.base_node import StrategyNode, get_all_parent_partial
from trilogy.core.models import Environment
from trilogy.core.env_processor import generate_graph


def test_base_node_copy():
    env = Environment()
    x = StrategyNode(
        input_concepts=[], output_concepts=[], environment=env, g=generate_graph(env)
    )

    y = x.copy()

    assert x.environment == y.environment


def test_get_parent_partial():
    env = Environment()
    env.parse(
        """
key order_id int;
property order_id.profit float;
              
key product_id int;
property product_id.price float;
              """
    )
    g = g = generate_graph(env)
    x = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["order_id"], env.concepts["product_id"]],
        environment=env,
        g=g,
        partial_concepts=[env.concepts["order_id"]],
    )

    y = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["product_id"], env.concepts["price"]],
        environment=env,
        g=g,
        partial_concepts=[],
    )

    partial = get_all_parent_partial([env.concepts["order_id"]], [x, y])

    assert len(partial) == 1

    assert partial[0].address == "local.order_id"

    z = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["order_id"]],
        environment=env,
        g=g,
        partial_concepts=[],
    )

    partial = get_all_parent_partial([env.concepts["order_id"]], [x, z])

    assert len(partial) == 0
