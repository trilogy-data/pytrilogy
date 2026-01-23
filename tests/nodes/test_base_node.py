from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes.base_node import StrategyNode, get_all_parent_partial


def test_base_node_copy():
    env = Environment()
    x = StrategyNode(input_concepts=[], output_concepts=[], environment=env)

    y = x.copy()

    assert x.environment == y.environment


def test_hide():
    env = Environment()
    env.parse("""
key order_id int;
property order_id.profit float;""")
    env = env.materialize_for_select()
    x = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["order_id"]],
        environment=env,
    )

    x.hide_output_concepts([env.concepts["order_id"]])
    assert len(x.hidden_concepts) == 1
    x.unhide_output_concepts([env.concepts["order_id"]])
    assert len(x.hidden_concepts) == 0


def test_partial():
    env = Environment()
    env.parse("""
key order_id int;
property order_id.profit float;""")
    env = env.materialize_for_select()
    x = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["order_id"]],
        environment=env,
    )

    x.add_partial_concepts([env.concepts["order_id"]])
    assert len(x.partial_concepts) == 1


def test_get_parent_partial():
    env = Environment()
    env.parse("""
key order_id int;
property order_id.profit float;
              
key product_id int;
property product_id.price float;
              """)
    env = env.materialize_for_select()
    x = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["order_id"], env.concepts["product_id"]],
        environment=env,
        partial_concepts=[env.concepts["order_id"]],
    )

    y = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["product_id"], env.concepts["price"]],
        environment=env,
        partial_concepts=[],
    )

    partial = get_all_parent_partial([env.concepts["order_id"]], [x, y])

    assert len(partial) == 1

    assert partial[0].address == "local.order_id"

    z = StrategyNode(
        input_concepts=[],
        output_concepts=[env.concepts["order_id"]],
        environment=env,
        partial_concepts=[],
    )

    partial = get_all_parent_partial([env.concepts["order_id"]], [x, z])

    assert len(partial) == 0
