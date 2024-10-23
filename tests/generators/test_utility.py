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


def resolve_join_order(sources):
    pass


from networkx import Graph, common_neighbors
from trilogy.hooks.graph_hook import GraphHook

def test_resolve_join_order_v2():

    test = Graph()

    test.add_edge("ds~orders", "c~order_id")
    test.add_edge("ds~orders", "c~product_id")
    test.add_edge("ds~orders", "c~customer_id")
    test.add_edge("ds~products", "c~product_id")
    test.add_edge("ds~products", "c~price")
    test.add_edge("ds~customer", "c~customer_id")
    test.add_edge("ds~customer", "c~customer_name")
    test.add_edge("ds~customer_address", "c~customer_id")
    test.add_edge("ds~customer_address", "c~address")
    test.add_edge("ds~customer_address", "c~city")

    partials = {
        "ds~orders": ["c~customer_id", "c~product_id"],
        "ds~customer_address": ["c~customer_id"],
    }

    datasources = [x for x in test.nodes if x.startswith("ds~")]

    # GraphHook().query_graph_built(test)
    concepts = [x for x in test.nodes if x.startswith("c~")]
    roots = {}
    for concept in concepts:
        eligible = [
            x
            for x in test.neighbors(concept)
            if x in datasources and test not in partials.get(x, [])
        ]
        if eligible:
            roots[concept] = eligible

    first = list(roots.values())[0][0]
    output = []
    finished = False
    seen = set(
        [
            first,
        ]
    )
    attempted = set(
        [
            first,
        ]
    )
    all = set(datasources)
    pivot_map = {concept:[x for x in test.neighbors(concept) if x in datasources] for concept in concepts}
    pivots = sorted([x for x in pivot_map if len(pivot_map[x]) > 1], key=lambda x: len(pivot_map[x]))
    
    
    while pivots:
        root = pivots.pop()
        # technically we should check all possible join keys for partials?
        partial_weight = {x: len(partials.get(x, [])) for x in pivot_map[root]}
        print(partial_weight)
        # sort so less partials is lest
        to_join = sorted([x for x in pivot_map[root]], key = lambda x: 0 if root in partials.get(x, []) else 1 )
        left = to_join.pop()
        while to_join:
            right = to_join.pop()
            left_is_partial = left in partials.get(root, [])
            right_is_partial = right in partials.get(root, [])
            left_is_nullable = left in partials.get(root, [])
            right_is_nullable = right in partials.get(root, [])
            if left_is_partial:
                join_type = JoinType.FULL
            elif right_is_partial or right_is_nullable:
                join_type = JoinType.LEFT_OUTER
            else:
                join_type = JoinType.INNER
            output.append({'left':left, 'right':right, 'type':join_type, 'keys': common_neighbors(test, left, right)})
        

    print(output)
    assert 1 == 0
