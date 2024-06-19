from preql.core.models import Environment
from preql.core.processing.node_generators import gen_select_node
from preql.core.env_processor import generate_graph
from preql.core.processing.nodes import ConstantNode, SelectNode


def test_gen_select_node_parents(test_environment: Environment):
    test_environment.concepts["category_top_50_revenue_products"]
    test_environment.concepts["category_id"]


def test_select_nodes():
    env = Environment()
    env.parse(
        """
const array_one <- [1,2,3];

const unnest <- unnest(array_one);

persist arrays into arrays from
select unnest;
          """,
        persist=True,
    )

    gnode = gen_select_node(
        concept=env.concepts["array_one"],
        local_optional=[],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )
    assert isinstance(gnode, ConstantNode)

    gnode = gen_select_node(
        concept=env.concepts["unnest"],
        local_optional=[],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    assert isinstance(gnode, SelectNode)
