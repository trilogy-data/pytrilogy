from trilogy.core.env_processor import generate_graph
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators import gen_select_node
from trilogy.core.processing.nodes import ConstantNode, SelectNode
from trilogy.core.statements.author import PersistStatement
from trilogy.hooks.query_debugger import DebuggingHook


def test_gen_select_node_parents(test_environment: Environment):
    test_environment.concepts["category_top_50_revenue_products"]
    test_environment.concepts["category_id"]


def test_select_nodes():
    env = Environment()
    DebuggingHook()
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
    assert isinstance(gnode, ConstantNode), type(gnode)

    gnode = gen_select_node(
        concept=env.concepts["unnest"],
        local_optional=[],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    assert isinstance(gnode, SelectNode)


def test_materialized_select():
    env = Environment()
    DebuggingHook()
    env.parse(
        """
key order_id int;
key customer_id int;
property customer_id.customer_name string;
property order_id.revenue float;

datasource customer (
    customer_id:customer_id,
    customer_name:customer_name)
grain (customer_id)
address customer;

datasource order (
    order_id:order_id,
    customer_id:customer_id,
    revenue:revenue)
grain (order_id)
address order;

datasource blended (
    order_id:order_id,
    revenue:revenue,
    customer_id:customer_id,
    customer_name:customer_name)
grain (order_id,)
address blended;
          """,
        persist=True,
    )

    gnode = gen_select_node(
        concept=env.concepts["order_id"],
        local_optional=[env.concepts[x] for x in ["customer_id", "customer_name"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 1


def test_materialized_select_with_filter():
    env = Environment()
    DebuggingHook()
    _, statements = env.parse(
        """
key order_id int;
key customer_id int;
property customer_id.customer_name string;
property order_id.revenue float;

datasource customer (
    customer_id:customer_id,
    customer_name:customer_name)
grain (customer_id)
address customer;

datasource order (
    order_id:order_id,
    customer_id:customer_id,
    revenue:revenue)
grain (order_id)
address order;

persist blended into blended from 
select order_id, revenue, customer_id, customer_name
where order_id = 1;

          """,
        persist=True,
    )

    persist: PersistStatement = statements[-1]

    assert env.datasources["blended"].non_partial_for == persist.select.where_clause
    gnode = gen_select_node(
        concept=env.concepts["order_id"],
        local_optional=[env.concepts[x] for x in ["customer_id", "customer_name"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
        conditions=persist.select.where_clause,
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 1

    # if we search without conditions, we should need the join
    gnode = gen_select_node(
        concept=env.concepts["order_id"],
        local_optional=[env.concepts[x] for x in ["customer_id", "customer_name"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 2
