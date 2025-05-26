from trilogy.core.enums import ComparisonOperator
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import BuildComparison, BuildWhereClause, Factory
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators import gen_select_node
from trilogy.core.processing.node_generators.select_merge_node import (
    get_graph_partial_nodes,
    resolve_subgraphs,
)
from trilogy.core.processing.nodes import ConstantNode, SelectNode
from trilogy.core.statements.author import PersistStatement
from trilogy.hooks.query_debugger import DebuggingHook


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
    env = env.materialize_for_select()
    gnode = gen_select_node(
        concepts=[
            env.concepts["array_one"],
        ],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )
    assert isinstance(gnode, ConstantNode), type(gnode)

    gnode = gen_select_node(
        concepts=[
            env.concepts["unnest"],
        ],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    assert isinstance(gnode, SelectNode)


def test_materialized_select():
    env = Environment()
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
    env = env.materialize_for_select()
    gnode = gen_select_node(
        concepts=[env.concepts["order_id"]]
        + [env.concepts[x] for x in ["customer_id", "customer_name"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 1


def test_resolve_subgraphs():
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

    gnode = resolve_subgraphs(
        g=generate_graph(env.materialize_for_select()),
        conditions=None,
        relevant=[
            env.concepts[x]
            for x in ["order_id", "customer_id", "customer_name", "revenue"]
        ],
    )
    # we shoud resolve only the highest level source
    assert len(gnode) == 1
    assert "ds~blended" in gnode


def test_resolve_subgraphs_conditions():
    env = Environment()
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
    order_id:~order_id,
    customer_id:customer_id,
    revenue:revenue)
grain (order_id)
complete where order_id = 1
address order
where order_id = 1;

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
    env = env.materialize_for_select()
    graph = generate_graph(env)
    to_remove = []
    for n in graph.nodes:
        if n.startswith("c~local.customer_name"):
            to_remove.append(n)
    for n in to_remove:
        graph.remove_node(n)
    conditions = BuildWhereClause(
        conditional=BuildComparison(
            left=env.concepts["order_id"], right=1, operator=ComparisonOperator.EQ
        )
    )
    assert get_graph_partial_nodes(graph, conditions)["ds~order"] == []
    gnode = resolve_subgraphs(
        g=graph,
        conditions=conditions,
        relevant=[
            env.concepts[x]
            for x in ["order_id", "customer_id", "customer_name", "revenue"]
        ],
    )
    # we shoud resolve only the highest level source
    assert len(gnode) == 1
    assert "ds~order" in gnode


def test_materialized_select_with_filter():
    env = Environment()
    factory = Factory(environment=env)

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
    env = env.materialize_for_select()
    # assert env.datasources["blended"].non_partial_for == persist.select.where_clause
    gnode = gen_select_node(
        concepts=[env.concepts["order_id"]]
        + [env.concepts[x] for x in ["customer_id", "customer_name"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
        conditions=factory.build(persist.select.where_clause),
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 1

    # if we search without conditions, we should need the join
    gnode = gen_select_node(
        concepts=[env.concepts["order_id"]]
        + [env.concepts[x] for x in ["customer_id", "customer_name"]],
        environment=env,
        g=generate_graph(env),
        depth=0,
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 2
