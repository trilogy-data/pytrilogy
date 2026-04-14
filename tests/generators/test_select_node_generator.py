from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildWhereClause,
    Factory,
)
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators import gen_select_node
from trilogy.core.processing.node_generators.select_merge_node import (
    SearchCriteria,
    create_datasource_node,
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
    graph = generate_graph(env)
    gnode = gen_select_node(
        concepts=[
            env.concepts["array_one"],
        ],
        environment=env,
        g=graph,
        depth=0,
    )
    assert isinstance(gnode, ConstantNode), type(gnode)

    gnode = gen_select_node(
        concepts=[
            env.concepts["unnest"],
        ],
        environment=env,
        g=graph,
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
    build_env = env.materialize_for_select()
    gnode = resolve_subgraphs(
        g=generate_graph(build_env),
        conditions=None,
        relevant=[
            build_env.concepts[x]
            for x in ["order_id", "customer_id", "customer_name", "revenue"]
        ],
        criteria=SearchCriteria.FULL_ONLY,
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
        criteria=SearchCriteria.FULL_ONLY,
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


GEOGRAPHY_DIR = Path(__file__).resolve().parents[1] / "modeling" / "geography"


@pytest.mark.parametrize(
    ("concept_names", "condition_names"),
    [
        (
            ["city", "species", "common_names", "tree_category"],
            ["city", "species"],
        ),
        (
            ["tree_category", "common_names", "species", "city"],
            ["species", "city"],
        ),
    ],
)
def test_gen_select_node_preserves_exact_match_subgraph_filters(
    concept_names: list[str], condition_names: list[str]
):
    env = Environment.from_file(GEOGRAPHY_DIR / "tree_enrichment.preql")
    env = env.materialize_for_select()

    comparisons = {
        "city": BuildComparison(
            left=env.concepts["city"],
            right="USBOS",
            operator=ComparisonOperator.EQ,
        ),
        "species": BuildComparison(
            left=env.concepts["species"],
            right="Oak",
            operator=ComparisonOperator.EQ,
        ),
    }
    conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=comparisons[condition_names[0]],
            right=comparisons[condition_names[1]],
            operator=BooleanOperator.AND,
        )
    )

    gnode = gen_select_node(
        concepts=[env.concepts[name] for name in concept_names],
        environment=env,
        g=generate_graph(env),
        depth=0,
        conditions=conditions,
    )

    resolved = gnode.resolve()
    assert len(resolved.datasources) == 2

    enrichment = next(ds for ds in resolved.datasources if "tree_enrichment" in ds.name)
    assert enrichment.condition is not None
    assert str(enrichment.condition) == "local.species = Oak"


def test_filtered_aggregate_does_not_use_unfiltered_grainless_cache():
    env = Environment()
    env.parse(
        """
key order_id int;
property order_id.state string;
property order_id.amount float;

metric total_amount <- sum(amount);

datasource orders (
    order_id: order_id,
    state: state,
    amount: amount,
)
grain (order_id)
address orders;

datasource cached_total (
    total_amount: total_amount,
)
address cached_total;
""",
        persist=True,
    )

    executor = Dialects.DUCK_DB.default_executor(environment=env)
    generated = executor.generate_sql(
        """
WHERE state = 'GA'
SELECT total_amount;
"""
    )[-1]

    assert '"cached_total"' not in generated, generated
    assert '"orders"."state" = \'GA\'' in generated, generated


def test_partial_is_full_preexisting_conditions_restricted_to_non_partial_for():
    """partial_is_full must not mark extra conditions as preexisting.

    When query conditions are a strict superset of non_partial_for, preexisting_conditions
    should be set to non_partial_for only, so the outer strategy still applies the extras.
    """
    env = Environment()
    env.parse(
        """
key sale_id int;
key item_id int;
property sale_id.sale_year int;
property item_id.item_price float;

datasource sales (
    sale_id: ~sale_id,
    item_id: ~item_id,
    sale_year: sale_year,
)
grain (sale_id, item_id)
complete where sale_year = 2021
address sales_table
where sale_year = 2021;

datasource items (
    item_id: item_id,
    item_price: item_price,
)
grain (item_id)
address items_table;
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    ds = build_env.datasources["sales"]
    assert ds.non_partial_for is not None

    # full conditions: year = 2021 (covered by non_partial_for) AND item_price > 100
    # item_price is NOT in the sales datasource outputs
    year_cond = BuildComparison(
        left=build_env.concepts["sale_year"], right=2021, operator=ComparisonOperator.EQ
    )
    price_cond = BuildComparison(
        left=build_env.concepts["item_price"], right=100, operator=ComparisonOperator.GT
    )
    full_conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=year_cond, right=price_cond, operator=BooleanOperator.AND
        )
    )

    node, _ = create_datasource_node(
        datasource=ds,
        all_concepts=[build_env.concepts["sale_id"], build_env.concepts["item_id"]],
        accept_partial=True,
        environment=build_env,
        depth=0,
        conditions=full_conditions,
    )

    # The extra item_price condition was never applied to the datasource;
    # preexisting_conditions must reflect only what's actually baked in.
    assert node.preexisting_conditions == ds.non_partial_for.conditional
    assert node.preexisting_conditions != full_conditions.conditional


def test_partial_is_full_extra_condition_rejected_by_select_node_generation():
    env = Environment()
    env.parse(
        """
key sale_id int;
key item_id int;
property sale_id.sale_year int;
property item_id.item_price float;

datasource sales (
    sale_id: ~sale_id,
    item_id: ~item_id,
    sale_year: sale_year,
)
grain (sale_id, item_id)
complete where sale_year = 2021
address sales_table
where sale_year = 2021;

datasource items (
    item_id: item_id,
    item_price: item_price,
)
grain (item_id)
address items_table;
""",
        persist=True,
    )
    build_env = env.materialize_for_select()

    year_cond = BuildComparison(
        left=build_env.concepts["sale_year"], right=2021, operator=ComparisonOperator.EQ
    )
    price_cond = BuildComparison(
        left=build_env.concepts["item_price"], right=100, operator=ComparisonOperator.GT
    )
    full_conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=year_cond, right=price_cond, operator=BooleanOperator.AND
        )
    )

    node = gen_select_node(
        concepts=[build_env.concepts["sale_id"], build_env.concepts["item_id"]],
        environment=build_env,
        g=generate_graph(build_env),
        depth=0,
        conditions=full_conditions,
    )

    # Select-node discovery must reject candidates that cannot guarantee
    # full condition satisfaction; the higher-level strategy can then build
    # a safe multi-step plan.
    assert node is None
