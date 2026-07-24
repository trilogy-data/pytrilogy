from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildWhereClause,
)
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators.common import (
    gen_enrichment_node,
    gen_property_enrichment_node,
)
from trilogy.core.processing.node_generators.group_node import gen_group_node


def address_set(concepts):
    return {c.address for c in concepts}


def _transitive_property_environment() -> Environment:
    env = Environment()
    env.parse(
        """
key sale_id int;
key customer_id int;
key address_id int;
key return_address_id int;
key region_id int;
property sale_id.amount int;
property customer_id.customer_name string;
property address_id.state string;
property return_address_id.return_state string;
property region_id.region_name string;

datasource sales (
    sale_id: sale_id,
    customer_id: customer_id,
    return_address_id: return_address_id,
    amount: amount,
)
grain (sale_id)
query '''
select 1 as sale_id, 10 as customer_id, 200 as return_address_id, 5 as amount
''';

datasource customers (
    customer_id: customer_id,
    customer_name: customer_name,
    address_id: address_id,
)
grain (customer_id)
query '''
select 10 as customer_id, 'Alice' as customer_name, 100 as address_id
''';

datasource addresses (
    address_id: address_id,
    state: state,
)
grain (address_id)
query '''
select 100 as address_id, 'GA' as state
''';

datasource return_addresses (
    return_address_id: return_address_id,
    return_state: return_state,
)
grain (return_address_id)
query '''
select 200 as return_address_id, 'TN' as return_state
''';

datasource regions (
    region_id: region_id,
    region_name: region_name,
)
grain (region_id)
query '''
select 1 as region_id, 'South' as region_name
''';

auto total_amount <- sum(amount) by customer_id;
""",
        persist=True,
    )
    return env


def _customer_amount_node(env: Environment):
    build_env = env.materialize_for_select()
    graph = generate_graph(build_env)
    history = History(base_environment=env)
    gnode = gen_group_node(
        concept=build_env.concepts["local.total_amount"],
        local_optional=[build_env.concepts["local.customer_id"]],
        environment=build_env,
        g=graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )
    return build_env, graph, history, gnode


def test_gen_property_enrichment_node_direct_key(
    test_environment, test_environment_graph
):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    prod = test_environment.concepts["category_id"]
    prod_r = test_environment.concepts["total_revenue"]
    gnode = gen_group_node(
        concept=prod_r,
        local_optional=[prod],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )

    node = gen_property_enrichment_node(
        base_node=gnode,
        extra_properties=[test_environment.concepts["category_name"]],
        environment=test_environment,
        g=test_environment_graph,
        depth=1,
        source_concepts=search_concepts,
        log_lambda=lambda x: x,
        history=history,
    )

    assert address_set(node.output_concepts) == address_set(
        [
            test_environment.concepts["category_id"],
            test_environment.concepts["category_name"],
            test_environment.concepts["total_revenue"],
        ]
    )


def test_gen_enrichment_node_transitive_property_lookup():
    env = _transitive_property_environment()
    build_env, graph, history, gnode = _customer_amount_node(env)

    node = gen_enrichment_node(
        base_node=gnode,
        join_keys=[build_env.concepts["local.customer_id"]],
        local_optional=[
            build_env.concepts["local.customer_id"],
            build_env.concepts["local.customer_name"],
            build_env.concepts["local.state"],
        ],
        environment=build_env,
        g=graph,
        depth=1,
        source_concepts=search_concepts,
        log_lambda=lambda x: x,
        history=history,
    )

    assert address_set(node.output_concepts) == {
        "local.customer_id",
        "local.customer_name",
        "local.state",
        "local.total_amount",
    }
    lookup_source = node.parents[1].resolve()
    assert {c.address for c in lookup_source.output_concepts} >= {
        "local.customer_id",
        "local.address_id",
        "local.customer_name",
        "local.state",
    }


def test_property_enrichment_keeps_only_reachable_condition_atoms():
    env = _transitive_property_environment()
    build_env, graph, history, gnode = _customer_amount_node(env)
    state_cond = BuildComparison(
        left=build_env.concepts["local.state"],
        right="GA",
        operator=ComparisonOperator.EQ,
    )
    return_state_cond = BuildComparison(
        left=build_env.concepts["local.return_state"],
        right="TN",
        operator=ComparisonOperator.EQ,
    )
    conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=state_cond,
            right=return_state_cond,
            operator=BooleanOperator.AND,
        )
    )

    node = gen_enrichment_node(
        base_node=gnode,
        join_keys=[build_env.concepts["local.customer_id"]],
        local_optional=[
            build_env.concepts["local.customer_id"],
            build_env.concepts["local.customer_name"],
            build_env.concepts["local.state"],
        ],
        environment=build_env,
        g=graph,
        depth=1,
        source_concepts=search_concepts,
        log_lambda=lambda x: x,
        history=history,
        conditions=conditions,
    )

    lookup_source = node.parents[1].resolve()
    assert lookup_source.condition == state_cond


def test_property_enrichment_falls_back_when_key_is_not_reachable():
    env = _transitive_property_environment()
    build_env, graph, history, gnode = _customer_amount_node(env)

    node = gen_enrichment_node(
        base_node=gnode,
        join_keys=[build_env.concepts["local.customer_id"]],
        local_optional=[
            build_env.concepts["local.customer_id"],
            build_env.concepts["local.region_name"],
        ],
        environment=build_env,
        g=graph,
        depth=1,
        source_concepts=search_concepts,
        log_lambda=lambda x: x,
        history=history,
    )

    assert node is gnode
