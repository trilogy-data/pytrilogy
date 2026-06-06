from trilogy.core.enums import Derivation, FunctionType, Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.author import AggregateWrapper, Function
from trilogy.core.models.build import BuildAggregateWrapper
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators import gen_group_node
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import FilterNode, GroupNode, MergeNode
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.common import agg_wrapper_to_concept, function_to_concept


def test_gen_group_node_parents(test_environment: Environment):
    test_environment = test_environment.materialize_for_select()
    comp = test_environment.concepts["category_top_50_revenue_products"]
    assert comp.derivation == Derivation.AGGREGATE
    assert comp.lineage
    assert test_environment.concepts["category_id"] in comp.lineage.concept_arguments
    assert comp.grain.components == {test_environment.concepts["category_id"].address}
    assert isinstance(comp.lineage, BuildAggregateWrapper)
    assert comp.lineage.by == [test_environment.concepts["category_id"]]
    parents = resolve_function_parent_concepts(comp, environment=test_environment)
    # parents should be both the value and the category
    assert len(parents) == 2
    assert test_environment.concepts["category_id"] in parents


def test_resolve_function_parents_sum_of_filter_on_keyed_property():
    # sum(filter(property ? condition)) by some grain: the SUM's parents
    # must include the property's key so downstream materialization fetches
    # rows at the property's row identity, not just the filtered value.
    env = Environment()
    env.parse("""
key order_id int;
property order_id.amount int;
property order_id.status string;

datasource orders (
    order_id,
    amount,
    status,
)
grain (order_id)
query '''select 1 as order_id, 10 as amount, 'open' as status''';

SELECT
    sum(amount ? status = 'open') as open_total,
;
""")
    build_env = env.materialize_for_select()
    sum_concept = build_env.concepts["local.open_total"]
    assert sum_concept.derivation == Derivation.AGGREGATE
    parents = resolve_function_parent_concepts(sum_concept, environment=build_env)
    parent_addresses = {p.address for p in parents}
    # The order_id key must be a parent — without it, the materialization
    # has no row identity for the filtered amount column.
    assert (
        "local.order_id" in parent_addresses
    ), f"missing order_id key; got {sorted(parent_addresses)}"


def test_resolve_function_parents_aggregate_of_aggregate_stops_at_boundary():
    # avg(sum(val1) by (group_key)): the outer avg's parents must NOT
    # surface the rowset grain or row-level keys that sit behind the inner
    # sum. The inner sum has already collapsed those rows to its own
    # ``by`` grain; they are only relevant when sourcing the inner sum
    # itself, not when sourcing the outer avg.
    env = Environment()
    env.parse("""
key row_id int;
property row_id.year int;
property row_id.group_key int;
property row_id.val1 int;

datasource src (
    row_id,
    year,
    group_key,
    val1,
)
grain (row_id)
query '''select 1 as row_id, 2001 as year, 100 as group_key, 10 as val1''';

with deduped as
SELECT year, group_key, val1,
;

auto inner_sum <- sum(deduped.val1) by deduped.group_key;

SELECT
    avg(inner_sum) as outer_avg,
;
""")
    build_env = env.materialize_for_select()
    outer = build_env.concepts["local.outer_avg"]
    assert outer.derivation == Derivation.AGGREGATE
    parents = resolve_function_parent_concepts(outer, environment=build_env)
    parent_addresses = {p.address for p in parents}
    # The inner sum's row-level upstreams (year, val1, the row_id key)
    # must NOT bleed up to the outer avg's parents.
    forbidden = {
        "deduped.year",
        "deduped.val1",
        "local.year",
        "local.val1",
        "local.row_id",
    }
    leaked = forbidden & parent_addresses
    assert not leaked, (
        f"row-level upstreams of inner aggregate leaked into outer avg parents: "
        f"{sorted(leaked)}; full parent set {sorted(parent_addresses)}"
    )


def test_resolve_function_parents_sum_of_filter_on_rowset():
    # SUM(rowset_col ? rowset_other_col = X) by rowset.group_key:
    # the SUM's parent concepts must include EVERY component of the
    # underlying rowset's declared grain, otherwise downstream
    # materialization will be pruned to just the filter's inputs and
    # dedup will collapse semantically-distinct rows.
    env = Environment()
    env.parse("""
key row_id int;
property row_id.year int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;

datasource src (
    row_id,
    year,
    group_key,
    val1,
    val2,
)
grain (row_id)
query '''select 1 as row_id, 2001 as year, 100 as group_key, 10 as val1, 100 as val2''';

with deduped as
SELECT year, group_key, val1, val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.val1 ? deduped.year = 2001) as v1_2001,
;
""")
    build_env = env.materialize_for_select()
    sum_concept = build_env.concepts["local.v1_2001"]
    assert sum_concept.derivation == Derivation.AGGREGATE
    parents = resolve_function_parent_concepts(sum_concept, environment=build_env)
    parent_addresses = {p.address for p in parents}
    # SUM must request every rowset grain component, not just the filter's
    # inputs (val1, year). Without group_key + val2, the downstream
    # materialization dedups at a strict subset of the rowset's grain.
    for required in (
        "deduped.year",
        "deduped.group_key",
        "deduped.val1",
        "deduped.val2",
    ):
        assert (
            required in parent_addresses
        ), f"missing {required} from SUM parents; got {sorted(parent_addresses)}"


def test_gen_group_node_basic(test_environment, test_environment_graph):
    history = History(base_environment=test_environment)
    test_environment = test_environment.materialize_for_select()
    prod = test_environment.concepts["product_id"]
    test_environment.concepts["revenue"]
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
    assert isinstance(gnode, (GroupNode, MergeNode))
    assert {x.address for x in gnode.output_concepts} == {prod_r.address, prod.address}


def test_gen_group_node_includes_compatible_optional_aggregate():
    env = Environment()
    env.parse("""
key row_id int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;

datasource src (
    row_id,
    group_key,
    val1,
    val2,
)
grain (row_id)
query '''select 1 as row_id, 1 as group_key, 10 as val1, 20 as val2''';

auto v1_total <- sum(val1) by group_key;
auto v2_total <- sum(val2) by group_key;

SELECT
    group_key,
    v1_total,
    v2_total,
;
""")
    build_env = env.materialize_for_select()
    gnode = gen_group_node(
        concept=build_env.concepts["local.v1_total"],
        local_optional=[
            build_env.concepts["local.group_key"],
            build_env.concepts["local.v2_total"],
        ],
        environment=build_env,
        g=generate_graph(build_env),
        depth=0,
        source_concepts=search_concepts,
        history=History(base_environment=env),
    )

    assert isinstance(gnode, GroupNode)
    assert [x.address for x in gnode.output_concepts] == [
        "local.v1_total",
        "local.group_key",
        "local.v2_total",
    ]
    assert len({x.address for x in gnode.input_concepts}) == len(gnode.input_concepts)


def test_gen_group_node(test_environment: Environment, test_environment_graph):
    history = History(base_environment=test_environment)
    DebuggingHook()
    test_environment = test_environment.materialize_for_select()
    cat = test_environment.concepts["category_id"]
    test_environment.concepts["category_top_50_revenue_products"]
    immediate_aggregate_input = test_environment.concepts[
        "products_with_revenue_over_50"
    ]
    gnode = gen_group_node(
        concept=test_environment.concepts["category_top_50_revenue_products"],
        local_optional=[],
        # local_optional=[cat],
        environment=test_environment,
        g=test_environment_graph,
        depth=0,
        source_concepts=search_concepts,
        history=history,
    )
    assert len(gnode.parents) == 1
    parent = gnode.parents[0]
    assert isinstance(parent, FilterNode)
    assert len(parent.all_concepts) == 2
    assert cat in parent.all_concepts
    assert immediate_aggregate_input in parent.all_concepts
    assert cat not in gnode.partial_concepts
    assert cat in gnode.all_concepts

    # check that the parent is a merge node
    parent.resolve()


def test_proper_parents(test_environment: Environment):
    base = Function(
        operator=FunctionType.COUNT,
        arguments=[test_environment.concepts["category_name"]],
        output_purpose=Purpose.PROPERTY,
        output_datatype=DataType.INTEGER,
    )

    new_agg = function_to_concept(
        base, name="base_agg", namespace="local", environment=test_environment
    )
    new_wrapper = agg_wrapper_to_concept(
        AggregateWrapper(
            function=base,
            by=[test_environment.concepts["category_name"]],
        ),
        name="agg_to_alt_grain",
        namespace="local",
        environment=test_environment,
    )
    test_environment.add_concept(new_agg)
    test_environment.add_concept(new_wrapper)
    test_environment = test_environment.materialize_for_select()

    resolved = resolve_function_parent_concepts(
        test_environment.concepts[new_agg.address],
        environment=test_environment,
    )
    assert len(resolved) == 2
    assert test_environment.concepts["category_id"] in resolved
    resolved = resolve_function_parent_concepts(
        test_environment.concepts[new_wrapper.address],
        environment=test_environment,
    )

    assert len(resolved) == 2
    assert test_environment.concepts["category_name"] in resolved
    assert test_environment.concepts["category_id"] in resolved
