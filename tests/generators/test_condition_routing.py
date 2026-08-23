from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    JoinType,
    SourceType,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildGrain,
    BuildParenthetical,
    BuildSubselectComparison,
    BuildWhereClause,
)
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    covered_conditions,
    datasource_conditions,
    preexisting_conditions,
)
from trilogy.core.processing.node_generators.select_merge_node import (
    _condition_can_apply_after_node_merge,
    _condition_remaining_after_parents,
    _condition_source_concepts,
    _conditions_can_be_sourced_by_components,
    _merge_condition_routing,
    _parents_apply_condition_atoms,
)
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.processing.nodes.merge_node import deduplicate_nodes


def _build_sales_environment():
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
    return env.materialize_for_select()


def _condition(left, right, operator=ComparisonOperator.EQ):
    return BuildComparison(left=left, right=right, operator=operator)


def test_datasource_conditions_skip_covered_non_partial_atom():
    build_env = _build_sales_environment()
    ds = build_env.datasources["sales"]
    year_cond = _condition(build_env.concepts["sale_year"], 2021)
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    full_conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=year_cond, right=price_cond, operator=BooleanOperator.AND
        )
    )

    routed = datasource_conditions(ds, full_conditions, None, partial_is_full=True)

    assert routed == ds.where.conditional


def test_preexisting_conditions_restricted_to_non_partial_for():
    build_env = _build_sales_environment()
    ds = build_env.datasources["sales"]
    conditions = BuildWhereClause(
        conditional=_condition(build_env.concepts["sale_year"], 2021)
    )

    existing = preexisting_conditions(
        ds, conditions, partial_is_full=True, satisfies_conditions=True
    )

    assert existing == ds.non_partial_for.conditional


def test_covered_conditions_returns_only_complete_where_atoms():
    build_env = _build_sales_environment()
    year_cond = _condition(build_env.concepts["sale_year"], 2021)
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    full_conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=year_cond, right=price_cond, operator=BooleanOperator.AND
        )
    )

    covered = covered_conditions(full_conditions, build_env)

    assert covered is not None
    assert covered.conditional == year_cond


def test_datasource_conditions_merge_injected_condition_with_datasource_where():
    build_env = _build_sales_environment()
    ds = build_env.datasources["sales"]
    injected = _condition(build_env.concepts["sale_id"], 10, ComparisonOperator.GT)

    routed = datasource_conditions(ds, None, injected, partial_is_full=False)

    assert routed == BuildConditional(
        left=ds.where.conditional,
        right=injected,
        operator=BooleanOperator.AND,
    )


def test_datasource_conditions_pushes_scalar_condition_on_output():
    build_env = _build_sales_environment()
    ds = build_env.datasources["items"]
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )

    routed = datasource_conditions(
        ds, BuildWhereClause(conditional=price_cond), None, partial_is_full=False
    )

    assert routed == price_cond


def test_datasource_conditions_drops_non_nullable_is_not_null_atom():
    build_env = _build_sales_environment()
    ds = build_env.datasources["items"]
    non_null_cond = _condition(
        build_env.concepts["item_price"],
        MagicConstants.NULL,
        ComparisonOperator.IS_NOT,
    )
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=non_null_cond, right=price_cond, operator=BooleanOperator.AND
        )
    )

    routed = datasource_conditions(ds, conditions, None, partial_is_full=False)

    assert routed == price_cond


def test_datasource_conditions_keeps_non_nullable_is_null_atom():
    build_env = _build_sales_environment()
    ds = build_env.datasources["items"]
    is_null_cond = _condition(
        build_env.concepts["item_price"], MagicConstants.NULL, ComparisonOperator.IS
    )

    routed = datasource_conditions(
        ds, BuildWhereClause(conditional=is_null_cond), None, partial_is_full=False
    )

    assert routed == is_null_cond


def test_datasource_conditions_ignores_existence_condition():
    build_env = _build_sales_environment()
    ds = build_env.datasources["items"]
    subselect_cond = BuildSubselectComparison(
        left=build_env.concepts["item_id"],
        right=build_env.concepts["item_price"],
        operator=ComparisonOperator.IN,
    )

    routed = datasource_conditions(
        ds, BuildWhereClause(conditional=subselect_cond), None, partial_is_full=False
    )

    assert routed is None


def test_datasource_conditions_ignores_non_scalar_aggregate_condition_on_output():
    env = Environment()
    env.parse(
        """
key sale_id int;
auto sale_count <- count(sale_id);

datasource sales_summary (
    sale_count: sale_count,
)
query '''
select 3 as sale_count
''';
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    ds = build_env.datasources["sales_summary"]
    count_cond = _condition(build_env.concepts["sale_count"], 1, ComparisonOperator.GT)

    routed = datasource_conditions(
        ds, BuildWhereClause(conditional=count_cond), None, partial_is_full=False
    )

    assert routed is None


def test_covered_conditions_returns_none_when_not_implied():
    build_env = _build_sales_environment()
    ds = build_env.datasources["sales"]
    assert ds.non_partial_for is not None
    year_cond = _condition(build_env.concepts["sale_year"], 2022)

    covered = covered_conditions(BuildWhereClause(conditional=year_cond), build_env)

    assert covered is None


def test_covered_conditions_returns_multiple_complete_where_atoms():
    env = Environment()
    env.parse(
        """
key sale_id int;
property sale_id.sale_year int;
property sale_id.sale_channel string;

datasource online_2021_sales (
    sale_id: ~sale_id,
    sale_year: sale_year,
    sale_channel: sale_channel,
)
grain (sale_id)
complete where sale_year = 2021 and sale_channel = 'online'
address online_2021_sales;
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    year_cond = _condition(build_env.concepts["sale_year"], 2021)
    channel_cond = _condition(build_env.concepts["sale_channel"], "online")
    full_conditions = BuildWhereClause(
        conditional=BuildConditional(
            left=year_cond,
            right=channel_cond,
            operator=BooleanOperator.AND,
        )
    )

    covered = covered_conditions(full_conditions, build_env)

    assert covered is not None
    assert covered.conditional == full_conditions.conditional


def test_covered_conditions_handles_parenthetical_atoms():
    build_env = _build_sales_environment()
    year_cond = _condition(build_env.concepts["sale_year"], 2021)
    wrapped = BuildWhereClause(conditional=BuildParenthetical(content=year_cond))

    covered = covered_conditions(wrapped, build_env)

    assert covered is not None
    assert covered.conditional == year_cond


def test_flat_component_condition_can_span_datasources():
    env = Environment()
    env.parse(
        """
key customer_id int;
key product_id int;
property customer_id.region string;
property product_id.category string;

datasource customers (
    customer_id: customer_id,
    region: region,
)
grain (customer_id)
address customers;

datasource products (
    product_id: product_id,
    category: category,
)
grain (product_id)
address products;
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    condition = BuildWhereClause(
        conditional=_condition(
            build_env.concepts["region"], build_env.concepts["category"]
        )
    )

    assert _conditions_can_be_sourced_by_components(condition, build_env)


def test_progressive_remaining_condition_after_grouped_parent():
    env = Environment()
    env.parse(
        """
key customer_id int;
key product_id int;
key order_date date;
property customer_id.region string;
property product_id.category string;
auto order_count <- count(customer_id);
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    date_cond = _condition(
        build_env.concepts["order_date"], "2024-01-16", ComparisonOperator.GTE
    )
    flat_cond = _condition(build_env.concepts["region"], build_env.concepts["category"])
    full = BuildWhereClause(
        conditional=BuildConditional(
            left=date_cond,
            right=flat_cond,
            operator=BooleanOperator.AND,
        )
    )
    grouped = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["order_count"]],
        environment=build_env,
        preexisting_conditions=date_cond,
    )
    region = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["region"]],
        environment=build_env,
    )
    category = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["category"]],
        environment=build_env,
    )

    remaining = _condition_remaining_after_parents([grouped, region, category], full)

    assert remaining == flat_cond
    assert not _condition_can_apply_after_node_merge(
        [grouped, region, category], full.conditional
    )
    assert _condition_can_apply_after_node_merge([grouped, region, category], remaining)


def test_merge_condition_routing_does_not_force_inner_for_filtered_parents():
    build_env = _build_sales_environment()
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    where = BuildWhereClause(conditional=price_cond)
    left = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["item_id"]],
        environment=build_env,
        preexisting_conditions=price_cond,
    )
    right = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["item_price"]],
        environment=build_env,
        preexisting_conditions=price_cond,
    )

    preexisting, merge_condition, join_type = _merge_condition_routing(
        [left, right],
        [build_env.concepts["item_id"], build_env.concepts["item_price"]],
        where,
    )

    assert _parents_apply_condition_atoms([left, right], where)
    assert preexisting == price_cond
    assert merge_condition is None
    assert join_type is None


def test_merge_condition_routing_reapplies_parent_conditions_when_available():
    build_env = _build_sales_environment()
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    where = BuildWhereClause(conditional=price_cond)
    left = StrategyNode(
        input_concepts=[],
        output_concepts=[
            build_env.concepts["item_id"],
            build_env.concepts["item_price"],
        ],
        environment=build_env,
        conditions=price_cond,
    )
    right = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["item_price"]],
        environment=build_env,
        conditions=price_cond,
    )
    left.preexisting_conditions = None
    right.preexisting_conditions = None

    preexisting, merge_condition, join_type = _merge_condition_routing(
        [left, right],
        [build_env.concepts["item_id"], build_env.concepts["item_price"]],
        where,
    )

    assert preexisting == price_cond
    assert merge_condition == price_cond
    assert join_type is None


def test_merge_condition_routing_forces_inner_only_when_one_parent_is_complete():
    build_env = _build_sales_environment()
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    where = BuildWhereClause(conditional=price_cond)
    filtered_parent = StrategyNode(
        input_concepts=[],
        output_concepts=[
            build_env.concepts["item_id"],
            build_env.concepts["item_price"],
        ],
        environment=build_env,
        preexisting_conditions=price_cond,
    )
    enumerator_parent = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["item_id"]],
        environment=build_env,
    )

    preexisting, merge_condition, join_type = _merge_condition_routing(
        [filtered_parent, enumerator_parent],
        [build_env.concepts["item_id"], build_env.concepts["item_price"]],
        where,
    )

    assert preexisting == price_cond
    assert merge_condition is None
    assert join_type == JoinType.INNER


def test_merge_condition_routing_keeps_filtered_key_parent():
    build_env = _build_sales_environment()
    price_cond = _condition(
        build_env.concepts["item_price"], 100, ComparisonOperator.GT
    )
    where = BuildWhereClause(conditional=price_cond)
    filtered_key_parent = StrategyNode(
        input_concepts=[],
        output_concepts=[build_env.concepts["item_id"]],
        environment=build_env,
        preexisting_conditions=price_cond,
    )
    dimension_parent = StrategyNode(
        input_concepts=[],
        output_concepts=[
            build_env.concepts["item_id"],
            build_env.concepts["item_price"],
        ],
        environment=build_env,
    )

    preexisting, merge_condition, join_type = _merge_condition_routing(
        [filtered_key_parent, dimension_parent],
        [build_env.concepts["item_id"], build_env.concepts["item_price"]],
        where,
    )

    assert preexisting == price_cond
    assert merge_condition is None
    assert join_type == JoinType.INNER


def test_condition_source_concepts_skips_missing_key_references():
    build_env = _build_sales_environment()
    item_price = build_env.concepts["item_price"]
    item_price.keys = {"local.missing_key"}
    price_cond = _condition(item_price, 100, ComparisonOperator.GT)

    sourced = _condition_source_concepts([price_cond], build_env)

    assert sourced == [item_price]


def test_parents_apply_condition_atoms_rejects_empty_and_existence_conditions():
    build_env = _build_sales_environment()
    item_id = build_env.concepts["item_id"]
    item_price = build_env.concepts["item_price"]
    existence_cond = BuildSubselectComparison(
        left=item_id,
        right=item_price,
        operator=ComparisonOperator.IN,
    )
    where = BuildWhereClause(conditional=existence_cond)
    parent = StrategyNode(
        input_concepts=[],
        output_concepts=[item_id, item_price],
        environment=build_env,
        preexisting_conditions=existence_cond,
    )

    assert not _parents_apply_condition_atoms([], where)
    assert not _parents_apply_condition_atoms([parent], where)


def test_merge_dedup_keeps_parent_with_nested_condition():
    build_env = _build_sales_environment()
    ds = build_env.datasources["items"]
    item_id = build_env.concepts["item_id"]
    item_price = build_env.concepts["item_price"]
    price_cond = _condition(item_price, 100, ComparisonOperator.GT)
    filtered_leaf = QueryDatasource(
        input_concepts=[item_id, item_price],
        output_concepts=[item_id],
        source_map={item_id.address: {ds}, item_price.address: {ds}},
        datasources=[ds],
        grain=BuildGrain.from_concepts([item_id, item_price]),
        joins=[],
        source_type=SourceType.DIRECT_SELECT,
        condition=price_cond,
    )
    grouped_filtered = QueryDatasource(
        input_concepts=[item_id],
        output_concepts=[item_id],
        source_map={item_id.address: {filtered_leaf}},
        datasources=[filtered_leaf],
        grain=BuildGrain.from_concepts([item_id]),
        joins=[],
        source_type=SourceType.GROUP,
    )
    plain_parent = QueryDatasource(
        input_concepts=[item_id],
        output_concepts=[item_id],
        source_map={item_id.address: {ds}},
        datasources=[ds],
        grain=BuildGrain.from_concepts([item_id]),
        joins=[],
        source_type=SourceType.DIRECT_SELECT,
    )

    _, merged, removed = deduplicate_nodes(
        {
            grouped_filtered.identifier: grouped_filtered,
            plain_parent.identifier: plain_parent,
        },
        "",
        build_env,
    )

    assert grouped_filtered.identifier in merged
    assert plain_parent.identifier in merged
    assert not removed
