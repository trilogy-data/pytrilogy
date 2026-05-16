from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
    BuildSubselectComparison,
    BuildWhereClause,
)
from trilogy.core.models.environment import Environment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.node_generators.common import _local_property_conditions
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    covered_conditions,
    datasource_conditions,
    preexisting_conditions,
)
from trilogy.core.processing.node_generators.select_merge_node import (
    _condition_can_apply_after_parent_merge,
    _condition_remaining_after_parents,
    _conditions_can_be_sourced_by_components,
)
from trilogy.core.processing.nodes import StrategyNode


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

    assert _conditions_can_be_sourced_by_components([], condition, build_env)


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
    assert not _condition_can_apply_after_parent_merge(
        [grouped, region, category], full.conditional
    )
    assert _condition_can_apply_after_parent_merge(
        [grouped, region, category], remaining
    )


def test_property_enrichment_keeps_only_local_condition_atoms():
    env = Environment()
    env.parse(
        """
key item_id int;
key store_id int;
key date_id int;
property item_id.item_desc string;
property store_id.store_name string;
property date_id.month_seq int;
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    date_cond = _condition(
        build_env.concepts["month_seq"], 1176, ComparisonOperator.GTE
    )
    store_cond = _condition(build_env.concepts["store_id"], 0, ComparisonOperator.GT)
    full = BuildWhereClause(
        conditional=BuildConditional(
            left=date_cond,
            right=store_cond,
            operator=BooleanOperator.AND,
        )
    )

    item_conditions, item_condition_concepts = _local_property_conditions(
        full,
        [build_env.concepts["item_id"], build_env.concepts["item_desc"]],
        {"local.item_id"},
    )
    store_conditions, store_condition_concepts = _local_property_conditions(
        full,
        [build_env.concepts["store_id"], build_env.concepts["store_name"]],
        {"local.store_id"},
    )

    assert item_conditions is None
    assert item_condition_concepts == []
    assert store_conditions is not None
    assert store_conditions.conditional == store_cond
    assert store_condition_concepts == []


def test_property_enrichment_keeps_lineage_relevant_condition_atoms():
    env = Environment()
    env.parse(
        """
key item_id int;
key date_id int;
key sales_channel string;
key order_id int;
property date_id.date date;
property date_id.month_seq int;
property <item_id, order_id, sales_channel>.sales_price float;
auto store_daily <- sum(sales_price ? sales_channel = 'STORE') by item_id, date;
auto store_cume <- sum store_daily over item_id order by date asc;
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    month_cond = _condition(
        build_env.concepts["month_seq"], 1200, ComparisonOperator.GTE
    )
    item_cond = _condition(build_env.concepts["item_id"], 0, ComparisonOperator.GT)
    channel_cond = BuildSubselectComparison(
        left=build_env.concepts["sales_channel"],
        right=("WEB", "STORE"),
        operator=ComparisonOperator.IN,
    )
    full = BuildWhereClause(
        conditional=BuildConditional(
            left=BuildConditional(
                left=month_cond,
                right=item_cond,
                operator=BooleanOperator.AND,
            ),
            right=channel_cond,
            operator=BooleanOperator.AND,
        )
    )

    local_conditions, condition_concepts = _local_property_conditions(
        full,
        [
            build_env.concepts["item_id"],
            build_env.concepts["date"],
            build_env.concepts["store_cume"],
        ],
        {"item_id", "date", "date_id"},
    )

    assert local_conditions is not None
    assert decompose_condition(local_conditions.conditional) == [
        month_cond,
        item_cond,
        channel_cond,
    ]
    assert {c.address for c in condition_concepts} == {
        "local.month_seq",
        "local.sales_channel",
    }
