from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildWhereClause,
)
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    covered_conditions,
    datasource_conditions,
    preexisting_conditions,
)


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


def test_datasource_conditions_skip_covered_non_partial_atom():
    build_env = _build_sales_environment()
    ds = build_env.datasources["sales"]
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

    routed = datasource_conditions(ds, full_conditions, None, partial_is_full=True)

    assert routed == ds.where.conditional


def test_preexisting_conditions_restricted_to_non_partial_for():
    build_env = _build_sales_environment()
    ds = build_env.datasources["sales"]
    conditions = BuildWhereClause(
        conditional=BuildComparison(
            left=build_env.concepts["sale_year"],
            right=2021,
            operator=ComparisonOperator.EQ,
        )
    )

    existing = preexisting_conditions(
        ds, conditions, partial_is_full=True, satisfies_conditions=True
    )

    assert existing == ds.non_partial_for.conditional


def test_covered_conditions_returns_only_complete_where_atoms():
    build_env = _build_sales_environment()
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

    covered = covered_conditions(full_conditions, build_env)

    assert covered is not None
    assert covered.conditional == year_cond
