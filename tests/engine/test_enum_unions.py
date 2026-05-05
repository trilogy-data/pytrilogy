import pytest

from trilogy import Dialects
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.core import EnumType
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.validation.environment import validate_environment

PREQL = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource ds_a (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 10 as sales
''';

datasource ds_b (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 20 as sales
''';
"""


def test_enum_type_parsed():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    concept = executor.environment.concepts["local.category"]
    assert isinstance(concept.datatype, EnumType)
    assert set(concept.datatype.values) == {"A", "B"}


def test_enum_comparison_invalid():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    with pytest.raises(Exception):
        executor.execute_text("select sales where category = 'C';")


def test_enum_union_injection():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    results = executor.execute_text("select sum(sales) as total_sales;")[-1].fetchall()
    assert len(results) == 1
    assert results[0].total_sales == 30


def test_enum_union_by_category():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL)
    results = executor.execute_text("select category, sales order by category asc;")[
        -1
    ].fetchall()
    assert len(results) == 2
    assert results[0].category == "A"
    assert results[0].sales == 10
    assert results[1].category == "B"
    assert results[1].sales == 20


# 4 sources: 2 script queries + 2 materialized tables, each pair covering both enum values.
# The materialized pair should be preferred; the union must be exactly 2 sources.
PREQL_OVERLAPPING = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource ds_a_query (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 10 as sales
''';

datasource ds_b_query (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 20 as sales
''';

datasource ds_a_table (
    ~category,
    sales
)
grain (category)
complete where category = 'A'
address category_a;

datasource ds_b_table (
    ~category,
    sales
)
grain (category)
complete where category = 'B'
address category_b;
"""


def test_overlapping_sources_picks_pair():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "CREATE TABLE category_a AS SELECT 'A' as category, 10 as sales"
    )
    executor.execute_raw_sql(
        "CREATE TABLE category_b AS SELECT 'B' as category, 20 as sales"
    )
    executor.execute_text(PREQL_OVERLAPPING)

    env = executor.environment.materialize_for_select()
    unions = get_union_sources(
        list(env.datasources.values()), [env.concepts["category"]]
    )

    assert len(unions) == 1, f"Expected 1 union group, got {len(unions)}"
    assert len(unions[0]) == 2, f"Expected pair of 2, got {len(unions[0])}"
    ds_names = {ds.name for ds in unions[0]}
    assert ds_names == {"ds_a_table", "ds_b_table"}


def test_overlapping_sources_no_duplicate_results():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "CREATE TABLE category_a AS SELECT 'A' as category, 10 as sales"
    )
    executor.execute_raw_sql(
        "CREATE TABLE category_b AS SELECT 'B' as category, 20 as sales"
    )
    executor.execute_text(PREQL_OVERLAPPING)

    results = executor.execute_text("select sum(sales) as total_sales;")[-1].fetchall()
    assert len(results) == 1
    # Would be 60 if all 4 sources were incorrectly unioned
    assert results[0].total_sales == 30


# Two sources covering both enum values but with no overlapping data fields.
# They share only the partition key, so the union should only expose that key.
# Sources with zero concept overlap should not be combinable at all.
PREQL_NO_OVERLAP = """
key category enum<string>['A', 'B'];
property <category>.field_a int;
property <category>.field_b int;

datasource ds_a_only (
    ~category,
    field_a
)
grain (category)
complete where category = 'A'
query '''
select 'A' as category, 1 as field_a
''';

datasource ds_b_only (
    ~category,
    field_b
)
grain (category)
complete where category = 'B'
query '''
select 'B' as category, 2 as field_b
''';
"""


PREQL_CAST = """
key sun_exposure string;
auto sun_exposure_label <- CASE sun_exposure
  when 'full_sun' then 'Full sun'
  when 'partial_shade' then 'Partial shade'
  when 'shade' then 'Shade'
  end::enum<string>['Full sun', 'Partial shade', 'Shade'];

datasource plants (
  sun_exposure: sun_exposure
)
grain (sun_exposure)
query '''
select 'full_sun' as sun_exposure
''';
"""


def test_enum_cast():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_CAST)
    concept = executor.environment.concepts["local.sun_exposure_label"]
    assert isinstance(concept.datatype, EnumType)
    assert set(concept.datatype.values) == {"Full sun", "Partial shade", "Shade"}
    results = executor.execute_text("select sun_exposure_label;")[-1].fetchall()
    assert len(results) == 1
    assert results[0].sun_exposure_label == "Full sun"


PREQL_ENUM_VALID = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource valid_ds (
    ~category,
    sales
)
grain (category)
query '''
select 'A' as category, 10 as sales
UNION ALL
select 'B' as category, 20 as sales
''';
"""

PREQL_ENUM_INVALID = """
key category enum<string>['A', 'B'];
property <category>.sales int;

datasource invalid_ds (
    ~category,
    sales
)
grain (category)
query '''
select 'C' as category, 99 as sales
''';
"""


def test_enum_validate_valid_values():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_ENUM_VALID)
    validate_environment(executor.environment, exec=executor)


def test_enum_validate_invalid_values():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_ENUM_INVALID)
    with pytest.raises(ModelValidationError):
        validate_environment(executor.environment, exec=executor)


def test_no_shared_data_fields_not_combined():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_NO_OVERLAP)

    env = executor.environment.materialize_for_select()
    unions = get_union_sources(
        list(env.datasources.values()), [env.concepts["category"]]
    )

    assert (
        len(unions) == 0
    ), f"Sources with no shared data fields should not be combined, got {unions}"


# Two derived aggregates pull different *subsets* of columns from the same enum
# partitioned arms (ds_a, ds_b, ds_c). Without union-CTE merging the planner
# emits two near-identical UNION CTEs; with it, both consumers share one.
PREQL_SHARED_UNION_ARMS = """
key category enum<string>['A', 'B', 'C'];
key item_id int;
key date_id int;
property <category, item_id>.x int;
property <category, item_id>.y int;
property <category, item_id>.flag int;
property date_id.year int;

datasource date_dim (
    date_id,
    year,
)
grain (date_id)
query '''
select 1 as date_id, 2024 as year
union all
select 2 as date_id, 2025 as year
''';

datasource ds_a (
    ~category,
    item_id,
    date_id,
    x,
    y,
    flag,
)
grain (category, item_id, date_id)
complete where category = 'A'
query '''
select 'A' as category, 1 as item_id, 1 as date_id, 1 as x, 10 as y, 1 as flag
''';

datasource ds_b (
    ~category,
    item_id,
    date_id,
    x,
    y,
    flag,
)
grain (category, item_id, date_id)
complete where category = 'B'
query '''
select 'B' as category, 1 as item_id, 1 as date_id, 2 as x, 20 as y, 1 as flag
''';

datasource ds_c (
    ~category,
    item_id,
    date_id,
    x,
    y,
    flag,
)
grain (category, item_id, date_id)
complete where category = 'C'
query '''
select 'C' as category, 1 as item_id, 2 as date_id, 3 as x, 30 as y, 0 as flag
''';
"""


SHARED_UNION_ARMS_QUERY = """
auto x_in_year <- sum(x ? year = 2024 and flag = 1) by item_id;
auto y_overall <- sum(y ? flag = 1) by item_id;

with x_summary as
SELECT
    max(x_in_year) as max_x,
;

with y_summary as
SELECT
    max(y_overall) as max_y,
;

SELECT
    x_summary.max_x,
    y_summary.max_y,
;
"""


def test_shared_union_arms_collapse_to_single_union():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(PREQL_SHARED_UNION_ARMS)
    sql = executor.generate_sql(SHARED_UNION_ARMS_QUERY)[-1]
    # 3 arms => 2 UNION ALL operators per union; one merged union => 2 total
    assert sql.count("UNION ALL") == 2, sql
    # each arm's underlying source select runs exactly once
    assert sql.count('as "ds_a"') == 1, sql
    assert sql.count('as "ds_b"') == 1, sql
    assert sql.count('as "ds_c"') == 1, sql
