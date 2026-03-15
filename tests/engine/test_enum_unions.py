import pytest

from trilogy import Dialects
from trilogy.core.models.core import EnumType
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)

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
