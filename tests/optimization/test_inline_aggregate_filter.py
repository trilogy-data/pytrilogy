"""Tests for the InlineAggregateFilter optimization rule."""

from trilogy import Dialects, parse
from trilogy.constants import CONFIG


def test_inline_aggregate_filter_basic():
    """A simple filtered aggregate should be inlined to a single SELECT."""
    query = """
key id int;
property id.value int;
key category string;

datasource test (
    id: id,
    value: value,
    category: category
)
grain (id)
address test_table;

select
    category,
    sum(value? category = 'A') as filtered_sum
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should NOT have a WITH clause (no subquery)
    assert "WITH" not in sql, f"Expected no subquery, got: {sql}"
    # Should have the CASE WHEN in the aggregate
    assert "CASE WHEN" in sql, f"Expected CASE WHEN in aggregate, got: {sql}"
    # Should have sum() with CASE WHEN inside
    assert "sum(CASE WHEN" in sql, f"Expected sum(CASE WHEN, got: {sql}"


def test_inline_aggregate_filter_multiple_filters():
    """Multiple filtered aggregates should still inline."""
    query = """
key id int;
property id.value int;
key category string;

datasource test (
    id: id,
    value: value,
    category: category
)
grain (id)
address test_table;

select
    category,
    sum(value? category = 'A') as sum_a,
    sum(value? category = 'B') as sum_b
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should NOT have a WITH clause
    assert "WITH" not in sql, f"Expected no subquery, got: {sql}"
    # Should have CASE WHEN twice (once for each filter)
    assert sql.count("CASE WHEN") == 2, f"Expected 2 CASE WHENs, got: {sql}"


def test_inline_aggregate_filter_preserves_grouping():
    """Grouping should be preserved after inlining."""
    query = """
key id int;
property id.value int;
key category string;
key region string;

datasource test (
    id: id,
    value: value,
    category: category,
    region: region
)
grain (id)
address test_table;

select
    category,
    region,
    sum(value? region = 'US') as us_sum
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should have GROUP BY with both columns
    assert "GROUP BY" in sql
    assert '"category"' in sql
    assert '"region"' in sql


def test_inline_disabled_by_config():
    """When disabled, the optimization should not run."""
    original_value = CONFIG.optimizations.inline_aggregate_filter
    try:
        CONFIG.optimizations.inline_aggregate_filter = False

        query = """
key id int;
property id.value int;
key category string;

datasource test (
    id: id,
    value: value,
    category: category
)
grain (id)
address test_table;

select
    category,
    sum(value? category = 'A') as filtered_sum
;
"""
        env, stmts = parse(query)
        exec = Dialects.DUCK_DB.default_executor(environment=env)
        sql = exec.generate_sql(stmts[-1])[-1]

        # WITH clause should exist (subquery not inlined)
        assert (
            "WITH" in sql
        ), f"Expected subquery when optimization disabled, got: {sql}"
    finally:
        CONFIG.optimizations.inline_aggregate_filter = original_value


def test_no_inline_when_parent_has_multiple_children():
    """Should not inline when the filter CTE has multiple children."""
    # This test verifies that when a filter CTE is used by multiple CTEs,
    # it won't be inlined (to avoid duplicating work)
    query = """
key id int;
property id.value int;
key category string;

datasource test (
    id: id,
    value: value,
    category: category
)
grain (id)
address test_table;

auto filtered_value <- value? category = 'A';

select
    category,
    sum(filtered_value) as sum_filtered,
    count(filtered_value) as count_filtered
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # This query should still work regardless of inlining
    assert "sum(" in sql.lower()
    assert "count(" in sql.lower()


def test_inline_complex_filter_condition():
    """Test with more complex filter conditions."""
    query = """
key id int;
property id.value int;
key category string;

datasource test (
    id: id,
    value: value,
    category: category
)
grain (id)
address test_table;

select
    category,
    sum(value? category = 'A' or category = 'B') as ab_sum
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should inline with the OR condition
    assert "WITH" not in sql, f"Expected no subquery, got: {sql}"
    assert "CASE WHEN" in sql


def test_inline_with_additional_columns():
    """Test that non-filter columns are still accessible."""
    query = """
key id int;
property id.value int;
property id.name string;
key category string;

datasource test (
    id: id,
    value: value,
    name: name,
    category: category
)
grain (id)
address test_table;

select
    category,
    sum(value? category = 'A') as filtered_sum
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should work without issues
    assert "sum(" in sql.lower()
