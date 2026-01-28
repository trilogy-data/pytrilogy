"""Tests for the InlineAggregateFilter optimization rule."""

from trilogy import Dialects, parse


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


where category = 'A'
select
    category,
    sum(value) by category -> total_value
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should NOT have a WITH clause (no subquery)
    assert "WITH" not in sql, f"Expected no subquery, got: {sql}"
