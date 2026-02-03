from logging import INFO

from trilogy import Dialects, parse


def test_inline_filter_basic():
    """A simple filtered aggregate should be inlined to a single SELECT."""

    from trilogy.hooks import DebuggingHook

    DebuggingHook(INFO)
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
    category || '-123' as category_label
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]

    # Should NOT have a WITH clause (no subquery)
    assert "WITH" not in sql, f"Expected no subquery, got: {sql}"
