from trilogy import Dialects
from trilogy.dialect.duckdb import _is_parenthesized

MODEL = """
key id int;
property id.code int?;
auto has_code <- code is not null;

datasource rows (id: id, code: code)
grain (id)
query '''select 1 as id, 10 as code union all select 2, null union all select 3, 30''';
"""


def test_bare_boolean_filter_renders_without_true_comparison():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    query = "where has_code select id order by id asc;"
    sql = executor.generate_sql(query)[-1]
    assert "= True" not in sql
    assert [r.id for r in executor.execute_text(query)[-1].fetchall()] == [1, 3]


def test_bare_boolean_in_case_keeps_values():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    query = (
        "select id, case when has_code then 'y' else 'n' end as flag order by id asc;"
    )
    assert "= True" not in executor.generate_sql(query)[-1]
    rows = executor.execute_text(query)[-1].fetchall()
    assert [r.flag for r in rows] == ["y", "n", "y"]


def test_is_parenthesized():
    assert _is_parenthesized("(a is not null)")
    assert _is_parenthesized("((a) or (b))")
    assert not _is_parenthesized("(a) or (b)")
    assert not _is_parenthesized("a is not null")
