from pathlib import Path

from trilogy import Dialects, parse
from trilogy.core.models.environment import Environment
from trilogy.dialect.results import MockResult, MockResultRow


def test_file_parsing():
    directory = Path(__file__).parent
    target = directory / "test_env.preql"
    parsed = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=directory)
    ).parse_file(target)
    assert len(list(parsed)) == 1


def test_can_handle_everything():
    env = Environment(working_path=Path(__file__).parent)
    env, queries = parse(
        """
import test_env as test_env;

key x int;

merge test_env.id into x;
                         
RAW_SQL('''select 1 ''');
""",
        environment=env,
    )
    execs = Dialects.DUCK_DB.default_executor(environment=env)
    for q in queries:
        execs.execute_query(q)


def test_mock_result():
    result = MockResult(
        columns=["a", "b", "c"],
        values=[
            MockResultRow({"a": 1, "b": 2, "c": 3}),
            MockResultRow({"a": 4, "b": 5, "c": 6}),
        ],
    )
    assert result.columns == ["a", "b", "c"]

    for row in result:
        assert isinstance(row, MockResultRow)
        assert set(result.keys()) == {"a", "b", "c"}
        assert all(isinstance(v, int) for v in row.values())
        _ = [v for v in row]

    result = MockResult(
        columns=["a", "b", "c"],
        values=[
            MockResultRow({"a": 1, "b": 2, "c": 3}),
            MockResultRow({"a": 4, "b": 5, "c": 6}),
        ],
    )
    assert result.columns == ["a", "b", "c"]

    x = result.fetchone()
    assert isinstance(x, MockResultRow)
    assert x["a"] == 1
    r2 = result.fetchmany(10)
    assert len(r2) == 1


def test_escape_literal_colons():
    from trilogy.engine import (
        LITERAL_COLON_ESCAPE,
        escape_literal_colons,
        unescape_literal_colons,
    )

    assert (
        escape_literal_colons("select regexp_extract(url, 'http(?:s)?://')")
        == f"select regexp_extract(url, 'http(?{LITERAL_COLON_ESCAPE}s)?{LITERAL_COLON_ESCAPE}//')"
    )
    # colons outside literals remain bindable params
    assert escape_literal_colons("select :param from t") == "select :param from t"
    # doubled-quote escape stays inside the literal
    assert (
        escape_literal_colons("select 'it''s :x'")
        == f"select 'it''s {LITERAL_COLON_ESCAPE}x'"
    )
    # backslash-escaped quote (BigQuery-style literal) does not end the string
    assert (
        escape_literal_colons("select 'a\\':b'")
        == f"select 'a\\'{LITERAL_COLON_ESCAPE}b'"
    )
    # unescape is the exact inverse for escaped output
    for sql in (
        "select regexp_extract(url, 'http(?:s)?://')",
        "select 'it''s :x'",
        "select ':a :b', ':c'",
    ):
        assert unescape_literal_colons(escape_literal_colons(sql)) == sql


def test_raw_sql_colon_in_string_literal_not_a_param():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_raw_sql("CREATE TABLE urls (url VARCHAR)")
    exec.execute_raw_sql("INSERT INTO urls VALUES ('https://example.com/x')")
    exec.parse_text("""key url string;
datasource urls (url: url) grain (url) address urls;
auto domain <- regexp_extract(url, 'http(?:s)?://([^/]+)', 1);
""")
    rs = exec.execute_query("select domain;")
    assert rs.fetchall() == [("example.com",)]
