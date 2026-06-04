"""Tests for the reduced no-Trilogy SQL toolset (``trilogy agent --toolset sql``)."""

import pytest

from trilogy.scripts import agent_sql_tools as sql_mod
from trilogy.scripts.agent_sql_tools import (
    SQL_TOOL_HANDLERS,
    SQL_TOOLS,
    _envelope,
    _execute_sql,
    _format_result,
    _last_statement,
    _readonly_violation,
    _strip_leading_comments,
    handle_read_file,
    handle_return_control,
    handle_run_file,
    handle_run_query,
    handle_write_file,
    sql_system_prompt,
)
from trilogy.scripts.agent_tools import AgentState


@pytest.fixture
def sql_engine(tmp_path, monkeypatch):
    """Real in-memory DuckDB executor, built lazily through ``_get_engine`` so
    the build path is exercised. Reset per test to avoid cross-test leakage."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duck_db"\n')
    monkeypatch.setattr(sql_mod, "_ENGINE", None)
    eng = sql_mod._get_engine()
    eng.execute_raw_sql("CREATE TABLE t (id INTEGER, name VARCHAR)")
    eng.execute_raw_sql("INSERT INTO t VALUES (1, 'a'), (2, NULL)")
    return eng


# --- comment / statement parsing ---


def test_strip_leading_comments_line_and_block():
    assert _strip_leading_comments("-- note\nselect 1") == "select 1"
    assert _strip_leading_comments("/* x */ select 1") == "select 1"
    assert _strip_leading_comments("  select 1") == "select 1"
    assert _strip_leading_comments("-- a\n/* b */\nselect 1") == "select 1"


def test_last_statement_skips_trailing_comment_and_empty():
    assert _last_statement("select 1; select 2;") == "select 2"
    assert _last_statement("select 1;\n-- trailing note") == "select 1"
    assert _last_statement("   ;  ;  ") == ""


# --- read-only enforcement ---


def test_readonly_violation_allows_select_family():
    for kw in ("select 1", "WITH x AS (select 1) select * from x", "DESCRIBE t"):
        assert _readonly_violation(kw) is None


def test_readonly_violation_rejects_writes_and_empty():
    assert "empty SQL" in _readonly_violation("")
    msg = _readonly_violation("INSERT INTO t VALUES (1)")
    assert msg is not None and "read-only" in msg


def test_readonly_violation_ignores_leading_comment():
    assert _readonly_violation("-- pick rows\nselect * from t") is None


# --- result formatting ---


def test_format_result_renders_header_rows_and_nulls():
    out = _format_result(["id", "name"], [(1, "a"), (2, None)])
    assert "id | name" in out
    assert "1 | a" in out
    assert "2 | NULL" in out
    assert "[2 row(s)]" in out


def test_format_result_truncates_and_reports_total(monkeypatch):
    monkeypatch.setattr(sql_mod, "_MAX_RESULT_ROWS", 2)
    out = _format_result(["id"], [(i,) for i in range(5)])
    assert "[5 row(s); first 2 shown]" in out


def test_format_result_handles_no_columns():
    assert "(no columns)" in _format_result([], [])


def test_envelope_shape():
    env = _envelope(1, "out", "err")
    assert env.startswith("exit_code: 1")
    assert "--- stdout ---\nout" in env
    assert "--- stderr ---\nerr" in env


# --- _execute_sql against a real DuckDB engine ---


def test_execute_sql_returns_rows(sql_engine):
    out = _execute_sql(AgentState(), "select id, name from t order by id")
    assert "exit_code: 0" in out
    assert "id | name" in out
    assert "2 | NULL" in out


def test_execute_sql_rejects_non_readonly(sql_engine):
    out = _execute_sql(AgentState(), "DELETE FROM t")
    assert "exit_code: 1" in out
    assert "read-only" in out


def test_execute_sql_reports_engine_error(sql_engine):
    out = _execute_sql(AgentState(), "select * from does_not_exist")
    assert "exit_code: 1" in out
    assert "--- stderr ---" in out


def test_execute_sql_runs_only_last_statement(sql_engine):
    out = _execute_sql(AgentState(), "select 1 as a; select 99 as b")
    assert "exit_code: 0" in out
    assert "99" in out


def test_execute_sql_truncates_output(sql_engine):
    out = _execute_sql(AgentState(tool_output_limit=15), "select * from t")
    assert "exit_code: 0" in out
    assert "truncated" in out


def test_execute_sql_handles_resultless_keys(monkeypatch):
    class _Result:
        def fetchall(self):
            return [(1,)]

        def keys(self):
            raise RuntimeError("no keys here")

    class _Engine:
        def execute_raw_sql(self, sql):
            return _Result()

    monkeypatch.setattr(sql_mod, "_get_engine", lambda: _Engine())
    out = _execute_sql(AgentState(), "select 1")
    assert "exit_code: 0" in out
    assert "1" in out


# --- file handlers ---


def test_handle_write_file_creates_nested_dirs(tmp_path):
    target = tmp_path / "sub" / "query01.sql"
    out = handle_write_file(AgentState(), {"path": str(target), "content": "select 1"})
    assert "wrote" in out
    assert target.read_text(encoding="utf-8") == "select 1"


def test_handle_write_file_validates_args():
    assert "non-empty string" in handle_write_file(AgentState(), {"content": "x"})
    assert "must be a string" in handle_write_file(
        AgentState(), {"path": "q.sql", "content": 5}
    )


def test_handle_read_file_roundtrip_and_errors(tmp_path):
    f = tmp_path / "schema.md"
    f.write_text("# tables", encoding="utf-8")
    assert "# tables" in handle_read_file(AgentState(), {"path": str(f)})
    assert "non-empty string" in handle_read_file(AgentState(), {"path": ""})
    assert "no such file" in handle_read_file(
        AgentState(), {"path": str(tmp_path / "missing")}
    )
    assert "not a file" in handle_read_file(AgentState(), {"path": str(tmp_path)})


def test_handle_read_file_truncates(tmp_path):
    f = tmp_path / "big.sql"
    f.write_text("x" * 1000, encoding="utf-8")
    out = handle_read_file(AgentState(tool_output_limit=80), {"path": str(f)})
    assert "truncated" in out


def test_handle_run_file_executes(sql_engine, tmp_path):
    q = tmp_path / "q.sql"
    q.write_text("select id from t order by id", encoding="utf-8")
    out = handle_run_file(AgentState(), {"path": str(q)})
    assert "exit_code: 0" in out


def test_handle_run_file_validates(tmp_path):
    assert "non-empty string" in handle_run_file(AgentState(), {"path": ""})
    assert "no such file" in handle_run_file(
        AgentState(), {"path": str(tmp_path / "nope.sql")}
    )


def test_handle_run_query_executes_and_validates(sql_engine):
    assert "exit_code: 0" in handle_run_query(AgentState(), {"sql": "select 1"})
    assert "non-empty string" in handle_run_query(AgentState(), {"sql": "   "})
    assert "non-empty string" in handle_run_query(AgentState(), {"sql": 5})


# --- return_control ---


def test_handle_return_control_sets_state():
    state = AgentState()
    assert handle_return_control(state, {"message": "done"}) == (
        "return_control_to_user: ok"
    )
    assert state.done is True
    assert state.farewell == "done"
    assert state.force_return is False


def test_handle_return_control_force_flag():
    state = AgentState()
    handle_return_control(state, {"message": "override", "force": True})
    assert state.force_return is True


def test_handle_return_control_rejects_non_string():
    state = AgentState()
    assert "must be a string" in handle_return_control(state, {"message": 1})
    assert state.done is False


# --- system prompt + registration ---


def test_sql_system_prompt_branches_on_schema_md():
    with_schema = sql_system_prompt(has_schema_md=True)
    without = sql_system_prompt(has_schema_md=False)
    assert "schema.md" in with_schema
    assert "SHOW TABLES" in without
    assert "plain DuckDB" in with_schema


def test_sql_tools_and_handlers_aligned():
    names = {t.name for t in SQL_TOOLS}
    assert names == set(SQL_TOOL_HANDLERS)
    assert "run_query" in names
    assert "write_file" in names
    for tool in SQL_TOOLS:
        assert tool.input_schema["type"] == "object"
