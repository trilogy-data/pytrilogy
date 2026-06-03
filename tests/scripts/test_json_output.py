"""JSON output-format tests for the Trilogy CLI.

`--format json` (and the `TRILOGY_OUTPUT_FORMAT=json` env var the agent
subprocess sets) makes every agent-facing command emit a stream of
pretty-printed JSON event objects instead of rich-formatted text — same
information, none of the formatting chrome. Events are newline-separated and
read with a streaming decoder (not line-by-line, since each object spans
multiple lines)."""

import json
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

TOML = '[engine]\ndialect = "duck_db"\n\n[engine.config]\ndb_location = "test.duckdb"\n'


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def _reset_output_format():
    """Each ``--format json`` invocation flips the process-global output format;
    restore ``rich`` after every test so we don't leak JSON mode into unrelated
    tests that assume the default rendering."""
    from trilogy.scripts import display_core

    yield
    display_core.set_output_format("rich")


def parse_events(output: str) -> list[dict]:
    """Decode successive pretty-printed JSON objects from stdout, each carrying
    an ``event`` field. Raises ``json.JSONDecodeError`` on non-JSON (rich)
    output."""
    decoder = json.JSONDecoder()
    events = []
    idx, n = 0, len(output)
    while idx < n:
        while idx < n and output[idx].isspace():
            idx += 1
        if idx >= n:
            break
        payload, end = decoder.raw_decode(output, idx)
        assert "event" in payload, payload
        events.append(payload)
        idx = end
    return events


def events_of(events: list[dict], name: str) -> list[dict]:
    return [e for e in events if e["event"] == name]


def _db_workspace(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "test.duckdb"))
    con.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'a'), (2, 'b')")
    con.execute("CHECKPOINT")
    con.close()
    (tmp_path / "trilogy.toml").write_text(TOML, encoding="utf-8")


def test_run_inline_emits_ndjson_result(runner):
    result = runner.invoke(
        cli,
        ["--format", "json", "run", "select 1 -> num, 'hi' -> greeting;", "duck_db"],
    )
    assert result.exit_code == 0, result.output
    events = parse_events(result.output)
    results = events_of(events, "result")
    assert len(results) == 1
    payload = results[0]
    assert payload["columns"] == ["num", "greeting"]
    assert payload["rows"] == [[1, "hi"]]
    assert payload["row_count"] == 1
    assert events_of(events, "summary")[0]["ok"] is True


def test_run_displayed_rows_truncation_reported(runner):
    """The result event reports the true total even when middle-truncated."""
    query = "select unnest([1,2,3,4,5,6,7,8,9,10]) -> n;"  # 10 rows
    result = runner.invoke(
        cli, ["--format", "json", "run", query, "duck_db", "--displayed-rows", "4"]
    )
    assert result.exit_code == 0, result.output
    payload = events_of(parse_events(result.output), "result")[0]
    assert payload["row_count"] == 10
    assert payload["displayed"] == 4
    assert payload["truncated"] is True
    assert payload["omitted"] == 6


def test_run_error_emits_error_event_and_nonzero_exit(runner):
    result = runner.invoke(
        cli, ["--format", "json", "run", "select notacolumn;", "duck_db"]
    )
    assert result.exit_code != 0
    assert events_of(parse_events(result.output), "error")


def test_env_var_enables_json(runner, monkeypatch):
    monkeypatch.setenv("TRILOGY_OUTPUT_FORMAT", "json")
    result = runner.invoke(cli, ["run", "select 1 -> num;", "duck_db"])
    assert result.exit_code == 0, result.output
    assert events_of(parse_events(result.output), "result")[0]["rows"] == [[1]]


def test_format_flag_overrides_env_back_to_rich(runner, monkeypatch):
    monkeypatch.setenv("TRILOGY_OUTPUT_FORMAT", "json")
    result = runner.invoke(
        cli, ["--format", "rich", "run", "select 1 -> num;", "duck_db"]
    )
    assert result.exit_code == 0, result.output
    # Rich output is not parseable as a stream of JSON events.
    with pytest.raises(json.JSONDecodeError):
        parse_events(result.output)


def test_default_is_rich(runner):
    result = runner.invoke(cli, ["run", "select 1 -> num;", "duck_db"])
    assert result.exit_code == 0, result.output
    with pytest.raises(json.JSONDecodeError):
        parse_events(result.output)


def test_explore_emits_concepts(runner, tmp_path):
    f = tmp_path / "flight.preql"
    f.write_text(
        "key id int;\nproperty id.carrier string;\n"
        "datasource flights (id, carrier) grain(id) "
        "query '''select 1 as id, 'AA' as carrier''';\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        cli, ["--format", "json", "explore", str(f), "--show", "all"]
    )
    assert result.exit_code == 0, result.output
    events = parse_events(result.output)
    concepts = events_of(events, "concepts")[0]
    decls = [d for ns in concepts["namespaces"].values() for d in ns]
    assert any("id int" in d for d in decls)
    assert any("carrier" in d for d in decls)
    assert events_of(events, "datasources")[0]["count"] == 1


def test_file_roundtrip_events(runner, tmp_path):
    target = tmp_path / "q.preql"
    body = "key id int;\n"
    write = runner.invoke(
        cli, ["--format", "json", "file", "write", str(target), "--content", body]
    )
    assert write.exit_code == 0, write.output
    assert events_of(parse_events(write.output), "write")[0]["path"] == str(target)

    read = runner.invoke(cli, ["--format", "json", "file", "read", str(target)])
    assert read.exit_code == 0, read.output
    assert events_of(parse_events(read.output), "file")[0]["content"] == body

    exists = runner.invoke(cli, ["--format", "json", "file", "exists", str(target)])
    assert exists.exit_code == 0
    assert events_of(parse_events(exists.output), "exists")[0]["exists"] is True

    missing = runner.invoke(
        cli, ["--format", "json", "file", "exists", str(tmp_path / "nope.preql")]
    )
    assert missing.exit_code == 1
    assert events_of(parse_events(missing.output), "exists")[0]["exists"] is False


def test_database_list_and_describe_events(runner, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _db_workspace(tmp_path)
    listed = runner.invoke(cli, ["--format", "json", "database", "list"])
    assert listed.exit_code == 0, listed.output
    tables = events_of(parse_events(listed.output), "tables")[0]["tables"]
    assert any(t["name"] == "users" for t in tables)

    described = runner.invoke(
        cli, ["--format", "json", "database", "describe", "users"]
    )
    assert described.exit_code == 0, described.output
    cols = events_of(parse_events(described.output), "columns")[0]["columns"]
    assert {c["name"] for c in cols} == {"id", "name"}


def test_plan_emits_plan_event(runner, tmp_path):
    f = tmp_path / "q.preql"
    f.write_text("select 1 -> x;\n", encoding="utf-8")
    result = runner.invoke(cli, ["--format", "json", "plan", str(f)])
    assert result.exit_code == 0, result.output
    plan = events_of(parse_events(result.output), "plan")[0]
    assert plan["execution_order"] == [["q.preql"]]
