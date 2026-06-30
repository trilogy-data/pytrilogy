"""CLI-surface tests for `trilogy run` — ergonomics that matter for agent loops."""

from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli


def test_run_rejects_unknown_dialect_with_usage_error(tmp_path: Path):
    """`trilogy run <file> <bad-dialect>` used to raise an uncaught
    ValueError('... is not a valid Dialects') with a Python traceback. It now
    surfaces as a clean UsageError listing valid dialects."""
    f = tmp_path / "q.preql"
    f.write_text("select 1 -> x;", encoding="utf-8")
    result = CliRunner().invoke(cli, ["run", str(f), "not_a_dialect"])
    assert result.exit_code != 0
    # Click writes UsageError to stderr; CliRunner merges them into output
    # by default. The traceback path would land in result.exception as
    # a raw ValueError, which is what this test guards against.
    combined = result.output or ""
    assert "not a valid dialect" in combined
    assert "duck_db" in combined
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_run_empty_stdin_with_imports_parses_imports_only(tmp_path: Path):
    """`echo "" | trilogy run --import raw/X:X -` lets an agent validate that
    its imports parse without supplying a query body. Used to error
    'No input on stdin.' and exit 2 — now succeeds with zero statements."""
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "tiny.preql").write_text(
        "key id int; property id.name string;", encoding="utf-8"
    )
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        # Replicate the workspace inside the isolated cwd
        (Path(cwd) / "raw").mkdir()
        (Path(cwd) / "raw" / "tiny.preql").write_text(
            "key id int; property id.name string;", encoding="utf-8"
        )
        result = runner.invoke(
            cli, ["run", "--import", "raw/tiny:tiny", "-", "duck_db"], input=""
        )
    assert result.exit_code == 0, (result.output, result.exception)


def test_run_inline_query_with_division_not_treated_as_path(tmp_path: Path):
    """Inline SQL with a `/` (division) used to be misclassified as a missing
    file path. Real agent failure mode: `having total > 0.0001 / 100.0 * sum(...)`
    via `--import raw/X:X` + inline body."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
        (Path(cwd) / "raw").mkdir()
        (Path(cwd) / "raw" / "tiny.preql").write_text(
            "key id int; property id.name string;", encoding="utf-8"
        )
        result = runner.invoke(
            cli,
            [
                "run",
                "--import",
                "raw/tiny:tiny",
                "select 1.0 / 2.0 as half;",
                "duck_db",
            ],
        )
    assert "does not exist" not in (result.output or ""), result.output
    assert result.exit_code == 0, (result.output, result.exception)


def test_format_flag_position_independent():
    """`--format json` is a group option; it must behave identically before
    the subcommand and after it. The post-subcommand form previously fell
    through to the DIALECT positional → 'json is not a valid dialect'."""
    runner = CliRunner()
    before = runner.invoke(
        cli, ["--format", "json", "run", "select 1 -> x;", "duck_db"]
    )
    after = runner.invoke(cli, ["run", "select 1 -> x;", "duck_db", "--format", "json"])
    for result in (before, after):
        assert result.exit_code == 0, (result.output, result.exception)
        assert "not a valid dialect" not in (result.output or "")
        assert "{" in (result.output or "")  # json events, not rich tables


def test_format_flag_after_subcommand_does_not_eat_dialect():
    """With `--format json` placed after the dialect, the real dialect token
    `duck_db` must still bind as the dialect (the value isn't mis-hoisted)."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["run", "select 1 -> x;", "duck_db", "--format", "json"]
    )
    assert result.exit_code == 0, (result.output, result.exception)
    assert "not a valid dialect" not in (result.output or "")


def test_debug_flag_position_independent(tmp_path: Path):
    f = tmp_path / "q.preql"
    f.write_text("select 1 -> x;", encoding="utf-8")
    runner = CliRunner()
    before = runner.invoke(cli, ["--debug", "run", str(f), "duck_db"])
    after = runner.invoke(cli, ["run", str(f), "duck_db", "--debug"])
    assert before.exit_code == 0, (before.output, before.exception)
    assert after.exit_code == 0, (after.output, after.exception)


def test_misplaced_format_value_as_dialect_gives_hint():
    """A bare `json` in the dialect slot (no surviving --format token, e.g.
    after `--`) yields the friendly hint, not a bare 'not a valid dialect'."""
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "select 1 -> x;", "json"])
    assert result.exit_code != 0
    combined = result.output or ""
    assert "not a valid dialect" in combined
    assert "--format" in combined


def _json_events(output: str) -> list[dict]:
    import json

    events, buf = [], ""
    for line in (output or "").splitlines():
        buf += line
        try:
            events.append(json.loads(buf))
            buf = ""
        except json.JSONDecodeError:
            continue
    return events


def test_run_definitions_only_file_is_not_silent_success(tmp_path: Path):
    """A file of only rowset/concept definitions with no consuming `select`
    used to run as `Executing 0 statements... Execution Complete` (exit 0) — a
    silent false success that drove agent churn. It now fails with an actionable
    message naming the missing select."""
    f = tmp_path / "defs.preql"
    f.write_text(
        "key id int;\nwith base_agg as select id, count(id) as c;",
        encoding="utf-8",
    )
    result = CliRunner().invoke(cli, ["run", str(f), "duck_db"])
    assert result.exit_code != 0, (result.output, result.exception)
    out = result.output or ""
    assert "Nothing was executed" in out
    assert "select" in out
    # Breakdown of what parsed (so the author sees the file wasn't empty).
    assert "1 concept" in out and "1 rowset" in out


def test_run_definitions_only_json_reports_not_ok(tmp_path: Path):
    """In JSON mode the summary must report ok:false (not the misleading
    ok:true, rows:0) and an error event must carry the guidance."""
    f = tmp_path / "defs.preql"
    f.write_text("with a as select 1 -> x;", encoding="utf-8")
    result = CliRunner().invoke(cli, ["--format", "json", "run", str(f), "duck_db"])
    assert result.exit_code != 0
    events = _json_events(result.output)
    summary = next(e for e in events if e.get("event") == "summary")
    assert summary["ok"] is False
    assert summary["statements"] == 0
    error = next(e for e in events if e.get("event") == "error")
    assert "Nothing was executed" in error["message"]


def test_run_select_returning_zero_rows_still_ok(tmp_path: Path):
    """The guard keys on zero *executable statements*, not zero result rows: a
    real select that returns no rows is a legitimate ok:true, rows:0."""
    f = tmp_path / "q.preql"
    f.write_text("select 1 -> x where x = 2;", encoding="utf-8")
    result = CliRunner().invoke(cli, ["--format", "json", "run", str(f), "duck_db"])
    assert result.exit_code == 0, (result.output, result.exception)
    summary = next(
        e for e in _json_events(result.output) if e.get("event") == "summary"
    )
    assert summary["ok"] is True
    assert summary["rows"] == 0
