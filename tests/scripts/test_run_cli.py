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
