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
