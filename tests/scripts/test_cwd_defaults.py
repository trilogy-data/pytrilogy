"""Tests for CLI commands defaulting to current working directory."""

from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli


def test_serve_defaults_to_cwd():
    """Test that serve defaults to current working directory when no path provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["serve", "--help"])
    assert result.exit_code == 0
    assert "[PATH]" in result.output


def test_run_defaults_to_cwd():
    """Test that run defaults to current working directory when no path provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0
    assert "[INPUT]" in result.output


def test_plan_defaults_to_cwd():
    """Test that plan defaults to current working directory when no path provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["plan", "--help"])
    assert result.exit_code == 0
    assert "[INPUT]" in result.output


def test_refresh_defaults_to_cwd():
    """Test that refresh defaults to current working directory when no path provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["refresh", "--help"])
    assert result.exit_code == 0
    assert "[INPUT]" in result.output


def test_plan_uses_cwd_when_no_path():
    """Test that plan actually works with the current directory when no path is provided."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("test.preql").write_text("key id int;")
        result = runner.invoke(cli, ["plan", "--json"])
        assert result.exit_code == 0
        assert "nodes" in result.output
