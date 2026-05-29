"""Tests for the agent-info command."""

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli


def test_agent_info():
    """Test that agent-info command outputs expected content."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info"])

    assert result.exit_code == 0

    # Check key sections are present
    assert "# Trilogy CLI - AI Agent Usage Guide" in result.output
    assert "## Overview" in result.output
    assert "## Quick Start" in result.output
    assert "## Commands Reference" in result.output

    # Check all commands are documented
    assert "### trilogy init" in result.output
    assert "### trilogy run" in result.output
    assert "### trilogy unit" in result.output
    assert "### trilogy integration" in result.output
    assert "### trilogy fmt" in result.output
    assert "### trilogy render" in result.output
    assert "### trilogy ingest" in result.output
    assert "### trilogy agent" in result.output
    # Niche commands (serve, public) live behind `agent-info serve`; the main
    # dump points to them but doesn't inline the section bodies.
    assert "trilogy agent-info serve" in result.output

    # Extended-references index advertises every on-demand subcommand so an
    # agent that only reads the main dump knows where to fetch the rest.
    assert "## Extended References" in result.output
    assert "trilogy agent-info report" in result.output
    assert "trilogy agent-info datasources" in result.output
    assert "trilogy agent-info serve" in result.output

    # Check configuration section
    assert "## Configuration File" in result.output
    assert "trilogy.toml" in result.output

    # Check dialects section
    assert "## Supported Dialects" in result.output
    assert "duckdb" in result.output
    assert "postgres" in result.output

    # Check common workflows
    assert "## Common Workflows" in result.output


def test_agent_info_report_subcommand():
    """Test that `agent-info report` prints the report format reference."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "report"])

    assert result.exit_code == 0
    assert "# Trilogy Report Format" in result.output
    assert ":::row" in result.output
    assert "trilogy render" in result.output
    # The general guide should not leak into the focused reference.
    assert "# Trilogy CLI - AI Agent Usage Guide" not in result.output


def test_agent_info_datasources_subcommand():
    """`agent-info datasources` carries the advanced datasource forms — partial
    and Python/Arrow — that are no longer inlined in the main dump."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "datasources"])

    assert result.exit_code == 0
    assert "Complete and Partial Datasources" in result.output
    assert "partial datasource orders_us" in result.output
    assert "Python Script Datasources" in result.output
    assert "enable_python_datasources" in result.output
    # No bleed of unrelated sections.
    assert "# Trilogy CLI - AI Agent Usage Guide" not in result.output


def test_agent_info_serve_subcommand():
    """`agent-info serve` carries the `trilogy public` + `trilogy serve`
    references — distribution/hosting topics moved out of the main dump."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "serve"])

    assert result.exit_code == 0
    assert "trilogy public" in result.output
    assert "trilogy serve" in result.output
    assert "FastAPI server" in result.output
    assert "trilogy-public-models" in result.output
    assert "# Trilogy CLI - AI Agent Usage Guide" not in result.output


def test_main_agent_info_does_not_inline_extracted_sections():
    """The advanced datasource bodies and the public/serve command details
    must NOT appear in the main dump — only the pointers do. Catches a
    regression where a section accidentally lands in both places."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info"])
    assert result.exit_code == 0
    # Substrings that only exist in the BODY of the moved sections, not in
    # the index pointers (which legitimately reference command names).
    assert "partial datasource orders_us" not in result.output
    assert "enable_python_datasources" not in result.output
    assert "/index.json" not in result.output  # serve endpoint table


def test_cli_help_contains_agent_notice():
    """Test that CLI help contains notice for AI agents."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "NOTE FOR AI AGENTS" in result.output
    assert "trilogy agent-info" in result.output
