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
    assert "### trilogy ingest" in result.output
    assert "### trilogy serve" in result.output
    assert "### trilogy agent" in result.output

    # Check configuration section
    assert "## Configuration File" in result.output
    assert "trilogy.toml" in result.output

    # Check dialects section
    assert "## Supported Dialects" in result.output
    assert "duckdb" in result.output
    assert "postgres" in result.output

    # Check common workflows
    assert "## Common Workflows" in result.output


def test_cli_help_contains_agent_notice():
    """Test that CLI help contains notice for AI agents."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "NOTE FOR AI AGENTS" in result.output
    assert "trilogy agent-info" in result.output
