"""Tests for the agent-info command."""

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli


def legacy_cli_reference_assertions():
    """Detailed command documentation moved behind the CLI drilldown."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "cli"])

    assert result.exit_code == 0

    # Check key sections are present
    assert "# Agent Usage Guide" in result.output
    assert "## Overview" in result.output
    assert "## Commands Reference" in result.output

    # Check core commands stay documented in the main dump.
    assert "### trilogy init" in result.output
    assert "### trilogy run" in result.output
    assert "### trilogy unit" in result.output
    assert "### trilogy integration" in result.output
    assert "### trilogy fmt" in result.output
    # render/ingest now have short stubs that point at their subcommands —
    # the heading is present but the full body has moved.
    assert "### trilogy render" in result.output
    assert "### trilogy ingest" in result.output

    # Extended-references index advertises every on-demand subcommand so an
    # agent that only reads the main dump knows where to fetch the rest.
    assert "## Extended References" in result.output
    assert "trilogy agent-info report" in result.output
    assert "trilogy agent-info datasources" in result.output
    assert "trilogy agent-info ingest" in result.output
    assert "trilogy agent-info config" in result.output
    assert "trilogy agent-info serve" in result.output
    assert "trilogy agent-info state" in result.output

    # Check configuration section (stub in the main dump; full schema +
    # dialects moved to `agent-info config`).
    assert "## Configuration File" in result.output
    assert "trilogy.toml" in result.output

    # Check common workflows
    assert "## Common Workflows" in result.output


def test_agent_info_cli_subcommand():
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "cli"])

    assert result.exit_code == 0
    assert "# Trilogy CLI and Workspace Operations" in result.output
    for command in (
        "init",
        "run",
        "explore",
        "file list",
        "file write",
        "fmt",
        "unit",
        "integration",
        "database list",
    ):
        assert f"trilogy {command}" in result.output
    assert len(result.output) < 5000
    assert "# Trilogy Report Format" not in result.output
    assert "enable_python_datasources" not in result.output


def test_agent_info_directory():
    """Bare agent-info is a compact routing directory."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info"])

    assert result.exit_code == 0
    for header in (
        "## Query authoring",
        "## Model, script, and datasource authoring",
        "## Creating, running, and managing projects and scripts",
    ):
        assert header in result.output
    for drilldown in (
        "query",
        "authoring",
        "cli",
        "syntax",
        "datasources",
        "ingest",
        "config",
        "report",
        "state",
        "serve",
    ):
        assert f"trilogy agent-info {drilldown}" in result.output
    assert len(result.output) < 5000
    assert "### trilogy run" not in result.output
    assert "enable_python_datasources" not in result.output


def test_agent_info_query_subcommand():
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "query"])

    assert result.exit_code == 0
    assert "## Trilogy Language Reference" in result.output
    assert "## Query structure" in result.output
    assert "python-datasource" in result.output
    assert "# Agent Usage Guide" not in result.output


def test_agent_info_authoring_subcommand():
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "authoring"])

    assert result.exit_code == 0
    assert "# Trilogy Model, Script, and Datasource Authoring" in result.output
    assert "Python Script Datasources" in result.output
    assert "syntax example python-datasource" in result.output
    assert "enable_python_datasources" in result.output


def test_agent_info_state_subcommand():
    """`agent-info state` is the on-demand reference for persisting asset state
    to a file and reading it back; the main dump only points at it."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "state"])

    assert result.exit_code == 0
    assert "# Trilogy Persisted State" in result.output
    for flag in ("--state-file", "--state-input", "--report-file", "--run-id"):
        assert flag in result.output
    for env_var in (
        "TRILOGY_STATE_FILE",
        "TRILOGY_STATE_INPUT",
        "TRILOGY_REPORT_FILE",
        "TRILOGY_RUN_ID",
    ):
        assert env_var in result.output
    # The two load-bearing rules of the read path.
    assert "physical address" in result.output
    assert "roots are always re-probed" in result.output.lower()

    # Esoteric: the body lives here, not in the always-loaded main dump.
    main = runner.invoke(cli, ["agent-info"])
    assert "# Trilogy Persisted State" not in main.output


def test_agent_info_report_subcommand():
    """`agent-info report` carries both the `trilogy render` command flags
    and the report markdown format (chart blocks, :::row layout)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "report"])

    assert result.exit_code == 0
    assert "# Trilogy Report Format" in result.output
    assert ":::row" in result.output
    # Render command details now live here, not in the main dump.
    assert "trilogy render" in result.output
    assert "--to {png|html}" in result.output
    assert "--theme {inter|inter-dark|editorial|editorial-dark}" in result.output
    assert "playwright install chromium" in result.output
    # The general guide should not leak into the focused reference.
    assert "# Agent Usage Guide" not in result.output


def test_agent_info_datasources_subcommand():
    """`agent-info datasources` is the one-stop datasource authoring reference
    — covers root, file-based, partial, and Python/Arrow forms."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "datasources"])

    assert result.exit_code == 0
    # Basic forms (moved from main dump)
    assert "## Root Datasources" in result.output
    assert "## File-Based Datasources" in result.output
    assert "root datasource raw_rides" in result.output
    # Advanced forms (already lived here)
    assert "Complete and Partial Datasources" in result.output
    assert "partial datasource orders_us" in result.output
    assert "Python Script Datasources" in result.output
    assert "enable_python_datasources" in result.output
    # No bleed of unrelated sections.
    assert "# Agent Usage Guide" not in result.output


def test_agent_info_ingest_subcommand():
    """`agent-info ingest` carries the full `trilogy ingest` command reference
    — args, options, and the warehouse/file/cloud examples that the main dump
    no longer inlines."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "ingest"])

    assert result.exit_code == 0
    assert "trilogy ingest" in result.output
    assert "--fks SPEC" in result.output
    assert "--all" in result.output
    # Distinct example signatures from the moved body.
    assert "trilogy ingest --all" in result.output
    assert "gs://my-bucket/sales.parquet" in result.output
    assert "# Agent Usage Guide" not in result.output


def test_agent_info_config_subcommand():
    """`agent-info config` carries the `trilogy.toml` schema and the env-var
    API-key convention — the workspace config rarely needs editing during a
    query task, so the full reference moved out of the main dump."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "config"])

    assert result.exit_code == 0
    assert "trilogy.toml" in result.output
    assert "[engine]" in result.output
    assert "[setup]" in result.output
    assert "[agent]" in result.output
    # API-key conventions live here too.
    assert "OPENROUTER_API_KEY" in result.output
    assert "ANTHROPIC_API_KEY" in result.output
    # Supported dialects moved here with the rest of the config reference.
    assert "## Supported Dialects" in result.output
    assert "duckdb" in result.output
    assert "postgres" in result.output
    assert "# Agent Usage Guide" not in result.output


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
    assert "# Agent Usage Guide" not in result.output


def test_main_agent_info_does_not_inline_extracted_sections():
    """The moved section bodies must NOT appear in the main dump — only the
    pointers do. Catches a regression where content accidentally lives in
    both places."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info"])
    assert result.exit_code == 0
    # Substrings that only exist in the BODY of the moved sections.
    assert "partial datasource orders_us" not in result.output
    assert "enable_python_datasources" not in result.output
    assert "/index.json" not in result.output  # serve endpoint table
    # Render full body indicators
    assert "--theme {inter|editorial}" not in result.output
    assert "playwright install chromium" not in result.output
    # Ingest full body indicators
    assert "--fks SPEC" not in result.output
    assert "gs://my-bucket/sales.parquet" not in result.output
    # Root/File-Based datasource example bodies
    assert "root datasource raw_rides" not in result.output
    assert "state unpublished" not in result.output
    # Config body indicators — the example `[engine]/[setup]/[agent]` block
    # and the API-key list no longer live in the main dump.
    assert "ANTHROPIC_API_KEY" not in result.output
    assert "OPENROUTER_API_KEY" not in result.output
    assert 'trilogy = ["setup.preql"]' not in result.output


def test_agent_info_syntax_lists_examples():
    """`agent-info syntax` with no subcommand lists the available examples."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "syntax"])
    assert result.exit_code == 0
    assert "Available Trilogy syntax examples" in result.output
    assert "trilogy agent-info syntax example" in result.output


def test_agent_info_syntax_example_no_name_lists():
    """`agent-info syntax example` with no NAME also prints the index."""
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "syntax", "example"])
    assert result.exit_code == 0
    assert "Available Trilogy syntax examples" in result.output


def test_agent_info_syntax_example_renders_named_body():
    from trilogy.ai.syntax_examples import available_names

    name = available_names()[0]
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "syntax", "example", name])
    assert result.exit_code == 0
    assert result.output.startswith("# ")


def test_agent_info_syntax_example_unknown_name_errors():
    runner = CliRunner()
    result = runner.invoke(cli, ["agent-info", "syntax", "example", "no-such-example"])
    assert result.exit_code == 2
    assert "Unknown syntax example" in result.output
    assert "Available Trilogy syntax examples" in result.output


def test_cli_help_contains_agent_notice():
    """Test that CLI help contains notice for AI agents."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "NOTE FOR AI AGENTS" in result.output
    assert "trilogy agent-info" in result.output
