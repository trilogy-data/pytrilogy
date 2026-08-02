"""Tests for the trilogy init command."""

import tempfile
from pathlib import Path

from click.testing import CliRunner

from trilogy.dialect.config import PostgresConfig
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import load_config_file
from trilogy.scripts.project_config import MODEL_ROOT_DIR
from trilogy.scripts.trilogy import cli


def test_init_creates_workspace_structure():
    """Test that init creates all expected files and directories."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir])

        # Check exit code
        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Check success message
        assert "Workspace initialized successfully" in result.output

        # Verify directory structure
        workspace = Path(tmpdir)
        assert (workspace / "trilogy.toml").exists()
        assert (workspace / "hello_world.preql").exists()
        assert (workspace / "root").is_dir()
        assert (workspace / "jobs").is_dir()


def test_init_current_directory():
    """Test that init works with current directory (default path)."""
    runner = CliRunner()

    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert Path("trilogy.toml").exists()
        assert Path("hello_world.preql").exists()
        assert Path("root").is_dir()
        assert Path("jobs").is_dir()


def test_init_fails_if_already_initialized():
    """Test that init fails if workspace already exists."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Initialize once
        result = runner.invoke(cli, ["init", tmpdir])
        assert result.exit_code == 0

        # Try to initialize again
        result = runner.invoke(cli, ["init", tmpdir])
        assert result.exit_code == 1
        assert "already initialized" in result.output
        assert "--force" in result.output


def test_init_force_overwrites_config():
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        assert runner.invoke(cli, ["init", tmpdir]).exit_code == 0

        result = runner.invoke(cli, ["init", tmpdir, "bigquery", "--force"])
        assert result.exit_code == 0, result.output
        assert "Overwrote configuration" in result.output
        assert 'dialect = "bigquery"' in (Path(tmpdir) / "trilogy.toml").read_text()


def test_init_force_keeps_existing_script():
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        assert runner.invoke(cli, ["init", tmpdir]).exit_code == 0
        hello_world = Path(tmpdir) / "hello_world.preql"
        hello_world.write_text("key edited int;\n")

        result = runner.invoke(cli, ["init", tmpdir, "--force"])
        assert result.exit_code == 0, result.output
        assert hello_world.read_text() == "key edited int;\n"


def test_init_force_into_bare_directory():
    """--force is not an error when there is nothing to overwrite."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir, "--force"])
        assert result.exit_code == 0, result.output
        assert "Created configuration" in result.output
        assert (Path(tmpdir) / "hello_world.preql").exists()


def test_init_trilogy_toml_content():
    """Test that trilogy.toml has expected content."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir])
        assert result.exit_code == 0

        config_path = Path(tmpdir) / "trilogy.toml"
        content = config_path.read_text()

        assert "[engine]" in content
        assert "# dialect" in content
        assert "# parallelism" in content
        assert "https://github.com/trilogy-data/pytrilogy" in content
        assert "[engine.config]" not in content


def test_init_with_dialect():
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir, "bigquery"])
        assert result.exit_code == 0, result.output

        content = (Path(tmpdir) / "trilogy.toml").read_text()
        assert 'dialect = "bigquery"' in content
        assert "[engine.config]" in content
        assert "# project = " in content

        config = load_config_file(Path(tmpdir) / "trilogy.toml")
        assert config.engine_dialect == Dialects.BIGQUERY


def test_init_dialect_alias_normalized():
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir, "duckdb"])
        assert result.exit_code == 0, result.output
        assert 'dialect = "duck_db"' in (Path(tmpdir) / "trilogy.toml").read_text()


def test_init_dialect_required_params_loadable():
    """Required params are written uncommented, so the file still parses."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir, "postgres"])
        assert result.exit_code == 0, result.output

        config = load_config_file(Path(tmpdir) / "trilogy.toml")
        assert config.engine_dialect == Dialects.POSTGRES
        assert isinstance(config.engine_config, PostgresConfig)
        assert config.engine_config.host == "localhost"


def test_init_invalid_dialect():
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir, "oracle"])
        assert result.exit_code != 0
        assert "not a valid dialect" in result.output
        assert not (Path(tmpdir) / "trilogy.toml").exists()


def test_init_dialect_in_path_slot_rejected():
    runner = CliRunner()

    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["init", "bigquery"])
        assert result.exit_code != 0
        assert "trilogy init . bigquery" in result.output
        assert not Path("bigquery").exists()


def test_init_hello_world_is_valid():
    """Test that the generated hello_world.preql is valid and can be parsed."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Initialize workspace
        result = runner.invoke(cli, ["init", tmpdir])
        assert result.exit_code == 0

        # Try to run unit test on hello_world.preql
        hello_world = Path(tmpdir) / "hello_world.preql"
        result = runner.invoke(cli, ["unit", str(hello_world)])

        # Should execute successfully
        assert result.exit_code == 0, f"hello_world.preql failed: {result.output}"


def test_init_shows_header():
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["init", tmpdir, "duckdb"])
        assert result.exit_code == 0
        assert "Trilogy Init" in result.output
        assert "duck_db" in result.output


def test_init_scaffold_is_ingest_default_output():
    """The directory init scaffolds is the one ingest writes into by default."""
    runner = CliRunner()

    with runner.isolated_filesystem():
        assert runner.invoke(cli, ["init"]).exit_code == 0
        Path("orders.csv").write_text("id,total\n1,2.5\n2,3.5\n")

        result = runner.invoke(cli, ["ingest", "orders.csv"])
        assert result.exit_code == 0, result.output
        assert (Path(MODEL_ROOT_DIR) / "orders.preql").exists()


def test_init_creates_parent_directories():
    """Test that init creates parent directories if they don't exist."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        nested_path = Path(tmpdir) / "nested" / "path" / "workspace"
        result = runner.invoke(cli, ["init", str(nested_path)])

        assert result.exit_code == 0
        assert (nested_path / "trilogy.toml").exists()
        assert nested_path.is_dir()
