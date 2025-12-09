"""Tests for the trilogy init command."""
import tempfile
from pathlib import Path

from click.testing import CliRunner

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
        assert (workspace / "raw").is_dir()
        assert (workspace / "derived").is_dir()
        assert (workspace / "jobs").is_dir()


def test_init_current_directory():
    """Test that init works with current directory (default path)."""
    runner = CliRunner()

    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["init"])

        assert result.exit_code == 0
        assert Path("trilogy.toml").exists()
        assert Path("hello_world.preql").exists()
        assert Path("raw").is_dir()
        assert Path("derived").is_dir()
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


def test_init_creates_parent_directories():
    """Test that init creates parent directories if they don't exist."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmpdir:
        nested_path = Path(tmpdir) / "nested" / "path" / "workspace"
        result = runner.invoke(cli, ["init", str(nested_path)])

        assert result.exit_code == 0
        assert (nested_path / "trilogy.toml").exists()
        assert nested_path.is_dir()
