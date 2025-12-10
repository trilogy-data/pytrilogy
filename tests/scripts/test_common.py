import tempfile
from pathlib import Path

from trilogy.scripts.common import find_trilogy_config


def test_find_trilogy_config_in_current_directory():
    """Test finding trilogy.toml in the starting directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text("[engine]\ndialect = 'duckdb'")

        result = find_trilogy_config(tmppath)
        assert result == config_file


def test_find_trilogy_config_in_parent_directory():
    """Test finding trilogy.toml in a parent directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text("[engine]\ndialect = 'duckdb'")

        subdir = tmppath / "subdir" / "nested"
        subdir.mkdir(parents=True)

        result = find_trilogy_config(subdir)
        assert result == config_file


def test_find_trilogy_config_from_file_path():
    """Test finding trilogy.toml when starting from a file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text("[engine]\ndialect = 'duckdb'")

        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        result = find_trilogy_config(test_file)
        assert result == config_file


def test_find_trilogy_config_not_found():
    """Test when trilogy.toml is not found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        subdir = tmppath / "subdir"
        subdir.mkdir()

        result = find_trilogy_config(subdir)
        assert result is None


def test_find_trilogy_config_uses_cwd_when_none():
    """Test that find_trilogy_config uses current working directory when no path provided."""
    result = find_trilogy_config(None)
    # Should return None or a valid path, but shouldn't crash
    assert result is None or isinstance(result, Path)


def test_find_trilogy_config_stops_at_first_match():
    """Test that search stops at the first trilogy.toml found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        # Create trilogy.toml at root
        root_config = tmppath / "trilogy.toml"
        root_config.write_text("[engine]\ndialect = 'bigquery'")

        # Create nested directory with its own trilogy.toml
        subdir = tmppath / "subdir"
        subdir.mkdir()
        subdir_config = subdir / "trilogy.toml"
        subdir_config.write_text("[engine]\ndialect = 'duckdb'")

        # Starting from subdir should find subdir config first
        result = find_trilogy_config(subdir)
        assert result == subdir_config

        # Starting from deeper nested dir should find subdir config
        deeper = subdir / "deeper"
        deeper.mkdir()
        result = find_trilogy_config(deeper)
        assert result == subdir_config
