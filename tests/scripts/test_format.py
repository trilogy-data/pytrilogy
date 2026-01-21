"""Tests for the format command."""

import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_format_single_file(runner):
    """Test formatting a single Trilogy script file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.preql"
        # Create a sample unformatted Trilogy script
        unformatted_script = "SELECT 1 as test ;"
        with open(file_path, "w") as f:
            f.write(unformatted_script)

        # Run the format command
        result = runner.invoke(cli, ["fmt", str(file_path)])

        # Check that the command was successful
        assert result.exit_code == 0
        assert "formatted 1 file" in result.output

        # Verify that the file was formatted correctly
        with open(file_path, "r") as f:
            formatted_script = f.read()
        expected_script = "SELECT\n    1 -> test,\n;"
        assert formatted_script.strip() == expected_script


def test_format_directory(runner):
    """Test formatting all Trilogy script files in a directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)
        # Create multiple sample unformatted Trilogy scripts
        scripts = {
            "script1.preql": "select 1              as one;",
            "script2.preql": "select 2 as test;",
        }
        for filename, content in scripts.items():
            with open(dir_path / filename, "w") as f:
                f.write(content)

        # Run the format command on the directory
        result = runner.invoke(cli, ["fmt", str(dir_path)])

        # Check that the command was successful
        assert result.exit_code == 0
        assert "formatted 2 file" in result.output

        # Verify that each file was formatted correctly
        expected_scripts = {
            "script1.preql": "SELECT\n    1 -> one,\n;",
            "script2.preql": "SELECT\n    2 -> test,\n;",
        }
        for filename, expected_content in expected_scripts.items():
            with open(dir_path / filename, "r") as f:
                formatted_script = f.read()
            assert formatted_script.strip() == expected_content


def test_format_directory_error(runner):
    """Test formatting with a syntax error in one of the files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)
        # Create one valid and one invalid Trilogy script
        valid_script = "SELECT 1 as test;"
        invalid_script = "SELECT col2 col3 col4"  # Missing column name
        with open(dir_path / "valid.preql", "w") as f:
            f.write(valid_script)
        with open(dir_path / "invalid.preql", "w") as f:
            f.write(invalid_script)

        # Run the format command on the directory
        result = runner.invoke(cli, ["fmt", str(dir_path)])

        # Check that the command reports the error
        assert result.exit_code == 0
        assert "1/2 files" in result.output
        assert "Failed to format" in result.output
        assert "Syntax" in result.output  # Error message should mention syntax
