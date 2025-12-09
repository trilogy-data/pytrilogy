import importlib.util
import os
import re
from pathlib import Path

import pytest
from click.exceptions import Exit
from click.testing import CliRunner

from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.display import set_rich_mode
from trilogy.scripts.trilogy import cli


def test_ingest():
    path = Path(__file__).parent
    runner = CliRunner()
    config_dir = path / "config_directory"
    args = [
        "ingest",
        "world_capitals",
        "--config",
        str(config_dir / "trilogy.toml"),
    ]
    results = runner.invoke(
        cli,
        args,
    )
    print(args)
    print(results.output)
    if results.exception:
        raise results.exception
    assert results.exit_code == 0

    # Check that the file was created in the raw directory
    raw_dir = config_dir / "raw"
    assert raw_dir.exists()

    output_file = raw_dir / "world_capitals.preql"
    assert output_file.exists()

    # Read and verify the content has the expected structure
    content = output_file.read_text()
    assert "country" in content
    assert "capital" in content
    # The grain detection should identify country as a key
    assert "key country" in content.lower() or "country: country" in content.lower()
