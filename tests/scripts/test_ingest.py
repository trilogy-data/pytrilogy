import importlib.util
import os
import re
from pathlib import Path
import tempfile

import pytest
from click.exceptions import Exit
from click.testing import CliRunner

from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.display import set_rich_mode
from trilogy.scripts.trilogy import cli

from trilogy.scripts.ingest import 


def test_ingest():
    path = Path(__file__).parent
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        args = [
                "ingest",
                "world_capitals",
                "--config",
                str(path / "config_directory" / "trilogy.toml"),
            ]
        results = runner.invoke(
            cli,
            args,
        )
        print(args)
        if results.exception:
            raise results.exception
        assert results.exit_code == 0
        for file in tmp_path.glob("**/*.preql"):
            assert file.name == "world_capitals"
