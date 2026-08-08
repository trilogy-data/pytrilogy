import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.scripts.source import source, source_command

ROOT = Path(__file__).parents[2]

HEADER = """# /// script
# dependencies = ["pyarrow"]
# ///
"""

SCRIPT = HEADER + """
import sys
sys.path.insert(0, {root!r})
from trilogy.io import run

STATES = ["CA", "NY"]

def landmarks(limit=None):
    rows = [{{"id": i, "state": STATES[i % 2]}} for i in range(20)]
    return rows[: limit or len(rows)]

if __name__ == "__main__":
    raise SystemExit(run(landmarks))
"""

LEGACY = HEADER + """
import sys
import pyarrow as pa

table = pa.table({"a": [1, 2]})
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
"""


@pytest.fixture
def script(tmp_path: Path) -> str:
    path = tmp_path / "landmarks.py"
    path.write_text(SCRIPT.format(root=str(ROOT)))
    return str(path)


@pytest.fixture
def legacy(tmp_path: Path) -> str:
    path = tmp_path / "legacy.py"
    path.write_text(LEGACY)
    return str(path)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_source_command_uses_uv_for_python_and_exec_otherwise():
    assert source_command("a.py", ["--describe"])[:2] == ["uv", "run"]
    assert source_command("./bin/landmarks", ["--describe"]) == [
        "./bin/landmarks",
        "--describe",
    ]


def test_describe_prints_a_datasource_block(runner: CliRunner, script: str):
    result = runner.invoke(source, ["describe", script])
    assert result.exit_code == 0, result.output
    assert "contract v1" in result.output
    assert "pushdown: limit" in result.output
    assert "datasource landmarks(" in result.output
    assert "id: id" in result.output


def test_describe_json(runner: CliRunner, script: str):
    result = runner.invoke(source, ["describe", script, "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert [f["name"] for f in payload["schema"]] == ["id", "state"]


def test_check_accepts_a_contract_script(runner: CliRunner, script: str):
    result = runner.invoke(source, ["check", script])
    assert result.exit_code == 0, result.output
    assert "implements contract v1" in result.output


def test_check_explains_a_script_that_predates_the_contract(
    runner: CliRunner, legacy: str
):
    result = runner.invoke(source, ["check", legacy])
    assert result.exit_code != 0
    assert "did not answer --describe with JSON" in result.output
    assert "trilogy.io.run()" in result.output
