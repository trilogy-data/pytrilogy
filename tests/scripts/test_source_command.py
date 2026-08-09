import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.dialect.python_source import PythonDatasourceError
from trilogy.io.errors import SCRIPT_ERROR_EXIT_CODE
from trilogy.scripts.source import invoke, source, source_command

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


BROKEN = HEADER + """
import sys
sys.path.insert(0, {root!r})
from trilogy.io import run

def landmarks():
    raise ValueError("the source could not reach its API")

if __name__ == "__main__":
    raise SystemExit(run(landmarks))
"""


@pytest.fixture
def broken(tmp_path: Path) -> str:
    path = tmp_path / "broken.py"
    path.write_text(BROKEN.format(root=str(ROOT)))
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


def test_check_reports_what_a_failing_script_said_about_itself(
    runner: CliRunner, broken: str
):
    """The script's own one-line report, not the uv log plus a traceback."""
    result = runner.invoke(source, ["check", broken])
    assert result.exit_code != 0
    assert "failed --describe" in result.output
    assert "ValueError: the source could not reach its API" in result.output
    assert "Traceback" not in result.output


def test_invoke_raises_on_a_failing_script(broken: str):
    with pytest.raises(PythonDatasourceError) as exc_info:
        invoke(broken, ["--describe"])
    assert exc_info.value.return_code == SCRIPT_ERROR_EXIT_CODE


def test_preview_prints_rows_as_csv(runner: CliRunner, script: str, capfd):
    result = runner.invoke(source, ["preview", script, "--limit", "3"])
    assert result.exit_code == 0, result.output
    lines = capfd.readouterr().out.splitlines()
    assert lines[0] == '"id","state"'
    assert len(lines) == 4


def test_preview_passes_the_contract_flags_through(
    runner: CliRunner, script: str, capfd
):
    result = runner.invoke(
        source,
        [
            "preview",
            script,
            "--limit",
            "2",
            "--columns",
            "state",
            "--filter",
            "state = CA",
            "--since",
            "2026-01-01",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    rows = [json.loads(line) for line in capfd.readouterr().out.splitlines()]
    assert rows == [{"state": "CA"}, {"state": "CA"}]
