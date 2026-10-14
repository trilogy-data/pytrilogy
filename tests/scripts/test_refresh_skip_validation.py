"""`refresh --skip-validation <datasource>` builds past a failing `assert`,
loudly; `--dry-run` lists the assertions that would gate each build."""

import json

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

MODEL = """key id int;
property id.source string;

root datasource src (id: id, source: source)
grain (id)
file `{data}/src.parquet`;

datasource built (id: id, source: source)
grain (id)
file `{data}/built.parquet`
freshness by id
assert count(id) >= 0.75 * prior(count(id))
assert count(id) by source >= 0.75 * prior(count(id) by source)
;
"""


def _write_src(path, rows: int) -> None:
    con = duckdb.connect()
    con.execute(
        f"""COPY (SELECT i AS id, CASE WHEN i % 10 = 0 THEN 'B' ELSE 'A' END AS source
            FROM range({rows}) t(i)) TO '{path.as_posix()}' (FORMAT PARQUET)"""
    )
    con.close()


def _count(path) -> int:
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT count(*) FROM '{path.as_posix()}'").fetchone()[0]
    finally:
        con.close()


@pytest.fixture
def project(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    _write_src(data / "src.parquet", 1000)
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duckdb"\n')
    (tmp_path / "model.preql").write_text(
        MODEL.format(data=data.as_posix()), encoding="utf-8"
    )
    runner = CliRunner()
    first = runner.invoke(cli, ["refresh", str(tmp_path / "model.preql"), "duckdb"])
    assert first.exit_code == 0, first.output
    assert _count(data / "built.parquet") == 1000
    _write_src(data / "src.parquet", 10)
    return tmp_path, data, runner


def _refresh(runner: CliRunner, root, *args: str):
    return runner.invoke(cli, ["refresh", str(root / "model.preql"), "duckdb", *args])


def _records(path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_failing_assertion_fails_the_refresh(project):
    root, data, runner = project
    result = _refresh(runner, root, "--force", "built")
    assert result.exit_code == 1, result.output
    flat = " ".join(result.output.split())
    assert "assertion(s) failed" in flat
    assert "count(id) = 10, prior(count(id)) = 1000" in flat
    assert _count(data / "built.parquet") == 1000


def test_skip_validation_builds_and_says_so(project, tmp_path):
    root, data, runner = project
    report = tmp_path / "exec.jsonl"
    result = _refresh(
        runner,
        root,
        "--force",
        "built",
        "--skip-validation",
        "built",
        "--report-file",
        str(report),
    )
    assert result.exit_code == 0, result.output
    assert "skipping 2 assertion(s) on built (--skip-validation)" in " ".join(
        result.output.split()
    )
    assert _count(data / "built.parquet") == 10
    (record,) = [r for r in _records(report) if r["type"] == "asset_assertions"]
    assert record["datasource_id"] == "built"
    assert record["skipped"] is True
    assert record["assertions"] == [
        "count(id) >= 0.75 * prior(count(id))",
        "count(id) by source >= 0.75 * prior(count(id) by source)",
    ]


def test_dry_run_lists_assertions(project, tmp_path):
    root, data, runner = project
    report = tmp_path / "exec.jsonl"
    result = _refresh(
        runner, root, "--force", "built", "--dry-run", "--report-file", str(report)
    )
    assert result.exit_code == 0, result.output
    flat = " ".join(result.output.split())
    assert "Would refresh built" in flat
    assert "assert count(id) >= 0.75 * prior(count(id))" in flat
    assert _count(data / "built.parquet") == 1000
    (record,) = [r for r in _records(report) if r["type"] == "asset_assertions"]
    assert record["dry_run"] is True and "skipped" not in record


@pytest.mark.parametrize(
    "name,message",
    [
        ("nope", "Unknown datasource passed to --skip-validation: nope"),
        ("src", "declares no `assert` clause"),
    ],
)
def test_skip_validation_rejects_bad_names(project, name, message):
    root, _, runner = project
    result = _refresh(runner, root, "--skip-validation", name)
    assert result.exit_code == 1, result.output
    assert message in " ".join(result.output.split())
