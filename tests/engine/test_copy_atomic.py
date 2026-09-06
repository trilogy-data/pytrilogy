from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.execution.staged_write import STAGING_DIR

_MODEL = """
key id int;
property id.name string;

datasource src (id, name) grain (id) address src;
"""


def _executor(tmp_path: Path, *, failing: bool) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql("CREATE TABLE base AS SELECT 1 AS id, 'alpha' AS name")
    name = "CASE WHEN id > 0 THEN error('boom') ELSE name END" if failing else "name"
    executor.execute_raw_sql(f"CREATE VIEW src AS SELECT id, {name} AS name FROM base")
    executor.parse_text(_MODEL)
    return executor


@pytest.mark.parametrize("fmt", ["parquet", "csv", "json"])
def test_copy_lands_only_the_final_file(tmp_path: Path, fmt: str):
    target = tmp_path / f"out.{fmt}"
    list(
        _executor(tmp_path, failing=False).execute_text(
            f"copy into {fmt} '{target.as_posix()}' from select id, name;"
        )
    )
    assert target.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == [target.name]


def test_failed_copy_keeps_previous_artifact(tmp_path: Path):
    target = tmp_path / "out.parquet"
    good = _executor(tmp_path, failing=False)
    list(
        good.execute_text(
            f"copy into parquet '{target.as_posix()}' from select id, name;"
        )
    )
    before = target.read_bytes()

    bad = _executor(tmp_path, failing=True)
    with pytest.raises(Exception, match="boom"):
        list(
            bad.execute_text(
                f"copy into parquet '{target.as_posix()}' from select id, name;"
            )
        )
    assert target.read_bytes() == before
    assert not (tmp_path / STAGING_DIR).exists()
    assert duckdb.sql(f"SELECT name FROM '{target.as_posix()}'").fetchall() == [
        ("alpha",)
    ]


def test_failed_copy_on_fresh_target_leaves_nothing(tmp_path: Path):
    target = tmp_path / "out.parquet"
    with pytest.raises(Exception, match="boom"):
        list(
            _executor(tmp_path, failing=True).execute_text(
                f"copy into parquet '{target.as_posix()}' from select id, name;"
            )
        )
    assert list(tmp_path.iterdir()) == []


def test_relative_target_resolves_under_working_path(tmp_path: Path):
    list(
        _executor(tmp_path, failing=False).execute_text(
            "copy into csv 'rel.csv' from select id, name;"
        )
    )
    assert (tmp_path / "rel.csv").exists()
    assert not (tmp_path / STAGING_DIR).exists()


def test_chart_copy_is_staged(tmp_path: Path):
    pytest.importorskip("altair")
    pytest.importorskip("vl_convert")
    target = tmp_path / "chart.svg"
    list(
        _executor(tmp_path, failing=False).execute_text(
            f"copy into svg '{target.as_posix()}' from chart layer bar "
            "( x_axis <- id, y_axis <- name );"
        )
    )
    assert target.read_text(encoding="utf-8").lstrip().startswith("<svg")
    assert not (tmp_path / STAGING_DIR).exists()
