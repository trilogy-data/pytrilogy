"""Tests for `trilogy explore` and `trilogy run --import`."""

from pathlib import Path
from textwrap import dedent

import pytest
from click.testing import CliRunner

from trilogy.scripts.run import _normalize_import
from trilogy.scripts.trilogy import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_preql(tmp_path: Path) -> Path:
    path = tmp_path / "flight.preql"
    path.write_text(
        dedent(
            """
            key id int;
            property id.carrier string;
            property id.distance int;

            datasource flights (
                id,
                carrier,
                distance
            )
            grain(id)
            query '''select 1 as id, 'AA' as carrier, 100 as distance''';
            """
        ).strip()
        + "\n"
    )
    return path


def test_explore_lists_concepts(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql)])
    assert result.exit_code == 0, result.output
    assert "Concepts" in result.output
    assert "local.id" in result.output
    assert "local.carrier" in result.output
    assert "local.distance" in result.output
    assert "flights" in result.output


def test_explore_hides_builtins_by_default(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql)])
    assert result.exit_code == 0
    assert "__preql_internal" not in result.output


def test_explore_include_builtins(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--include-builtins"])
    assert result.exit_code == 0
    assert "__preql_internal" in result.output


def test_explore_show_concepts_only(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--show", "concepts"])
    assert result.exit_code == 0
    assert "Concepts" in result.output
    assert "Datasources" not in result.output
    assert "Imports" not in result.output


def test_explore_show_datasources_only(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--show", "datasources"])
    assert result.exit_code == 0
    assert "Datasources" in result.output
    assert "flights" in result.output
    assert "Concepts" not in result.output


def test_explore_purpose_filter(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--purpose", "key"])
    assert result.exit_code == 0
    assert "local.id" in result.output
    assert "local.carrier" not in result.output


def test_explore_grep_filter(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--grep", "carrier"])
    assert result.exit_code == 0
    assert "local.carrier" in result.output
    assert "local.distance" not in result.output


def test_explore_missing_file(runner, tmp_path: Path):
    result = runner.invoke(cli, ["explore", str(tmp_path / "nope.preql")])
    assert result.exit_code == 2


def test_explore_parse_error_reports_cleanly(runner, tmp_path: Path):
    bad = tmp_path / "bad.preql"
    bad.write_text("this is not trilogy")
    result = runner.invoke(cli, ["explore", str(bad)])
    assert result.exit_code == 1
    assert "Failed to parse" in result.output


def test_normalize_import_strips_suffix():
    assert _normalize_import("flight.preql") == "flight"
    assert _normalize_import("flight") == "flight"


def test_normalize_import_paths_to_dots():
    assert _normalize_import("root/flight.preql") == "root.flight"
    assert _normalize_import("root\\flight.preql") == "root.flight"
    assert _normalize_import("./flight.preql") == "flight"
    assert _normalize_import("./root/flight.preql") == "root.flight"


def test_run_import_prepends_to_inline_query(
    runner, monkeypatch, tmp_path: Path, sample_preql: Path
):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        cli,
        [
            "run",
            "--import",
            "flight.preql",
            "select id, carrier;",
            "duckdb",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert "AA" in result.output or "carrier" in result.output


def test_run_import_rejects_on_file_input(runner, sample_preql: Path):
    result = runner.invoke(
        cli, ["run", "--import", "flight.preql", str(sample_preql), "duckdb"]
    )
    assert result.exit_code == 2
    assert "only applies to inline queries" in result.output
