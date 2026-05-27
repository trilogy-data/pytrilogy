"""Tests for `trilogy explore` and `trilogy run --import`."""

from pathlib import Path
from textwrap import dedent

import pytest
from click.testing import CliRunner

from trilogy.scripts.run import _format_import, _normalize_import
from trilogy.scripts.trilogy import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_preql(tmp_path: Path) -> Path:
    path = tmp_path / "flight.preql"
    path.write_text(dedent("""
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
            """).strip() + "\n")
    return path


def test_explore_lists_concepts(runner, sample_preql: Path):
    # Default output is the namespace-grouped view, which still names every
    # concept by leaf — just collapsed under a header.
    result = runner.invoke(cli, ["explore", str(sample_preql)])
    assert result.exit_code == 0, result.output
    assert "Available Concepts" in result.output
    assert "carrier" in result.output
    assert "distance" in result.output


def test_explore_show_all_includes_datasources_and_imports(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--show", "all"])
    assert result.exit_code == 0
    assert "Available Concepts" in result.output
    assert "Concepts" in result.output
    assert "Datasources" in result.output
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
    # In grouped output, ``id`` shows up under "keys:" — and the only-properties
    # leaves (carrier, distance) are filtered out.
    assert "id" in result.output
    assert "carrier" not in result.output
    assert "distance" not in result.output


def test_explore_purpose_filter_accepts_multiple(runner, sample_preql: Path):
    result = runner.invoke(
        cli,
        [
            "explore",
            str(sample_preql),
            "--purpose",
            "key",
            "--purpose",
            "property",
        ],
    )
    assert result.exit_code == 0
    assert "id" in result.output
    assert "carrier" in result.output
    assert "distance" in result.output


def test_explore_grep_filter(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--grep", "carrier"])
    assert result.exit_code == 0
    assert "carrier" in result.output
    assert "distance" not in result.output


def test_explore_grep_filter_accepts_multiple(runner, sample_preql: Path):
    result = runner.invoke(
        cli,
        [
            "explore",
            str(sample_preql),
            "--grep",
            "carrier",
            "--grep",
            "distance",
        ],
    )
    assert result.exit_code == 0
    assert "carrier" in result.output
    assert "distance" in result.output
    # `id` is neither — should be filtered out, even though it's a key concept.
    assert "keys:" not in result.output


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


def test_format_import_bare():
    assert _format_import("flight") == "import flight;\n"
    assert _format_import("root/flight.preql") == "import root.flight;\n"


def test_format_import_alias():
    assert _format_import("raw/item:item") == "import raw.item as item;\n"
    assert _format_import("root/flight.preql:f") == "import root.flight as f;\n"


def test_run_import_alias_namespaces(
    runner, monkeypatch, tmp_path: Path, sample_preql: Path
):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        cli,
        ["run", "--import", "flight.preql:flight", "select flight.id;", "duckdb"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def test_run_inline_appends_missing_terminator(
    runner, monkeypatch, tmp_path: Path, sample_preql: Path
):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        cli,
        ["run", "--import", "flight.preql:flight", "select flight.id", "duckdb"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def test_compact_datatype_handles_traits_enums_and_lower():
    from trilogy.scripts.explore import _compact_datatype

    assert _compact_datatype("Trait<STRING, ['us_state']>") == "string::us_state"
    assert (
        _compact_datatype("Trait<Trait<STRING, ['us_state']>, ['code']>")
        == "string::us_state::code"
    )
    long_enum = "enum<" + ",".join(f"'value_{i}'" for i in range(12)) + ">"
    assert len(long_enum) > 60
    out = _compact_datatype(long_enum)
    assert out.startswith("enum<'value_0','value_1','value_2',")
    assert "+9" in out
    assert _compact_datatype("INT") == "int"


def test_explore_metrics_appear_in_group_view(runner, tmp_path: Path):
    """Hits the ``metrics:`` branch in `_emit_groups` — a metric concept in a
    group surfaces under ``metrics:`` rather than under keys/props."""
    path = tmp_path / "sales.preql"
    path.write_text(
        "key id int;\n" "property id.amount int;\n" "auto total <- sum(amount);\n",
        encoding="utf-8",
    )
    result = runner.invoke(cli, ["explore", str(path)])
    assert result.exit_code == 0, result.output
    assert "metrics:" in result.output
    assert "total" in result.output


def test_explore_show_imports_lists_alias_and_path(runner, tmp_path: Path):
    leaf = tmp_path / "leaf.preql"
    leaf.write_text("key x int;", encoding="utf-8")
    main = tmp_path / "main.preql"
    main.write_text("import leaf as leaf;\nkey y int;\n", encoding="utf-8")
    result = runner.invoke(cli, ["explore", str(main), "--show", "imports"])
    assert result.exit_code == 0, result.output
    assert "Imports" in result.output
    assert "leaf" in result.output
