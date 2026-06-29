"""Tests for `trilogy explore` and `trilogy run --import`."""

import json as json_module
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
    # --include-builtins surfaces internal concepts in the flat concepts table…
    result = runner.invoke(
        cli, ["explore", str(sample_preql), "--include-builtins", "--show", "concepts"]
    )
    assert result.exit_code == 0
    assert "__preql_internal" in result.output


def test_explore_groups_never_expose_internal_namespace(runner, sample_preql: Path):
    # …but the grouped/imported summary (the default view the agent reads) never
    # lists the internal `__preql_internal` model, even with --include-builtins.
    result = runner.invoke(cli, ["explore", str(sample_preql), "--include-builtins"])
    assert result.exit_code == 0
    assert "__preql_internal" not in result.output


def test_explore_show_concepts_only(runner, sample_preql: Path):
    result = runner.invoke(cli, ["explore", str(sample_preql), "--show", "concepts"])
    assert result.exit_code == 0
    assert "Concepts" in result.output
    assert "Datasources" not in result.output
    assert "Imports" not in result.output


@pytest.fixture
def annotated_import_preql(tmp_path: Path) -> Path:
    (tmp_path / "dem.preql").write_text(dedent("""
            key cd_id int;
            property cd_id.edu string;
            datasource cd (cd_id, edu) grain(cd_id)
            query '''select 1 as cd_id, 'College' as edu''';
            """).strip() + "\n")
    parent = tmp_path / "sales.preql"
    parent.write_text("import dem as dem; # demographics recorded at point of sale\n")
    return parent


def test_explore_surfaces_import_description_in_groups(runner, annotated_import_preql):
    result = runner.invoke(cli, ["explore", str(annotated_import_preql)])
    assert result.exit_code == 0, result.output
    assert "demographics recorded at point of sale" in result.output


def test_explore_surfaces_import_description_in_imports_table(
    runner, annotated_import_preql
):
    result = runner.invoke(
        cli, ["explore", str(annotated_import_preql), "--show", "imports"]
    )
    assert result.exit_code == 0, result.output
    assert "demographics recorded at point of sale" in result.output


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


def test_explore_regex_filter_substring(runner, sample_preql: Path):
    """A bare word still works as a substring match under the new ``--regex``
    flag — same observable behaviour as the old substring-only ``--grep``."""
    result = runner.invoke(cli, ["explore", str(sample_preql), "--regex", "carrier"])
    assert result.exit_code == 0
    assert "carrier" in result.output
    assert "distance" not in result.output


def test_explore_regex_filter_accepts_multiple(runner, sample_preql: Path):
    """Multiple ``--regex`` patterns OR together — a concept is kept when ANY
    pattern matches."""
    result = runner.invoke(
        cli,
        [
            "explore",
            str(sample_preql),
            "--regex",
            "carrier",
            "--regex",
            "distance",
        ],
    )
    assert result.exit_code == 0
    assert "carrier" in result.output
    assert "distance" in result.output
    # `id` is neither — should be filtered out, even though it's a key concept.
    # In the flat layout, a key surfaces as an indented detail line `    KEY`.
    assert "    KEY\n" not in result.output


def test_explore_regex_filter_supports_metacharacters(runner, tmp_path: Path):
    """The whole point of the rename: regex syntax actually works. The agent's
    repeated failure mode on ``date\\.(year|week_seq)`` becomes a real match."""
    p = tmp_path / "x.preql"
    p.write_text(
        "key id int;\n" "import x as sold_date;\n" "property id.weight float;\n",
        encoding="utf-8",
    )
    # Just verify the regex compiles and runs without 67-char-empty-result.
    # The exact concept addresses in `x.preql` depend on parsing, so look
    # for the presence of an `Available Concepts` header rather than a
    # specific concept name.
    result = runner.invoke(cli, ["explore", str(p), "--regex", r"id|sold_date"])
    assert result.exit_code == 0, result.output
    assert "Available Concepts" in result.output


def test_explore_regex_filter_rejects_invalid_pattern(runner, sample_preql: Path):
    """A malformed pattern aborts with exit 2 and a readable ``re.error`` —
    not a silent empty-result that the agent would interpret as 'no matches'."""
    result = runner.invoke(
        cli, ["explore", str(sample_preql), "--regex", "[unbalanced"]
    )
    assert result.exit_code == 2
    assert "Invalid --regex" in result.output


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


def test_format_import_accepts_dotted_form():
    """Dotted form matches the in-file `import a.b as c;` syntax exactly. An
    agent that learns the CLI form should be able to copy it verbatim into a
    `.preql` file without rewriting separators — this is the whole point of
    aligning the two surfaces."""
    assert _format_import("raw.item") == "import raw.item;\n"
    assert _format_import("raw.item:item") == "import raw.item as item;\n"
    assert _format_import("raw.unified_sales:s") == "import raw.unified_sales as s;\n"


def test_normalize_import_dotted_form_is_idempotent():
    """Dotted input must round-trip unchanged so a single dotted form is the
    canonical CLI spelling."""
    assert _normalize_import("flight") == "flight"
    assert _normalize_import("raw.item") == "raw.item"
    assert _normalize_import("raw.unified_sales") == "raw.unified_sales"


def test_run_import_dotted_form_works_end_to_end(
    runner, monkeypatch, tmp_path: Path, sample_preql: Path
):
    """End-to-end check that the dotted `--import` form (matching in-file
    syntax) actually runs — guards against a regression where the path-style
    parsing path silently rejected pure dotted names."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        cli,
        ["run", "--import", "flight:flight", "select flight.id;", "duckdb"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


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
    """A metric concept shows up as a normal concept line with the role
    ``METRIC`` on the indented detail line — distinct from KEY/PROP."""
    path = tmp_path / "sales.preql"
    path.write_text(
        "key id int;\n" "property id.amount int;\n" "auto total <- sum(amount);\n",
        encoding="utf-8",
    )
    result = runner.invoke(cli, ["explore", str(path)])
    assert result.exit_code == 0, result.output
    assert "total : " in result.output
    assert "    METRIC" in result.output


@pytest.fixture
def conformed_preql(tmp_path: Path) -> Path:
    """A fact that imports one date dimension under two roles — the minimal
    conformed-dimension shape the v2 dedup targets."""
    (tmp_path / "d.preql").write_text(
        dedent("""
            key id int; # a calendar date
            property id.year int; # four-digit year
            datasource d (id, year) grain(id)
            query '''select 1 as id, 2001 as year''';
            """).strip() + "\n",
        encoding="utf-8",
    )
    fact = tmp_path / "f.preql"
    fact.write_text(
        "import d as sold_date;\nimport d as ship_date;\nkey fid int;\n",
        encoding="utf-8",
    )
    return fact


def _concepts_payload(env, expand_roles=False, version=None):
    from trilogy.scripts.explore import build_concepts_payload

    items = [
        (k, v)
        for k, v in env.concepts.items()
        if not k.startswith("__") and not k.startswith("local._env_")
    ]
    return build_concepts_payload(
        env, items, expand_imports=True, expand_roles=expand_roles, version=version
    )


def test_explore_v2_collapses_conformed_roles(conformed_preql: Path):
    from trilogy.scripts.explore import _load_environment

    payload = _concepts_payload(_load_environment(conformed_preql), version=2)
    assert payload["version"] == 2
    # The two date roles share one combined-key entry, not two full schemas.
    combined = [k for k in payload["namespaces"] if "," in k]
    assert combined == ["sold_date, ship_date"] or combined == ["ship_date, sold_date"]
    assert "same_as" not in json_module.dumps(payload)


def test_explore_v1_renders_every_role_in_full(conformed_preql: Path):
    from trilogy.scripts.explore import _load_environment

    payload = _concepts_payload(_load_environment(conformed_preql), version=1)
    assert payload["version"] == 1
    assert "sold_date" in payload["namespaces"]
    assert "ship_date" in payload["namespaces"]
    assert not any("," in k for k in payload["namespaces"])


def test_explore_expand_roles_forces_full_at_v2(conformed_preql: Path):
    from trilogy.scripts.explore import _load_environment

    payload = _concepts_payload(
        _load_environment(conformed_preql), expand_roles=True, version=2
    )
    assert payload["version"] == 2  # version unchanged…
    assert "sold_date" in payload["namespaces"]  # …but roles not collapsed
    assert "ship_date" in payload["namespaces"]


def test_explore_default_json_version_is_2(conformed_preql: Path):
    from trilogy.scripts.explore import _load_environment, render_version

    assert render_version("json") == 2
    payload = _concepts_payload(_load_environment(conformed_preql))
    assert payload["version"] == 2


def test_render_version_override_restores(conformed_preql: Path):
    from trilogy.scripts.explore import render_version, render_version_override

    assert render_version("json") == 2
    with render_version_override("json", 1):
        assert render_version("json") == 1
    assert render_version("json") == 2


def test_explore_show_imports_lists_alias_and_path(runner, tmp_path: Path):
    leaf = tmp_path / "leaf.preql"
    leaf.write_text("key x int;", encoding="utf-8")
    main = tmp_path / "main.preql"
    main.write_text("import leaf as leaf;\nkey y int;\n", encoding="utf-8")
    result = runner.invoke(cli, ["explore", str(main), "--show", "imports"])
    assert result.exit_code == 0, result.output
    assert "Imports" in result.output
    assert "leaf" in result.output
