"""Key-only marker tables: ingest leaves them as islands and gives the marked
table a membership flag (``auto is_<marker> <- key in marker.key``)."""

import tempfile
from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.ingest_helpers.fk_inference import (
    TableFKInfo,
    infer_marker_tables,
)
from trilogy.scripts.ingest_helpers.formatting import canonicalize_names
from trilogy.scripts.ingest_helpers.introspection import IntrospectionLevel
from trilogy.scripts.trilogy import cli


def _info(name: str, columns: list[str], key: str) -> TableFKInfo:
    return TableFKInfo(
        name=name,
        sql_relation=f'"{name}"',
        raw_columns=columns,
        raw_to_canonical=canonicalize_names(columns),
        key_raw_columns=[key],
        unique_verdicts={key: True},
    )


def test_fast_links_key_only_table_to_the_table_owning_its_key():
    tables = [
        _info(
            "Policy_Amount",
            ["Policy_Amount_Identifier", "Policy_Amount"],
            "Policy_Amount_Identifier",
        ),
        _info("Premium", ["Policy_Amount_Identifier"], "Policy_Amount_Identifier"),
        _info("Policy", ["Policy_Identifier", "Policy_Number"], "Policy_Identifier"),
    ]
    links = infer_marker_tables(tables, None, IntrospectionLevel.FAST)
    assert [(m.marker, m.parent, m.parent_column) for m in links] == [
        ("Premium", "Policy_Amount", "Policy_Amount_Identifier")
    ]


def test_tables_with_properties_are_never_markers():
    tables = [
        _info(
            "Policy_Amount",
            ["Policy_Amount_Identifier", "Policy_Amount"],
            "Policy_Amount_Identifier",
        ),
        _info(
            "Premium", ["Policy_Amount_Identifier", "Note"], "Policy_Amount_Identifier"
        ),
    ]
    assert infer_marker_tables(tables, None, IntrospectionLevel.FAST) == []


def test_two_key_only_tables_do_not_mark_each_other():
    tables = [
        _info("Premium", ["Policy_Amount_Identifier"], "Policy_Amount_Identifier"),
        _info("Discount", ["Policy_Amount_Identifier"], "Policy_Amount_Identifier"),
    ]
    assert infer_marker_tables(tables, None, IntrospectionLevel.FAST) == []


def _marker_config(tmppath: Path) -> Path:
    setup_sql = tmppath / "setup.sql"
    setup_sql.write_text(
        "CREATE TABLE amounts (amount_id INTEGER PRIMARY KEY, amount DOUBLE);\n"
        "CREATE TABLE premiums (amount_id INTEGER PRIMARY KEY);\n"
        "CREATE TABLE audited (amount_id INTEGER PRIMARY KEY);\n"
        "INSERT INTO amounts VALUES (1,10.0),(2,20.0),(3,30.0),(4,40.0);\n"
        "INSERT INTO premiums VALUES (2),(4);\n"
        "INSERT INTO audited VALUES (1),(2),(3),(4);"
    )
    config_file = tmppath / "trilogy.toml"
    config_file.write_text(
        '[engine]\ndialect = "duckdb"\n\n'
        f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
    )
    return config_file


def test_ingest_flags_marked_rows_and_skips_a_full_cover_twin():
    """`premiums` marks half of `amounts` and becomes `amounts.is_premiums`;
    `audited` covers every amount, so a flag would always be true and it is
    left alone. The marker file itself stays a key-only island."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = _marker_config(tmppath)
        out_dir = tmppath / "raw"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "amounts,premiums,audited",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        amounts = (out_dir / "amounts.preql").read_text()
        assert "import premiums as premiums;" in amounts
        assert "auto is_premiums <- amount_id in premiums.amount_id;" in amounts
        assert (
            "# Flags: is_premiums (a premiums row exists for this amount_id)."
            in amounts
        )
        assert "audited" not in amounts
        premiums = (out_dir / "premiums.preql").read_text()
        assert (
            "A row marks a amounts row; query through amounts.is_premiums." in premiums
        )
        assert "import" not in premiums

        query = tmppath / "query.preql"
        query.write_text(
            "import raw.amounts as a;\n\n"
            "select sum(a.amount ? a.is_premiums) as premium_total,"
            " count(a.amount_id) as rows_seen;\n"
        )
        result = runner.invoke(
            cli, ["run", str(query), "duckdb", "--config", str(config_file)]
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "60.0" in result.output
        assert "4" in result.output
