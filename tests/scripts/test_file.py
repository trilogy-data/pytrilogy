"""Tests for the `trilogy file` command group."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.scripts.file_helpers import (
    FileNotFoundError,
    FileOperationError,
    LocalFileBackend,
    get_backend,
    register_backend,
)
from trilogy.scripts.trilogy import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def _reset_output_format():
    """`--format json` flips the process-global output format; restore the
    default afterwards so JSON mode doesn't leak into the rich-output tests."""
    from trilogy.scripts import display_core

    yield
    display_core.set_output_format("rich")


def test_list_empty_directory(runner, tmp_path: Path):
    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "No entries" in result.output


def test_list_with_entries_filters_to_preql_by_default(runner, tmp_path: Path):
    """The Trilogy-focused default keeps the listing to ``.preql`` files plus
    directories — the noise of ``__pycache__``, ``.sql`` exports, build
    artefacts is filtered out so the model surface is the first thing the
    reader sees."""
    (tmp_path / "a.preql").write_text("import x;\n")
    (tmp_path / "b.sql").write_text("select 1;\n")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.preql").write_text("import y;\n")

    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "a.preql" in result.output
    assert "b.sql" not in result.output
    assert "sub/" in result.output or "sub\\" in result.output


def test_list_all_flag_includes_non_preql(runner, tmp_path: Path):
    (tmp_path / "a.preql").write_text("import x;\n")
    (tmp_path / "b.sql").write_text("select 1;\n")
    result = runner.invoke(cli, ["file", "list", str(tmp_path), "--all"])
    assert result.exit_code == 0, result.output
    assert "a.preql" in result.output
    assert "b.sql" in result.output


def test_list_renders_preql_description(runner, tmp_path: Path):
    """Leading ``#`` block on a .preql file surfaces beneath the path as a
    description line — same format the agent's ``list_files`` tool uses."""
    (tmp_path / "fact.preql").write_text(
        "# Unified sales fact across all channels.\n"
        "# Use this for cross-channel rollups instead of importing per-channel facts.\n"
        "key id int;\n",
        encoding="utf-8",
    )
    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "fact.preql" in result.output
    assert "↳" in result.output
    assert "Unified sales fact across all channels." in result.output
    assert "cross-channel rollups" in result.output


def test_list_no_description_block_emits_path_only(runner, tmp_path: Path):
    (tmp_path / "bare.preql").write_text("key id int;\n", encoding="utf-8")
    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "bare.preql" in result.output
    assert "↳" not in result.output


def test_list_json_surfaces_preql_description(runner, tmp_path: Path):
    """The agent reads `file list` as JSON, so each .preql entry must carry its
    leading-comment description there too — this is what replaces the standalone
    `list_files` tool the Trilogy toolset used to expose."""
    import json

    (tmp_path / "fact.preql").write_text(
        "# Unified sales fact across all channels.\nkey id int;\n", encoding="utf-8"
    )
    result = runner.invoke(cli, ["--format", "json", "file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    event = json.loads(result.output)
    entry = next(e for e in event["entries"] if e["path"].endswith("fact.preql"))
    assert entry["description"] == "Unified sales fact across all channels."


def test_list_recursive(runner, tmp_path: Path):
    (tmp_path / "top.preql").write_text("x")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "deep.preql").write_text("y")

    result = runner.invoke(cli, ["file", "list", str(tmp_path), "--recursive"])
    assert result.exit_code == 0
    assert "top.preql" in result.output
    assert "deep.preql" in result.output


def test_list_long_format(runner, tmp_path: Path):
    target = tmp_path / "a.preql"
    target.write_text("hello")

    result = runner.invoke(cli, ["file", "list", str(tmp_path), "--long"])
    assert result.exit_code == 0
    assert "FILE" in result.output


def test_list_missing_path(runner, tmp_path: Path):
    missing = tmp_path / "does_not_exist"
    result = runner.invoke(cli, ["file", "list", str(missing)])
    assert result.exit_code == 1
    assert "No such path" in result.output


def test_read_file(runner, tmp_path: Path):
    target = tmp_path / "a.preql"
    target.write_text("hello world")

    result = runner.invoke(cli, ["file", "read", str(target)])
    assert result.exit_code == 0
    assert "hello world" in result.output


def test_read_missing_file(runner, tmp_path: Path):
    result = runner.invoke(cli, ["file", "read", str(tmp_path / "missing.preql")])
    assert result.exit_code == 1
    assert "No such file" in result.output


def test_write_inline_content(runner, tmp_path: Path):
    target = tmp_path / "out.txt"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--content", "select 1;"]
    )
    assert result.exit_code == 0, result.output
    assert target.read_text() == "select 1;"
    assert "Wrote" in result.output


def test_write_from_stdin(runner, tmp_path: Path):
    target = tmp_path / "out.txt"
    result = runner.invoke(cli, ["file", "write", str(target)], input="import foo;\n")
    assert result.exit_code == 0, result.output
    assert target.read_text() == "import foo;\n"


def test_write_from_file(runner, tmp_path: Path):
    src = tmp_path / "src.txt"
    src.write_text("payload")
    dst = tmp_path / "dst.txt"

    result = runner.invoke(cli, ["file", "write", str(dst), "--from-file", str(src)])
    assert result.exit_code == 0, result.output
    assert dst.read_text() == "payload"


def test_write_creates_parent_directories(runner, tmp_path: Path):
    target = tmp_path / "nested" / "deep" / "out.txt"
    result = runner.invoke(cli, ["file", "write", str(target), "-c", "ok"])
    assert result.exit_code == 0, result.output
    assert target.read_text() == "ok"


def test_write_overwrites(runner, tmp_path: Path):
    target = tmp_path / "out.txt"
    target.write_text("old")
    result = runner.invoke(cli, ["file", "write", str(target), "-c", "new"])
    assert result.exit_code == 0
    assert target.read_text() == "new"


def test_write_no_create_fails_when_missing(runner, tmp_path: Path):
    target = tmp_path / "out.preql"
    result = runner.invoke(
        cli, ["file", "write", str(target), "-c", "hi", "--no-create"]
    )
    assert result.exit_code == 1
    assert "Refusing to create" in result.output
    assert not target.exists()


def test_write_escapes_decodes_newlines(runner, tmp_path: Path):
    target = tmp_path / "out.preql"
    result = runner.invoke(
        cli,
        [
            "file",
            "write",
            str(target),
            "--content",
            "import flight;\\nauto x <- count(id);\\n",
            "--escapes",
        ],
    )
    assert result.exit_code == 0, result.output
    assert target.read_text() == "import flight;\nauto x <- count(id);\n"


def test_write_escapes_requires_content(runner, tmp_path: Path):
    target = tmp_path / "out.preql"
    result = runner.invoke(cli, ["file", "write", str(target), "--escapes"], input="hi")
    assert result.exit_code == 2


def test_write_escapes_off_keeps_literal(runner, tmp_path: Path):
    target = tmp_path / "out.txt"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--content", "line1\\nline2"]
    )
    assert result.exit_code == 0
    assert target.read_text() == "line1\\nline2"


def test_write_rejects_both_sources(runner, tmp_path: Path):
    src = tmp_path / "src.preql"
    src.write_text("x")
    target = tmp_path / "dst.preql"
    result = runner.invoke(
        cli,
        [
            "file",
            "write",
            str(target),
            "-c",
            "inline",
            "--from-file",
            str(src),
        ],
    )
    assert result.exit_code == 2


def test_write_from_url_file_scheme(runner, tmp_path: Path):
    src = tmp_path / "snippet.preql"
    src.write_text("import flight;\nauto x <- count(id);\n", encoding="utf-8")
    target = tmp_path / "dst.preql"
    url = src.resolve().as_uri()
    result = runner.invoke(cli, ["file", "write", str(target), "--from-url", url])
    assert result.exit_code == 0, result.output
    assert target.read_text() == "import flight;\nauto x <- count(id);\n"


def test_write_from_url_rejects_unknown_scheme(runner, tmp_path: Path):
    target = tmp_path / "dst.preql"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--from-url", "ftp://example.com/x"]
    )
    assert result.exit_code == 2
    assert "Unsupported" in result.output


def test_write_from_url_missing(runner, tmp_path: Path):
    target = tmp_path / "dst.preql"
    url = (tmp_path / "does_not_exist.preql").resolve().as_uri()
    result = runner.invoke(cli, ["file", "write", str(target), "--from-url", url])
    assert result.exit_code == 2
    assert "Failed to fetch" in result.output


def test_write_rejects_url_plus_content(runner, tmp_path: Path):
    target = tmp_path / "dst.preql"
    result = runner.invoke(
        cli,
        [
            "file",
            "write",
            str(target),
            "--content",
            "x",
            "--from-url",
            "file:///tmp/x",
        ],
    )
    assert result.exit_code == 2


def test_delete_file(runner, tmp_path: Path):
    target = tmp_path / "a.preql"
    target.write_text("x")
    result = runner.invoke(cli, ["file", "delete", str(target)])
    assert result.exit_code == 0
    assert not target.exists()


def test_delete_missing_without_force(runner, tmp_path: Path):
    result = runner.invoke(cli, ["file", "delete", str(tmp_path / "missing")])
    assert result.exit_code == 1


def test_delete_missing_with_force(runner, tmp_path: Path):
    result = runner.invoke(
        cli, ["file", "delete", str(tmp_path / "missing"), "--force"]
    )
    assert result.exit_code == 0


def test_delete_dir_requires_recursive(runner, tmp_path: Path):
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "x.preql").write_text("x")
    result = runner.invoke(cli, ["file", "delete", str(sub)])
    assert result.exit_code == 2
    assert sub.exists()

    result = runner.invoke(cli, ["file", "delete", str(sub), "--recursive"])
    assert result.exit_code == 0
    assert not sub.exists()


def test_move_file(runner, tmp_path: Path):
    src = tmp_path / "a.preql"
    src.write_text("payload")
    dst = tmp_path / "b.preql"
    result = runner.invoke(cli, ["file", "move", str(src), str(dst)])
    assert result.exit_code == 0
    assert not src.exists()
    assert dst.read_text() == "payload"


def test_move_creates_parent_dirs(runner, tmp_path: Path):
    src = tmp_path / "a.preql"
    src.write_text("payload")
    dst = tmp_path / "nested" / "deep" / "b.preql"
    result = runner.invoke(cli, ["file", "move", str(src), str(dst)])
    assert result.exit_code == 0
    assert dst.read_text() == "payload"


def test_move_missing_source(runner, tmp_path: Path):
    result = runner.invoke(
        cli, ["file", "move", str(tmp_path / "missing"), str(tmp_path / "dst")]
    )
    assert result.exit_code == 1


def test_exists_true(runner, tmp_path: Path):
    target = tmp_path / "a.preql"
    target.write_text("x")
    result = runner.invoke(cli, ["file", "exists", str(target)])
    assert result.exit_code == 0
    assert "true" in result.output


def test_exists_false(runner, tmp_path: Path):
    result = runner.invoke(cli, ["file", "exists", str(tmp_path / "missing")])
    assert result.exit_code == 1
    assert "false" in result.output


def test_round_trip_quickstart_example(runner, tmp_path: Path):
    """Mirrors the README quickstart: write a derived datasource script.

    Uses ``--force`` because the body is a README snippet (no resolvable
    ``flight`` import here) — the test is about CRUD round-trip, not parse.
    """
    target = tmp_path / "reporting.preql"
    body = (
        "import flight;\n\n"
        "auto flight_date <- flight.dep_time::date;\n"
        "auto flight_count <- count(id);\n\n"
        "datasource daily_airplane_usage (\n"
        "    flight_date,\n"
        "    flight.model.name,\n"
        "    flight_count\n"
        ")\n"
        "grain(flight_date, flight.model.name)\n"
        "file './daily_airplane_usage.parquet'\n"
        ";\n"
    )
    result = runner.invoke(cli, ["file", "write", str(target), "--force"], input=body)
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, ["file", "read", str(target)])
    assert result.exit_code == 0
    assert "daily_airplane_usage" in result.output
    assert "import flight" in result.output


def test_write_preql_rejects_truncated_content(runner, tmp_path: Path):
    """A .preql write with broken syntax fails before bytes hit disk."""
    target = tmp_path / "query.preql"
    result = runner.invoke(
        cli,
        ["file", "write", str(target), "--content", "auto orders_per_customer"],
    )
    assert result.exit_code == 1, result.output
    assert "not syntactically valid Trilogy" in result.output
    assert not target.exists()


def test_write_preql_rejects_html_escapes(runner, tmp_path: Path):
    """HTML-escaped operators are flagged with a pointed message."""
    target = tmp_path / "query.preql"
    result = runner.invoke(
        cli,
        ["file", "write", str(target), "--content", "select 1 ? x &lt; 2;"],
    )
    assert result.exit_code == 1
    assert "HTML-escaped characters" in result.output
    assert not target.exists()


def test_write_preql_force_bypasses_validation(runner, tmp_path: Path):
    """--force lets a bad .preql body land — the agent bypass loophole, made
    explicit and opt-in rather than implicit through `trilogy file write`."""
    target = tmp_path / "query.preql"
    result = runner.invoke(
        cli,
        [
            "file",
            "write",
            str(target),
            "--content",
            "auto orders_per_customer",
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output
    assert target.read_text() == "auto orders_per_customer"


def test_validate_preql_syntax_wraps_unexpected_exception(monkeypatch):
    """A non-InvalidSyntaxException from the parser is wrapped as a typed
    string so a buggy parser never crashes the write surface."""
    import trilogy.parsing.parse_engine_v2 as parse_engine
    from trilogy.scripts.file_helpers import preql_validation

    def boom(_):
        raise RuntimeError("parser exploded")

    # Validation runs through the configured backend via parse_syntax (pest by
    # default); a bug there must be wrapped, not propagated.
    monkeypatch.setattr(parse_engine, "parse_syntax", boom)
    msg = preql_validation.validate_preql_syntax("select 1 -> x;")
    assert msg is not None
    assert "RuntimeError" in msg
    assert "parser exploded" in msg


def test_write_non_preql_skips_validation(runner, tmp_path: Path):
    """Non-.preql files don't get the Trilogy parser run against them."""
    target = tmp_path / "notes.md"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--content", "auto x not closed"]
    )
    assert result.exit_code == 0, result.output
    assert target.read_text() == "auto x not closed"


def _duckdb_dir(tmp_path: Path) -> Path:
    """A directory with a minimal duckdb ``trilogy.toml`` so ``--show-sql``
    (and ``_last_statement_sql``) have an engine to compile against."""
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duckdb"\n')
    return tmp_path


def test_write_show_sql_emits_generated_sql(runner, tmp_path: Path):
    """``--show-sql`` compiles the last statement and prints its SQL alongside
    the write confirmation so an agent can inspect the codegen before running."""
    target = _duckdb_dir(tmp_path) / "q.preql"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--content", "select 1 -> x;", "--show-sql"]
    )
    assert result.exit_code == 0, result.output
    assert "Wrote" in result.output
    assert "Generated SQL" in result.output
    assert 'as "x"' in result.output


def test_write_show_sql_json_carries_last_statement_sql(runner, tmp_path: Path):
    import json

    target = _duckdb_dir(tmp_path) / "q.preql"
    result = runner.invoke(
        cli,
        [
            "--format",
            "json",
            "file",
            "write",
            str(target),
            "--content",
            "select 1 -> x;",
            "--show-sql",
        ],
    )
    assert result.exit_code == 0, result.output
    event = json.loads(result.output)
    assert event["event"] == "write"
    assert 'as "x"' in event["last_statement_sql"]


def test_write_without_show_sql_skips_compile(runner, tmp_path: Path):
    """No ``--show-sql`` means no compile step, so no generated SQL surfaces."""
    target = _duckdb_dir(tmp_path) / "q.preql"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--content", "select 1 -> x;"]
    )
    assert result.exit_code == 0, result.output
    assert "Generated SQL" not in result.output


def test_last_statement_sql_none_without_engine(tmp_path: Path):
    from trilogy.scripts.file import _last_statement_sql

    target = tmp_path / "q.preql"
    target.write_text("select 1 -> x;\n")
    assert _last_statement_sql(str(target)) is None


def test_last_statement_sql_none_for_definitions_only(tmp_path: Path):
    from trilogy.scripts.file import _last_statement_sql

    target = _duckdb_dir(tmp_path) / "defs.preql"
    target.write_text("key id int;\n")
    assert _last_statement_sql(str(target)) is None


def test_last_statement_sql_none_when_unresolvable(runner, tmp_path: Path):
    """A query that can't resolve (undefined concept) is an authoring mistake,
    not a codegen fault: the write still succeeds and no SQL is emitted."""
    from trilogy.scripts.file import _last_statement_sql

    target = _duckdb_dir(tmp_path) / "q.preql"
    target.write_text("select undefined_thing -> x;\n")
    assert _last_statement_sql(str(target)) is None

    result = runner.invoke(
        cli,
        [
            "file",
            "write",
            str(target),
            "--content",
            "select undefined_thing -> x;",
            "--show-sql",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Wrote" in result.output
    assert "Generated SQL" not in result.output


def test_last_statement_sql_raises_on_render_error(tmp_path: Path, monkeypatch):
    """A statement that resolves but fails to *render* is a codegen bug — the
    error propagates instead of being swallowed into a silent None."""
    import trilogy.executor as executor_module
    from trilogy.scripts.file import _last_statement_sql

    def boom(self, command):
        raise RuntimeError("codegen exploded")

    monkeypatch.setattr(executor_module.Executor, "generate_sql", boom)
    target = _duckdb_dir(tmp_path) / "q.preql"
    target.write_text("select 1 -> x;\n")
    with pytest.raises(RuntimeError, match="codegen exploded"):
        _last_statement_sql(str(target))


def test_write_show_sql_render_error_confirms_write_then_surfaces(
    runner, tmp_path: Path, monkeypatch
):
    """When ``--show-sql`` compilation raises, the bytes still landed: confirm
    the write, then surface the codegen error with a non-zero exit."""
    from trilogy.scripts import file as file_module

    def boom(_path):
        raise RuntimeError("codegen exploded")

    monkeypatch.setattr(file_module, "_last_statement_sql", boom)
    target = tmp_path / "q.preql"
    result = runner.invoke(
        cli, ["file", "write", str(target), "--content", "select 1 -> x;", "--show-sql"]
    )
    assert result.exit_code == 1, result.output
    assert "Wrote" in result.output
    assert "codegen exploded" in result.output
    assert target.read_text() == "select 1 -> x;"


def test_get_backend_rejects_unknown_scheme():
    with pytest.raises(FileOperationError):
        get_backend("s3://bucket/key")


def test_register_backend_extension_point():
    class DummyBackend(LocalFileBackend):
        scheme = "dummy"

    register_backend("dummy", lambda: DummyBackend())
    backend = get_backend("dummy://some/path")
    assert isinstance(backend, DummyBackend)


def test_local_backend_direct_read_missing(tmp_path: Path):
    backend = LocalFileBackend()
    with pytest.raises(FileNotFoundError):
        backend.read(str(tmp_path / "missing.preql"))


def test_local_backend_list_caps_entries(tmp_path: Path):
    for i in range(10):
        (tmp_path / f"f{i:02d}.preql").write_text("x")
    backend = LocalFileBackend()
    entries = backend.list(str(tmp_path), max_entries=5)
    assert len(entries) == 5
    assert [e.path for e in entries] == sorted(e.path for e in entries)


def test_list_cap_emits_truncation_notice(runner, tmp_path: Path, monkeypatch):
    monkeypatch.setattr("trilogy.scripts.file.LIST_MAX_ENTRIES", 3)
    for i in range(6):
        (tmp_path / f"f{i}.preql").write_text("x")
    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "capped at 3 entries" in result.output
