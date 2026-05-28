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


def test_list_empty_directory(runner, tmp_path: Path):
    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "No entries" in result.output


def test_list_with_entries(runner, tmp_path: Path):
    (tmp_path / "a.preql").write_text("import x;\n")
    (tmp_path / "b.sql").write_text("select 1;\n")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.preql").write_text("import y;\n")

    result = runner.invoke(cli, ["file", "list", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "a.preql" in result.output
    assert "b.sql" in result.output
    assert "sub/" in result.output or "sub\\" in result.output


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
    import trilogy.parsing.v2.lark_backend as lark_backend
    from trilogy.scripts.file_helpers import preql_validation

    def boom(_):
        raise RuntimeError("parser exploded")

    monkeypatch.setattr(lark_backend, "parse_lark", boom)
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
