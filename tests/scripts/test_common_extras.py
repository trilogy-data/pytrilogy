"""Coverage for trilogy.scripts.common helpers not exercised by integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.exceptions import Exit

from trilogy.scripts import common
from trilogy.scripts.common import ExecutionStats


def test_format_stats_persist_singular_plural_label():
    out_single = common.format_stats(ExecutionStats(persist_count=1))
    out_plural = common.format_stats(ExecutionStats(persist_count=2))
    assert "1 table persisted" in out_single
    assert "2 tables persisted" in out_plural


def test_format_stats_combines_multiple_categories():
    stats = ExecutionStats(persist_count=2, update_count=3, validate_count=1)
    out = common.format_stats(stats)
    assert "2 tables persisted" in out
    assert "3 datasources updated" in out
    assert "1 datasource validated" in out


def test_format_stats_respects_stat_types_filter():
    stats = ExecutionStats(persist_count=1, update_count=4)
    out = common.format_stats(stats, stat_types=["update"])
    assert "persisted" not in out
    assert "4 datasources updated" in out


def test_resolve_input_directory_returns_glob(tmp_path):
    (tmp_path / "a.preql").write_text("")
    (tmp_path / "b.preql").write_text("")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "c.preql").write_text("")

    out = common.resolve_input(tmp_path)
    names = sorted(p.name for p in out)
    assert names == ["a.preql", "b.preql", "c.preql"]


def test_resolve_input_single_file_returns_self(tmp_path):
    f = tmp_path / "only.preql"
    f.write_text("")
    assert common.resolve_input(f) == [f]


def test_resolve_input_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        common.resolve_input(tmp_path / "nope.preql")


def test_get_runtime_config_returns_default_when_no_toml(tmp_path):
    """A directory with no trilogy.toml returns the empty RuntimeConfig default."""
    cfg = common.get_runtime_config(tmp_path)
    assert cfg.startup_trilogy == []
    assert cfg.startup_sql == []


def test_get_runtime_config_surfaces_load_failure(tmp_path):
    """Malformed trilogy.toml ⇒ print_error + handle_execution_exception → Exit."""
    bad = tmp_path / "trilogy.toml"
    bad.write_text("not = valid = toml [[")
    with pytest.raises(Exit):
        common.get_runtime_config(tmp_path)


def test_looks_like_path_recognizes_separators_and_extensions():
    from trilogy.scripts.common import _looks_like_path

    assert _looks_like_path("foo/bar.preql") is True
    assert _looks_like_path("foo\\bar.preql") is True
    assert _looks_like_path("q.preql") is True
    assert _looks_like_path("q.sql") is True
    assert _looks_like_path("q.toml") is True
    assert _looks_like_path("select 1 -> x;") is False


def test_emit_progress_label_no_callback_is_silent():
    common._emit_progress_label("anything")


def test_emit_progress_label_swallows_callback_exceptions():
    def boom(label: str) -> None:
        raise RuntimeError("explode")

    token = common._PROGRESS_LABEL_CALLBACK.set(boom)
    try:
        common._emit_progress_label("trigger")
    finally:
        common._PROGRESS_LABEL_CALLBACK.reset(token)


def test_emit_progress_label_invokes_callback():
    received: list[str] = []
    token = common._PROGRESS_LABEL_CALLBACK.set(received.append)
    try:
        common._emit_progress_label("step-1")
    finally:
        common._PROGRESS_LABEL_CALLBACK.reset(token)
    assert received == ["step-1"]


def test_flush_debugging_hooks_writes_each_debugging_hook(monkeypatch, capsys):
    """flush_debugging_hooks calls `.write()` on every DebuggingHook attached to
    the executor and prints a notice for each output file."""
    from trilogy.hooks.query_debugger import DebuggingHook

    class _Stub(DebuggingHook):
        def __init__(self):
            self.calls = 0
            self.output_file: Path | str = "trace.log"

        def write(self):
            self.calls += 1

    hook = _Stub()

    class _NonDebugHook:
        pass

    class _StubExec:
        hooks = [hook, _NonDebugHook()]

    common.flush_debugging_hooks(_StubExec())
    assert hook.calls == 1
    out = capsys.readouterr().out
    assert "trace.log" in out
