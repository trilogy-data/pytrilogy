"""Direct tests for display_ingest helpers (header, progress, summary, _shorten)."""

import importlib.util

import pytest

from trilogy.scripts.display import set_rich_mode
from trilogy.scripts.display_ingest import (
    _PlainIngestProgress,
    _shorten,
    ingest_progress,
    show_ingest_header,
    show_ingest_summary,
)
from trilogy.scripts.ingest import IngestSummaryRow

RICH_AVAILABLE = importlib.util.find_spec("rich") is not None
requires_rich = pytest.mark.skipif(
    not RICH_AVAILABLE, reason="Rich library not available"
)


class TestShorten:
    def test_short_path_unchanged(self):
        assert _shorten("short.csv", max_len=50) == "short.csv"

    def test_long_path_uses_parent_basename(self):
        path = "/a/very/deeply/nested/dir/structure/file.csv"
        out = _shorten(path, max_len=30)
        assert out.startswith(".../")
        assert out.endswith("/file.csv")

    def test_extreme_path_falls_back_to_tail_truncation(self):
        # Even `parent/basename` is too long for the budget → tail truncation.
        path = "/x/" + "z" * 200 + "/" + "y" * 200 + ".csv"
        out = _shorten(path, max_len=20)
        assert len(out) == 20
        assert out.startswith("...")

    def test_path_without_parent_uses_basename(self):
        out = _shorten("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.csv", max_len=10)
        # Basename is too long too → tail truncation.
        assert out.startswith("...")


class TestShowIngestSummary:
    def test_empty_rows_short_circuits(self):
        # No exception, no print — just an early return.
        show_ingest_summary([])

    @requires_rich
    def test_rich_summary_renders(self, capsys):
        with set_rich_mode(True):
            show_ingest_summary(
                [
                    IngestSummaryRow("a.csv", "raw/a.preql", "3", "id", "ok"),
                    IngestSummaryRow("b.csv", "-", "-", "-", "failed: IOError"),
                ]
            )
        out = capsys.readouterr().out
        assert "Ingest Summary" in out
        assert "a.csv" in out
        assert "b.csv" in out

    def test_plain_summary_fallback(self, capsys):
        with set_rich_mode(False):
            show_ingest_summary(
                [IngestSummaryRow("a.csv", "raw/a.preql", "3", "id", "ok")]
            )
        out = capsys.readouterr().out
        assert "a.csv -> raw/a.preql" in out
        assert "3 cols" in out
        assert "grain=id" in out


class TestShowIngestHeader:
    @requires_rich
    def test_rich_header(self, capsys):
        with set_rich_mode(True):
            show_ingest_header(["a.csv"], "raw/", "duck_db", "trilogy.toml")
        out = capsys.readouterr().out
        assert "Trilogy Ingest" in out
        assert "duck_db" in out

    def test_plain_header_no_config(self, capsys):
        with set_rich_mode(False):
            show_ingest_header(["a.csv", "b.csv"], "raw/", "duck_db")
        out = capsys.readouterr().out
        assert "sources=2" in out
        assert "dialect=duck_db" in out
        assert "config=" not in out

    def test_plain_header_with_config(self, capsys):
        with set_rich_mode(False):
            show_ingest_header(["a.csv"], "raw/", "duck_db", "trilogy.toml")
        out = capsys.readouterr().out
        assert "config=trilogy.toml" in out


class TestIngestProgress:
    def test_plain_fallback_logs_each_step(self, capsys):
        with set_rich_mode(False), ingest_progress(total=2) as progress:
            progress.step("a.csv", "introspecting")
            progress.advance()
            progress.step("b.csv", "writing")
            progress.advance()
        out = capsys.readouterr().out
        assert "[1/2] a.csv: introspecting" in out
        assert "[2/2] b.csv: writing" in out

    @requires_rich
    def test_rich_progress_yields_rich_class(self):
        from trilogy.scripts.display_ingest import _RichIngestProgress

        with set_rich_mode(True), ingest_progress(total=1) as progress:
            assert isinstance(progress, _RichIngestProgress)
            progress.step("x.csv", "writing")
            progress.advance()


class TestPlainIngestProgressDirect:
    """Cover the no-Rich progress class directly (Rich may be installed in CI)."""

    def test_step_and_advance(self, capsys):
        p = _PlainIngestProgress(total=3)
        p.step("a", "stage1")
        p.advance()
        p.step("b", "stage2")
        p.advance()
        out = capsys.readouterr().out
        assert "[1/3] a: stage1" in out
        assert "[2/3] b: stage2" in out
