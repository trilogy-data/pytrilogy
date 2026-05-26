"""Coverage for display_core helpers not exercised by the integration paths:
``_needs_safe_wrapping`` early-returns, ``_make_safe_stdout`` failure modes,
and the fd-level stderr capture (``_FdStderrCapture``)."""

import io
import os
import sys

import pytest

from trilogy.scripts import display_core

try:
    import rich  # noqa: F401

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


def test_needs_safe_wrapping_empty_encoding_is_false():
    assert display_core._needs_safe_wrapping(None) is False
    assert display_core._needs_safe_wrapping("") is False


def test_needs_safe_wrapping_utf8_variants_are_false():
    assert display_core._needs_safe_wrapping("UTF-8") is False
    assert display_core._needs_safe_wrapping("utf_8") is False
    assert display_core._needs_safe_wrapping("UTF8SIG") is False


def test_needs_safe_wrapping_narrow_codepage_is_true():
    assert display_core._needs_safe_wrapping("cp1252") is True
    assert display_core._needs_safe_wrapping("ascii") is True


def test_make_safe_stdout_returns_none_for_utf8(monkeypatch):
    class _Fake:
        encoding = "utf-8"

    monkeypatch.setattr(sys, "stdout", _Fake())
    assert display_core._make_safe_stdout() is None


def test_make_safe_stdout_returns_none_when_no_buffer(monkeypatch):
    class _Fake:
        encoding = "cp1252"

    monkeypatch.setattr(sys, "stdout", _Fake())
    assert display_core._make_safe_stdout() is None


def test_make_safe_stdout_returns_none_on_wrapping_failure(monkeypatch):
    class _BadBuffer:
        def write(self, *args, **kwargs):
            raise RuntimeError("nope")

    class _Fake:
        encoding = "cp1252"
        buffer = _BadBuffer()

    monkeypatch.setattr(sys, "stdout", _Fake())
    monkeypatch.setattr(
        io,
        "TextIOWrapper",
        lambda *a, **kw: (_ for _ in ()).throw(ValueError("bad wrap")),
    )
    assert display_core._make_safe_stdout() is None


def test_set_mode_disables_state():
    original = display_core.is_rich_available()
    try:
        display_core.set_rich_mode(False)
        assert display_core.is_rich_available() is False
        assert display_core.console is None
        assert display_core.error_console is None
    finally:
        display_core.set_rich_mode(original)


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich library not available")
def test_set_mode_enables_state():
    original = display_core.is_rich_available()
    try:
        display_core.set_rich_mode(False)
        display_core.set_rich_mode(True)
        assert display_core.is_rich_available() is True
        assert display_core.console is not None
    finally:
        display_core.set_rich_mode(original)


def test_fd_stderr_capture_no_op_when_rich_disabled(monkeypatch):
    monkeypatch.setattr(display_core, "RICH_AVAILABLE", False)
    monkeypatch.setattr(display_core, "console", None)
    cap = display_core._FdStderrCapture()
    with cap:
        os.write(2, b"raw stderr\n")
    assert cap._orig_fd is None


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich library not available")
def test_fd_stderr_capture_routes_fd_writes_through_console(monkeypatch):
    captured: list[str] = []

    class _StubConsole:
        def print(self, text, *args, **kwargs):
            captured.append(text)

    monkeypatch.setattr(display_core, "RICH_AVAILABLE", True)
    monkeypatch.setattr(display_core, "console", _StubConsole())

    cap = display_core._FdStderrCapture()
    with cap:
        os.write(2, b"line one\nline two\n")
    assert any("line one" in c for c in captured)
    assert any("line two" in c for c in captured)


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich library not available")
def test_fd_stderr_capture_flushes_trailing_partial_line(monkeypatch):
    captured: list[str] = []

    class _StubConsole:
        def print(self, text, *args, **kwargs):
            captured.append(text)

    monkeypatch.setattr(display_core, "RICH_AVAILABLE", True)
    monkeypatch.setattr(display_core, "console", _StubConsole())

    cap = display_core._FdStderrCapture()
    with cap:
        os.write(2, b"no-newline-tail")
    assert any("no-newline-tail" in c for c in captured)


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich library not available")
def test_fd_stderr_capture_prefixes_context_label(monkeypatch):
    captured: list[str] = []

    class _StubConsole:
        def print(self, text, *args, **kwargs):
            captured.append(text)

    monkeypatch.setattr(display_core, "RICH_AVAILABLE", True)
    monkeypatch.setattr(display_core, "console", _StubConsole())

    cap = display_core._FdStderrCapture(get_context=lambda: "worker-3")
    with cap:
        os.write(2, b"complete-line\n")
        os.write(2, b"trailing-no-nl")
    combined = "\n".join(captured)
    assert "worker-3" in combined
    assert "complete-line" in combined
    assert "trailing-no-nl" in combined
