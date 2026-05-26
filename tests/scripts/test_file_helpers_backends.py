"""Coverage for the LocalFileBackend list/exists/move corner cases not hit
through the CLI tests."""

from __future__ import annotations

from pathlib import Path

from trilogy.scripts.file_helpers import LocalFileBackend
from trilogy.scripts.file_helpers.backends import _scheme_for


def test_local_backend_list_returns_single_entry_for_file(tmp_path: Path):
    target = tmp_path / "a.txt"
    target.write_text("hello")
    backend = LocalFileBackend()
    entries = backend.list(str(target))
    assert len(entries) == 1
    assert entries[0].path.endswith("a.txt")
    assert entries[0].is_dir is False
    assert entries[0].size == 5


def test_local_backend_list_dir_skips_unreadable_entry(tmp_path: Path, monkeypatch):
    """An OSError during stat() in the listing loop is swallowed so the rest
    of the directory still enumerates."""
    (tmp_path / "good.txt").write_text("ok")
    (tmp_path / "bad.txt").write_text("nope")

    real_stat = Path.stat

    def maybe_boom(self, *args, **kwargs):
        if self.name == "bad.txt":
            raise OSError("no perms")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", maybe_boom)
    backend = LocalFileBackend()
    entries = backend.list(str(tmp_path), recursive=False)
    names = {Path(e.path).name for e in entries}
    assert "good.txt" in names
    assert "bad.txt" not in names


def test_scheme_for_windows_drive_letter_is_local():
    assert _scheme_for(r"C:\foo\bar") == ""


def test_scheme_for_file_url():
    assert _scheme_for("file:///tmp/x") == "file"


def test_scheme_for_remote_url():
    assert _scheme_for("gs://bucket/key") == "gs"
