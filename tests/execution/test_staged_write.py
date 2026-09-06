from pathlib import Path

import pytest

from trilogy.execution.staged_write import (
    STAGING_DIR,
    is_remote_target,
    staged_write,
    write_text_staged,
)


def test_success_replaces_target_and_leaves_no_staging(tmp_path: Path):
    target = tmp_path / "out.parquet"
    target.write_bytes(b"old")
    with staged_write(str(target)) as staged:
        assert Path(staged).parent == tmp_path / STAGING_DIR
        assert Path(staged).suffix == ".tmp"
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"
    assert not (tmp_path / STAGING_DIR).exists()


def test_failure_keeps_previous_target_and_removes_staged_file(tmp_path: Path):
    target = tmp_path / "out.parquet"
    target.write_bytes(b"old")
    with pytest.raises(RuntimeError), staged_write(str(target)) as staged:
        Path(staged).write_bytes(b"partial")
        raise RuntimeError("boom")
    assert target.read_bytes() == b"old"
    assert not (tmp_path / STAGING_DIR).exists()


def test_failure_on_fresh_target_writes_nothing(tmp_path: Path):
    target = tmp_path / "out.parquet"
    with pytest.raises(RuntimeError), staged_write(str(target)):
        raise RuntimeError("boom")
    assert list(tmp_path.iterdir()) == []


def test_stale_leftover_for_same_target_is_swept(tmp_path: Path):
    staging = tmp_path / STAGING_DIR
    staging.mkdir()
    stale = staging / "out.parquet.deadbeef.tmp"
    stale.write_bytes(b"truncated")
    duck_stale = staging / "tmp_out.parquet.deadbeef.tmp"
    duck_stale.write_bytes(b"truncated")
    other = staging / "other.parquet.deadbeef.tmp"
    other.write_bytes(b"someone else's")
    suffix_twin = staging / "a_out.parquet.deadbeef.tmp"
    suffix_twin.write_bytes(b"someone else's")
    with staged_write(str(tmp_path / "out.parquet")) as staged:
        assert not stale.exists()
        assert not duck_stale.exists()
        Path(staged).write_bytes(b"new")
    assert other.exists()
    assert suffix_twin.exists()
    assert staging.exists()


def test_remote_target_yields_itself(tmp_path: Path):
    with staged_write("gs://bucket/out.parquet") as staged:
        assert staged == "gs://bucket/out.parquet"
    assert list(tmp_path.iterdir()) == []
    assert is_remote_target("s3://bucket/x")
    assert not is_remote_target("C:/data/out.parquet")


def test_missing_parent_directory_is_reported(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="does not exist"), staged_write(
        str(tmp_path / "missing" / "out.parquet")
    ):
        pass


def test_write_text_staged_round_trips_utf8(tmp_path: Path):
    target = tmp_path / "state.json"
    write_text_staged(target, "caf\u00e9")
    assert target.read_text(encoding="utf-8") == "caf\u00e9"
    assert not (tmp_path / STAGING_DIR).exists()


def test_configured_root_on_same_filesystem_is_preferred(tmp_path: Path):
    root = tmp_path / "scratch" / "instance"
    target = tmp_path / "out" / "out.parquet"
    target.parent.mkdir()
    with staged_write(str(target), str(root)) as staged:
        assert Path(staged).parent == root
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"
    assert root.is_dir()
    assert list(root.iterdir()) == []
    assert not (target.parent / STAGING_DIR).exists()


def test_configured_root_leaves_other_targets_scratch_alone(tmp_path: Path):
    root = tmp_path / "scratch"
    root.mkdir()
    twin = root / "out.parquet.deadbeef.tmp"
    twin.write_bytes(b"in flight for a target elsewhere")
    with staged_write(str(tmp_path / "out.parquet"), str(root)) as staged:
        Path(staged).write_bytes(b"new")
    assert twin.exists()


@pytest.mark.parametrize("root", ["gs://bucket/scratch", None])
def test_unusable_root_falls_back_to_sibling(tmp_path: Path, root: str | None):
    target = tmp_path / "out.parquet"
    with staged_write(str(target), root) as staged:
        assert Path(staged).parent == tmp_path / STAGING_DIR
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"


def test_root_on_another_filesystem_falls_back_to_sibling(tmp_path: Path, monkeypatch):
    from trilogy.execution import staged_write as module

    monkeypatch.setattr(module, "_same_filesystem", lambda a, b: False)
    root = tmp_path / "scratch"
    target = tmp_path / "out.parquet"
    with staged_write(str(target), str(root)) as staged:
        assert Path(staged).parent == tmp_path / STAGING_DIR
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"
    assert list(root.iterdir()) == []


def test_claim_retries_when_a_sibling_removes_the_empty_directory(
    tmp_path: Path, monkeypatch
):
    from trilogy.execution import staged_write as module

    original = Path.touch
    calls: list[int] = []

    def flaky_touch(self: Path, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            self.parent.rmdir()
            raise FileNotFoundError(self)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "touch", flaky_touch)
    target = tmp_path / "out.parquet"
    with staged_write(str(target)) as staged:
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"
    assert len(calls) == 2

    monkeypatch.setattr(module, "_CREATE_ATTEMPTS", 1)
    calls.clear()
    with pytest.raises(FileNotFoundError), staged_write(str(target)):
        pass


def test_sweep_tolerates_an_undeletable_leftover(tmp_path: Path, monkeypatch):
    staging = tmp_path / STAGING_DIR
    staging.mkdir()
    stale = staging / "out.parquet.deadbeef.tmp"
    stale.write_bytes(b"held open elsewhere")
    original = Path.unlink

    def refuse(self: Path, *args, **kwargs):
        if self == stale:
            raise PermissionError(self)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", refuse)
    target = tmp_path / "out.parquet"
    with staged_write(str(target)) as staged:
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"
    assert stale.exists()


def test_root_that_cannot_be_created_falls_back_to_sibling(tmp_path: Path):
    from trilogy.execution.staged_write import _same_filesystem

    blocker = tmp_path / "blocker"
    blocker.write_bytes(b"a file where the root should be")
    target = tmp_path / "out.parquet"
    with staged_write(str(target), str(blocker / "scratch")) as staged:
        assert Path(staged).parent == tmp_path / STAGING_DIR
        Path(staged).write_bytes(b"new")
    assert target.read_bytes() == b"new"
    assert not _same_filesystem(tmp_path / "missing", tmp_path)
