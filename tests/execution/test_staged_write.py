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


def test_remote_target_without_credentials_writes_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """No credentials loses the protection, never the write."""
    monkeypatch.delenv("GOOGLE_HMAC_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_HMAC_SECRET", raising=False)
    with staged_write("gs://bucket/out.parquet") as staged:
        assert staged == "gs://bucket/out.parquet"
    assert list(tmp_path.iterdir()) == []
    assert is_remote_target("s3://bucket/x")
    assert not is_remote_target("C:/data/out.parquet")


class FakeRemote:
    """The three operations ``staged_write`` asks an object store for."""

    def __init__(self, existing: tuple[str, ...] = ()):
        self.objects = {name: b"old" for name in existing}
        self.copied: list[tuple[str, str]] = []
        self.deleted: list[str] = []

    def copy_file(self, src: str, dst: str) -> None:
        if src not in self.objects:
            raise FileNotFoundError(src)
        self.objects[dst] = self.objects[src]
        self.copied.append((src, dst))

    def delete_file(self, path: str) -> None:
        self.deleted.append(path)
        self.objects.pop(path, None)

    def get_file_info(self, selector):
        from types import SimpleNamespace

        return [
            SimpleNamespace(path=name)
            for name in sorted(self.objects)
            if name.startswith(selector.base_dir.rstrip("/") + "/")
        ]


@pytest.fixture()
def fake_remote(monkeypatch: pytest.MonkeyPatch):
    def install(remote: FakeRemote) -> FakeRemote:
        monkeypatch.setattr(
            "trilogy.execution.staged_write._remote_filesystem",
            lambda scheme, uri: remote,
        )
        return remote

    return install


def test_remote_success_stages_then_copies_over_the_target(fake_remote):
    remote = fake_remote(FakeRemote(("bucket/trees/out.parquet",)))
    target = "gcs://bucket/trees/out.parquet"
    with staged_write(target) as staged:
        assert staged.startswith(f"gcs://bucket/trees/{STAGING_DIR}/out.parquet.")
        assert staged.endswith(".tmp")
        # The real object is untouched while the write is in flight.
        assert remote.objects["bucket/trees/out.parquet"] == b"old"
        remote.objects[staged.split("://", 1)[1]] = b"new"
    assert remote.objects["bucket/trees/out.parquet"] == b"new"
    assert len(remote.copied) == 1
    # The staged key is always cleaned up, success or not.
    assert remote.deleted and remote.deleted[-1].startswith(
        f"bucket/trees/{STAGING_DIR}/out.parquet."
    )


def test_remote_failure_leaves_the_previous_object_intact(fake_remote):
    """The exact regression: a killed writer must not truncate the target."""
    remote = fake_remote(FakeRemote(("bucket/trees/out.parquet",)))
    with pytest.raises(RuntimeError), staged_write(
        "gcs://bucket/trees/out.parquet"
    ) as staged:
        remote.objects[staged.split("://", 1)[1]] = b"PAR1-partial-no-footer"
        raise RuntimeError("OOM")
    assert remote.objects["bucket/trees/out.parquet"] == b"old"
    assert remote.copied == []
    assert not [k for k in remote.objects if STAGING_DIR in k]


def test_remote_failure_on_a_fresh_target_publishes_nothing(fake_remote):
    remote = fake_remote(FakeRemote())
    with pytest.raises(RuntimeError), staged_write(
        "gcs://bucket/trees/new.parquet"
    ) as staged:
        remote.objects[staged.split("://", 1)[1]] = b"partial"
        raise RuntimeError("boom")
    assert remote.objects == {}


def test_remote_sweeps_what_an_earlier_killed_writer_staged(fake_remote):
    stale = f"bucket/trees/{STAGING_DIR}/out.parquet.deadbeef.tmp"
    sibling = f"bucket/trees/{STAGING_DIR}/other.parquet.cafe1234.tmp"
    remote = fake_remote(FakeRemote((stale, sibling)))
    with staged_write("gcs://bucket/trees/out.parquet") as staged:
        remote.objects[staged.split("://", 1)[1]] = b"new"
    assert stale in remote.deleted
    # Another target's staged object is not ours to collect.
    assert sibling not in remote.deleted


def test_non_object_store_scheme_writes_in_place():
    """``file://`` paths are platform-shaped and never staged remotely."""
    from trilogy.execution.staged_write import _remote_filesystem

    assert _remote_filesystem("file", "file:///tmp/out.parquet") is None


def test_gcs_credentials_build_an_s3_endpoint_filesystem(
    monkeypatch: pytest.MonkeyPatch,
):
    """GCS is finalized with the same HMAC pair DuckDB writes with."""
    from trilogy.execution.staged_write import _remote_filesystem

    monkeypatch.setenv("GOOGLE_HMAC_KEY", "key")
    monkeypatch.setenv("GOOGLE_HMAC_SECRET", "secret")
    filesystem = _remote_filesystem("gcs", "gcs://bucket/out.parquet")
    assert filesystem is not None
    # Not pyarrow's GcsFileSystem, which would authenticate as ADC instead.
    assert type(filesystem).__name__ == "S3FileSystem"
    options = filesystem.__reduce__()[1][0]
    assert options["endpoint_override"] == "storage.googleapis.com"
    assert options["access_key"] == "key"


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
