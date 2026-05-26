import tempfile
from pathlib import Path

from trilogy.staging import StagingConfig, StagingType


def test_staging_default():
    cfg = StagingConfig()
    assert cfg.staging_type == StagingType.LOCAL
    assert cfg.resolved_root == (
        str(Path(tempfile.gettempdir()).resolve()).replace("\\", "/") + "/"
    )


def test_staging_local_path():
    cfg = StagingConfig(path="/tmp/my-staging")
    assert cfg.staging_type == StagingType.LOCAL
    assert cfg.resolved_root.endswith("/")
    assert "my-staging" in cfg.resolved_root


def test_staging_gcs():
    cfg = StagingConfig(path="gs://bucket/prefix")
    assert cfg.staging_type == StagingType.GCS
    assert cfg.resolved_root == "gs://bucket/prefix/"


def test_staging_s3():
    cfg = StagingConfig(path="s3://bucket/prefix")
    assert cfg.staging_type == StagingType.S3
    assert cfg.resolved_root == "s3://bucket/prefix/"


def test_staging_trailing_slash_normalized():
    cfg = StagingConfig(path="gs://bucket/prefix/")
    assert cfg.resolved_root == "gs://bucket/prefix/"


def test_staging_get_file_path():
    cfg = StagingConfig(path="gs://bucket/staging")
    assert cfg.get_file_path("file.arrow") == "gs://bucket/staging/file.arrow"


def test_staging_get_file_path_local():
    cfg = StagingConfig(path="/tmp/staging")
    result = cfg.get_file_path("file.arrow")
    assert result.endswith("/file.arrow")
    assert "staging" in result


def test_staging_get_file_path_default():
    cfg = StagingConfig()
    result = cfg.get_file_path("file.arrow")
    assert result.endswith("/file.arrow")


def test_register_cleanup_remote_is_noop():
    cfg = StagingConfig(path="s3://bucket/staging")
    cfg.register_cleanup("s3://bucket/staging/sub")
    assert cfg._cleanup_paths == set()


def test_register_cleanup_local_creates_and_tracks(tmp_path):
    import os

    cfg = StagingConfig(path=str(tmp_path))
    target = str(tmp_path / "worker") + "/"
    cfg.register_cleanup(target)
    assert os.path.isdir(target.rstrip("/"))
    assert target.rstrip("/") in cfg._cleanup_paths
    # Second register on same path is a no-op (already tracked).
    cfg.register_cleanup(target)
    assert len(cfg._cleanup_paths) == 1


def test_prepare_executor_subdir_makes_and_registers(tmp_path):
    import os

    cfg = StagingConfig(path=str(tmp_path))
    path = cfg.prepare_executor_subdir("inst-1")
    assert path.endswith("inst-1/")
    assert os.path.isdir(path.rstrip("/"))
    assert path.rstrip("/") in cfg._cleanup_paths


def test_register_cleanup_dir_branch(tmp_path, monkeypatch):
    """Drive the directory branch of the registered atexit closure by capturing
    the registered callback and invoking it manually."""
    import atexit

    captured: list = []
    original = atexit.register
    monkeypatch.setattr(atexit, "register", lambda fn: captured.append(fn))

    cfg = StagingConfig(path=str(tmp_path))
    target = str(tmp_path / "rm-me") + "/"
    cfg.register_cleanup(target)
    monkeypatch.setattr(atexit, "register", original)

    assert captured, "expected an atexit handler to be registered"
    # Directory exists because register_cleanup mkdir'd it.
    import os

    assert os.path.isdir(target.rstrip("/"))
    captured[0]()
    assert not os.path.exists(target.rstrip("/"))


def test_register_cleanup_file_branch(tmp_path, monkeypatch):
    """If the path is a file (not a directory) when cleanup fires, the unlink
    branch must run instead of rmtree."""
    import atexit
    import os

    captured: list = []
    original = atexit.register
    monkeypatch.setattr(atexit, "register", lambda fn: captured.append(fn))

    cfg = StagingConfig(path=str(tmp_path))
    target_file = str(tmp_path / "rm-me.tmp")
    cfg.register_cleanup(target_file)
    monkeypatch.setattr(atexit, "register", original)

    # register_cleanup created a dir; replace it with a file to exercise
    # the file branch on cleanup.
    import shutil

    if os.path.isdir(target_file):
        shutil.rmtree(target_file)
    with open(target_file, "w") as fh:
        fh.write("x")
    assert os.path.isfile(target_file)
    captured[0]()
    assert not os.path.exists(target_file)


def test_register_cleanup_oserror_swallowed(tmp_path, monkeypatch):
    """OSError during cleanup is silently swallowed."""
    import atexit
    import os

    captured: list = []
    monkeypatch.setattr(atexit, "register", lambda fn: captured.append(fn))
    cfg = StagingConfig(path=str(tmp_path))
    target = str(tmp_path / "phantom.tmp")
    cfg.register_cleanup(target)
    # Remove the directory before cleanup runs so unlink raises OSError.
    import shutil

    if os.path.isdir(target):
        shutil.rmtree(target)
    captured[0]()
