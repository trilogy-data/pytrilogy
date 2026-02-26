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
