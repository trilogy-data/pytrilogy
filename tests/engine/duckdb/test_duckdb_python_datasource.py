import re

from trilogy.dialect.duckdb import get_python_datasource_setup_sql
from trilogy.staging import StagingConfig


def test_python_datasource_disabled_sql():
    """Test SQL generation when python datasources are disabled."""
    sql = get_python_datasource_setup_sql(enabled=False, is_windows=False)
    assert "error" in sql.lower()
    assert "enable_python_datasources=True" in sql


def test_python_datasource_enabled_unix():
    """Test SQL generation for unix when enabled."""
    sql = get_python_datasource_setup_sql(enabled=True, is_windows=False)
    assert "INSTALL shellfs" in sql
    assert "INSTALL arrow" in sql
    assert "read_arrow" in sql
    assert "uv run" in sql


def test_python_datasource_enabled_windows_unique_ids():
    """Test that Windows mode generates unique temp files per instance_id."""
    sql1 = get_python_datasource_setup_sql(
        enabled=True, is_windows=True, instance_id="test-id-1"
    )
    sql2 = get_python_datasource_setup_sql(
        enabled=True, is_windows=True, instance_id="test-id-2"
    )

    assert "trilogy_uv_run_test-id-1.arrow" in sql1
    assert "trilogy_uv_run_test-id-2.arrow" in sql2
    assert sql1 != sql2


def test_python_datasource_windows_auto_uuid():
    """Test that Windows mode generates UUID when no instance_id provided."""
    sql1 = get_python_datasource_setup_sql(enabled=True, is_windows=True)
    sql2 = get_python_datasource_setup_sql(enabled=True, is_windows=True)

    uuid_pattern = r"trilogy_uv_run_[a-f0-9-]{36}\.arrow"
    assert re.search(uuid_pattern, sql1)
    assert re.search(uuid_pattern, sql2)
    assert sql1 != sql2


def test_python_datasource_windows_structure():
    """Test Windows SQL has correct structure."""
    sql = get_python_datasource_setup_sql(
        enabled=True, is_windows=True, instance_id="test"
    )
    assert "INSTALL shellfs" in sql
    assert "INSTALL arrow" in sql
    assert "SET VARIABLE __trilogy_uv_temp_file" in sql
    assert "read_json" in sql
    assert "read_arrow" in sql
    assert "getvariable" in sql


def test_python_datasource_windows_custom_staging():
    """Test that Windows mode uses the staging config path."""
    staging = StagingConfig(path="/custom/staging")
    sql = get_python_datasource_setup_sql(
        enabled=True, is_windows=True, instance_id="test", staging=staging
    )
    assert "/custom/staging/trilogy_uv_run_test.arrow" in sql


def test_python_datasource_windows_gcs_staging():
    """Test that Windows mode supports GCS staging URIs."""
    staging = StagingConfig(path="gs://my-bucket/staging")
    sql = get_python_datasource_setup_sql(
        enabled=True, is_windows=True, instance_id="test", staging=staging
    )
    assert "gs://my-bucket/staging/trilogy_uv_run_test.arrow" in sql


def test_python_datasource_windows_s3_staging():
    """Test that Windows mode supports S3 staging URIs."""
    staging = StagingConfig(path="s3://my-bucket/staging")
    sql = get_python_datasource_setup_sql(
        enabled=True, is_windows=True, instance_id="test", staging=staging
    )
    assert "s3://my-bucket/staging/trilogy_uv_run_test.arrow" in sql
