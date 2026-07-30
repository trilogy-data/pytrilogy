from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from trilogy.dialect import python_source
from trilogy.dialect.python_source import (
    ParquetStreamWriter,
    PythonDatasourceError,
    build_uv_command,
    normalize_object_uri,
    open_uri_sink,
    retry_delay,
    source_key,
    staged_object_name,
    stream_script,
)

SCRIPTS = Path(__file__).parent
FIB = str(SCRIPTS / "fib.py")
ERROR = str(SCRIPTS / "error.py")


def test_build_uv_command_splits_args():
    assert build_uv_command("a.py", "--x 1 --y 'two words'") == [
        "uv",
        "run",
        "--no-project",
        "--quiet",
        "a.py",
        "--x",
        "1",
        "--y",
        "two words",
    ]


def test_source_key_is_stable_and_args_sensitive():
    assert source_key("/a/b.py") == source_key("/a/b.py")
    assert source_key("/a/b.py") != source_key("/a/c.py")
    assert source_key("/a/b.py") != source_key("/a/b.py", "--flag")


def test_staged_object_name_is_identifier_safe():
    name = staged_object_name("/tmp/my-data source.py", prefix="trilogy_py_")
    assert name.startswith("trilogy_py_my_data_source_")
    assert name.replace("_", "").isalnum()


def test_staged_object_name_truncates_long_stems():
    name = staged_object_name("/tmp/" + "x" * 100 + ".py")
    assert len(name) == 32 + 1 + 12


def test_retry_delay_clamps_past_configured_delays():
    assert retry_delay(1) == 0.5
    assert retry_delay(2) == 1.5
    assert retry_delay(99) == 1.5


def test_normalize_object_uri():
    assert normalize_object_uri("gcs://b/p") == "gs://b/p"
    assert normalize_object_uri("gs://b/p") == "gs://b/p"
    assert normalize_object_uri("/local/p") == "/local/p"


def test_stream_script_writes_parquet(tmp_path: Path):
    target = tmp_path / "fib.parquet"
    rows = stream_script(FIB, "", ParquetStreamWriter(open_uri_sink(str(target))))

    assert rows == 25
    table = pq.read_table(target)
    assert table.num_rows == 25
    assert table.column_names == ["index", "fibonacci"]


def test_stream_script_surfaces_script_error(tmp_path: Path):
    target = tmp_path / "err.parquet"
    with pytest.raises(PythonDatasourceError) as exc_info:
        stream_script(ERROR, "", ParquetStreamWriter(open_uri_sink(str(target))))

    assert "A helpful error" in str(exc_info.value)
    assert exc_info.value.return_code != 0


def test_stream_script_streams_batch_at_a_time(tmp_path: Path):
    """The writer sees batches incrementally, never one materialized table."""
    seen: list[int] = []

    def write(schema: pa.Schema, batches) -> int:
        rows = 0
        for batch in batches:
            seen.append(batch.num_rows)
            rows += batch.num_rows
        return rows

    assert stream_script(FIB, "", write) == 25
    assert sum(seen) == 25


def test_stream_script_retries_retryable_errors(monkeypatch: pytest.MonkeyPatch):
    attempts: list[int] = []

    def fake_stream_once(script, args, write):
        attempts.append(1)
        if len(attempts) == 1:
            return 1, 0, "error: failed to acquire file lock", None
        return 0, 7, "", None

    monkeypatch.setattr(python_source, "_stream_once", fake_stream_once)
    monkeypatch.setattr(python_source.time, "sleep", lambda _: None)

    assert stream_script("script.py", "", lambda schema, batches: 0) == 7
    assert len(attempts) == 2


def test_stream_script_does_not_retry_real_errors(monkeypatch: pytest.MonkeyPatch):
    attempts: list[int] = []

    def fake_stream_once(script, args, write):
        attempts.append(1)
        return 1, 0, "SyntaxError: bad script", None

    monkeypatch.setattr(python_source, "_stream_once", fake_stream_once)

    with pytest.raises(PythonDatasourceError):
        stream_script("script.py", "", lambda schema, batches: 0)
    assert len(attempts) == 1
