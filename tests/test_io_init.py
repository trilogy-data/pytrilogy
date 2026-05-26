import io as _io
import sys
from contextlib import contextmanager
from typing import Iterator

import pyarrow as pa
import pytest

from trilogy.io import emit


@contextmanager
def _capture_stdout_buffer():
    buf = _io.BytesIO()
    original = sys.stdout

    class _Wrapper:
        @property
        def buffer(self):
            return buf

        def flush(self):
            pass

    sys.stdout = _Wrapper()
    try:
        yield buf
    finally:
        sys.stdout = original


def test_emit_table():
    def make() -> pa.Table:
        return pa.table({"a": [1, 2], "b": ["x", "y"]})

    with _capture_stdout_buffer() as buf:
        emit(make)
    reader = pa.ipc.open_stream(buf.getvalue())
    out = reader.read_all()
    assert out.column("a").to_pylist() == [1, 2]
    assert out.column("b").to_pylist() == ["x", "y"]


def test_emit_batches():
    schema = pa.schema([("a", pa.int64())])

    def make() -> Iterator[pa.RecordBatch]:
        yield pa.record_batch([pa.array([1, 2])], schema=schema)
        yield pa.record_batch([pa.array([3, 4])], schema=schema)

    with _capture_stdout_buffer() as buf:
        emit(make)
    reader = pa.ipc.open_stream(buf.getvalue())
    out = reader.read_all()
    assert out.column("a").to_pylist() == [1, 2, 3, 4]


def test_emit_missing_return_hint():
    def no_hint():
        return pa.table({"a": [1]})

    with pytest.raises(TypeError, match="return type hint"):
        emit(no_hint)


def test_emit_unsupported_return_type():
    def bad() -> int:
        return 1

    with pytest.raises(TypeError, match="Unsupported return type"):
        emit(bad)


def test_emit_iterator_non_recordbatch_falls_to_unsupported():
    def gen() -> Iterator[int]:
        yield 1

    with pytest.raises(TypeError, match="Unsupported return type"):
        emit(gen)
