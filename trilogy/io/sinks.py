"""Write an Arrow stream out in the format the caller asked for.

Batches are written as they arrive, so a source is never fully buffered --
including on the parquet-to-object-store path, which is what lets a script
stage straight to GCS instead of round-tripping Arrow through the consumer.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from enum import Enum
from typing import IO, Any

import pyarrow as pa


class Format(str, Enum):
    ARROW = "arrow"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


def normalize_object_uri(uri: str) -> str:
    """pyarrow registers GCS under the ``gs`` scheme only; accept ``gcs://`` too."""
    if uri.startswith("gcs://"):
        return "gs://" + uri[len("gcs://") :]
    return uri


def open_uri_sink(uri: str) -> Callable[[], IO[bytes]]:
    """Return a factory opening a writable binary stream at ``uri``.

    Resolves local paths, ``gs://`` and ``s3://`` through pyarrow's filesystem
    layer, which authenticates from the ambient environment (ADC for GCS).
    """
    from pyarrow import fs as pafs

    filesystem, path = pafs.FileSystem.from_uri(normalize_object_uri(uri))
    # Object stores have no directories, but a local filesystem will not create
    # the parent of a nested staging path on its own.
    if isinstance(filesystem, pafs.LocalFileSystem):
        parent = path.rsplit("/", 1)[0]
        if parent:
            filesystem.create_dir(parent, recursive=True)
    return lambda: filesystem.open_output_stream(path)


@contextmanager
def _sink(output: str | None) -> Iterator[Any]:
    if output is None:
        yield sys.stdout.buffer
        sys.stdout.buffer.flush()
        return
    with open_uri_sink(output)() as stream:
        yield stream


def write(
    reader: pa.RecordBatchReader,
    fmt: Format = Format.ARROW,
    output: str | None = None,
) -> int:
    with _sink(output) as stream:
        if fmt is Format.ARROW:
            return _write_arrow(reader, stream)
        if fmt is Format.PARQUET:
            return _write_parquet(reader, stream)
        if fmt is Format.CSV:
            return _write_csv(reader, stream)
        return _write_json(reader, stream)


def _write_arrow(reader: pa.RecordBatchReader, stream: Any) -> int:
    rows = 0
    with pa.ipc.new_stream(stream, reader.schema) as writer:
        for batch in reader:
            writer.write_batch(batch)
            rows += batch.num_rows
    return rows


def _write_parquet(reader: pa.RecordBatchReader, stream: Any) -> int:
    import pyarrow.parquet as pq

    rows = 0
    with pq.ParquetWriter(stream, reader.schema, compression="snappy") as writer:
        for batch in reader:
            writer.write_batch(batch)
            rows += batch.num_rows
    return rows


def _write_csv(reader: pa.RecordBatchReader, stream: Any) -> int:
    from pyarrow import csv

    rows = 0
    # `needed`, not pyarrow's default `all_valid`: the rust implementation quotes
    # only where it has to, and these bytes are part of the cross-language
    # contract (see tests/io/test_conformance.py).
    options = csv.WriteOptions(quoting_style="needed")
    with csv.CSVWriter(stream, reader.schema, write_options=options) as writer:
        for batch in reader:
            writer.write_batch(batch)
            rows += batch.num_rows
    return rows


def _write_json(reader: pa.RecordBatchReader, stream: Any) -> int:
    """Newline-delimited JSON. pyarrow has a reader but no writer.

    Compact separators to match arrow-rs's line-delimited writer byte for byte.
    """
    import json

    rows = 0
    for batch in reader:
        for row in batch.to_pylist():
            encoded = json.dumps(row, default=str, separators=(",", ":"))
            stream.write(encoded.encode("utf-8"))
            stream.write(b"\n")
            rows += 1
    return rows
