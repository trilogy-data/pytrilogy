#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys
import zlib

import pyarrow as pa

rows = [
    {"text": value, "crc32": zlib.crc32(value.encode())}
    for value in sorted(["trilogy", "semantic", "datasource"])
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
