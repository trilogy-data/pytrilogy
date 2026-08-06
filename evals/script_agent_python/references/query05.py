#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa

values = [4, 8, 15, 16, 23, 42]
rows = [
    {
        "index": i,
        "value": value,
        "moving_average": sum(values[max(0, i - 2) : i + 1])
        / len(values[max(0, i - 2) : i + 1]),
    }
    for i, value in enumerate(values)
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
