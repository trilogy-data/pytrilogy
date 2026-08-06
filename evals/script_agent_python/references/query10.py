#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa

left = [[1, 2, 3], [4, 5, 6]]
right = [[7, 8], [9, 10], [11, 12]]
rows = [
    {"row": i, "column": j, "value": sum(left[i][k] * right[k][j] for k in range(3))}
    for i in range(2)
    for j in range(2)
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
