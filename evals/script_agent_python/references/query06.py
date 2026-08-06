#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa

xs = [1, 2, 3, 4, 5]
ys = [2, 3, 5, 4, 6]
x_mean = sum(xs) / len(xs)
y_mean = sum(ys) / len(ys)
slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / sum(
    (x - x_mean) ** 2 for x in xs
)
rows = [{"slope": slope, "intercept": y_mean - slope * x_mean}]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
