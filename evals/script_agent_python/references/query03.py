#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa


def collatz(value: int) -> list[int]:
    values = [value]
    while value != 1:
        value = value // 2 if value % 2 == 0 else 3 * value + 1
        values.append(value)
    return values


rows = [{"step": i, "value": value} for i, value in enumerate(collatz(27))]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
