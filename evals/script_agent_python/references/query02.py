#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa


def is_prime(value: int) -> bool:
    return value >= 2 and all(
        value % divisor for divisor in range(2, int(value**0.5) + 1)
    )


values = [value for value in range(2, 201) if is_prime(value)]
rows = [{"prime": value, "ordinal": i + 1} for i, value in enumerate(values)]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
