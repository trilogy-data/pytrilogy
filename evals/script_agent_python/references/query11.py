#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa


def roman(value: int) -> str:
    pairs = [(10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I")]
    output = ""
    for amount, symbol in pairs:
        while value >= amount:
            output += symbol
            value -= amount
    return output


rows = [{"number": value, "roman": roman(value)} for value in range(1, 26)]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
