#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import itertools
import sys

import pyarrow as pa

items = [("A", 6, 30), ("B", 3, 14), ("C", 4, 16), ("D", 2, 9)]
options = []
for size in range(len(items) + 1):
    for chosen in itertools.combinations(items, size):
        weight = sum(item[1] for item in chosen)
        if weight <= 10:
            options.append(
                (
                    sum(item[2] for item in chosen),
                    ",".join(item[0] for item in chosen),
                    weight,
                )
            )
value, names, weight = max(
    options, key=lambda item: (item[0], tuple(-ord(c) for c in item[1]))
)
rows = [{"items": names, "weight": weight, "value": value}]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
