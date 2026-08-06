#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import random
import sys

import pyarrow as pa

samples = 100000
rng = random.Random(2026)
inside = sum(rng.random() ** 2 + rng.random() ** 2 <= 1 for _ in range(samples))
rows = [
    {"samples": samples, "inside_circle": inside, "pi_estimate": 4 * inside / samples}
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
