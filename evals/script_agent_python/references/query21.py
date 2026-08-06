#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import json
import sys
from collections import defaultdict
from urllib.request import urlopen

import pyarrow as pa

with urlopen("https://jsonplaceholder.typicode.com/todos", timeout=30) as response:
    data = json.load(response)
counts: dict[int, list[int]] = defaultdict(lambda: [0, 0])
for item in data:
    counts[item["userId"]][0 if item["completed"] else 1] += 1
rows = [
    {"user_id": user, "completed_count": values[0], "incomplete_count": values[1]}
    for user, values in sorted(counts.items())
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
