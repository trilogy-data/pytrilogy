#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import json
import sys
from collections import Counter
from urllib.request import urlopen

import pyarrow as pa

with urlopen("https://jsonplaceholder.typicode.com/albums", timeout=30) as response:
    data = json.load(response)
counts = Counter(item["userId"] for item in data)
rows = [
    {"user_id": user_id, "album_count": count}
    for user_id, count in sorted(counts.items())
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
