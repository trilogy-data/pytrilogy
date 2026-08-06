#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import json
import sys
from urllib.request import urlopen

import pyarrow as pa

with urlopen("https://jsonplaceholder.typicode.com/users", timeout=30) as response:
    data = json.load(response)
rows = [
    {"id": item["id"], "name": item["name"], "email": item["email"].lower()}
    for item in data
    if item["email"].lower().endswith(".biz")
]
rows.sort(key=lambda row: row["id"])

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
