#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import json
import sys
from urllib.request import urlopen

import pyarrow as pa

with urlopen(
    "https://date.nager.at/api/v3/PublicHolidays/2024/US", timeout=30
) as response:
    data = json.load(response)
rows = [
    {
        "date": item["date"],
        "name": item["name"],
        "global": item["global"],
    }
    for item in data
]
rows.sort(key=lambda row: (row["date"], row["name"]))

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
