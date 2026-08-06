#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import json
import sys

import pyarrow as pa

data = json.loads(
    '{"departments":[{"name":"engineering","employees":[{"name":"Ada","level":3},{"name":"Linus","level":4}]},{"name":"sales","employees":[{"name":"Grace","level":2}]}]}'
)
rows = [
    {
        "department": department["name"],
        "employee": employee["name"],
        "level": employee["level"],
    }
    for department in data["departments"]
    for employee in department["employees"]
]
rows.sort(key=lambda row: (row["department"], row["employee"]))

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
