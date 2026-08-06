#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys
from datetime import date

import pyarrow as pa

rows = [
    {"date": date(year, month, 13).isoformat()}
    for year in range(2020, 2031)
    for month in range(1, 13)
    if date(year, month, 13).weekday() == 4
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
