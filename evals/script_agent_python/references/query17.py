#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import csv
import io
import sys
from collections import defaultdict

import pyarrow as pa

source = (
    "product,quantity,price\napple,3,1.20\nbanana,5,0.50\napple,2,1.20\npear,4,0.80\n"
)
totals: dict[str, list[float]] = defaultdict(lambda: [0, 0.0])
for item in csv.DictReader(io.StringIO(source)):
    totals[item["product"]][0] += int(item["quantity"])
    totals[item["product"]][1] += int(item["quantity"]) * float(item["price"])
rows = [
    {"product": product, "quantity": int(values[0]), "revenue": values[1]}
    for product, values in sorted(totals.items())
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
