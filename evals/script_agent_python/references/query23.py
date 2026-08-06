#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import csv
import io
import sys
from collections import defaultdict
from urllib.request import urlopen

import pyarrow as pa

with urlopen(
    "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv", timeout=30
) as response:
    text = response.read().decode()
groups: dict[str, list[float]] = defaultdict(list)
for item in csv.DictReader(io.StringIO(text)):
    groups[item["species"]].append(float(item["sepal_length"]))
rows = [
    {
        "species": species,
        "row_count": len(values),
        "average_sepal_length": sum(values) / len(values),
    }
    for species, values in sorted(groups.items())
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
