#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import re
import sys
from collections import Counter

import pyarrow as pa

counts = Counter(
    re.findall(r"[a-z]+", "To be, or not to be: that is the question.".lower())
)
rows = [
    {"word": word, "count": count}
    for word, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
