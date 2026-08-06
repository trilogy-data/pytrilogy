#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa


def distances() -> dict[str, int]:
    graph = {
        "A": [("B", 4), ("C", 2)],
        "B": [("D", 5)],
        "C": [("B", 1), ("D", 8), ("E", 10)],
        "D": [("E", 2)],
        "E": [("D", 2)],
    }
    result = {node: 10**9 for node in graph}
    result["A"] = 0
    pending = set(graph)
    while pending:
        node = min(pending, key=lambda candidate: result[candidate])
        pending.remove(node)
        for neighbor, weight in graph[node]:
            result[neighbor] = min(result[neighbor], result[node] + weight)
    return result


rows = [
    {"node": node, "distance": distance}
    for node, distance in sorted(distances().items())
]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
