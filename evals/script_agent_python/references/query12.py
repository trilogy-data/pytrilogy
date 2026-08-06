#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=16"]
# ///

import sys

import pyarrow as pa


def ordered_tasks() -> list[str]:
    edges = [
        ("clean", "build"),
        ("lint", "test"),
        ("build", "test"),
        ("test", "package"),
    ]
    tasks = sorted({item for edge in edges for item in edge})
    incoming = {
        task: {left for left, right in edges if right == task} for task in tasks
    }
    result: list[str] = []
    while incoming:
        task = min(task for task, required in incoming.items() if not required)
        result.append(task)
        incoming.pop(task)
        for required in incoming.values():
            required.discard(task)
    return result


rows = [{"position": i + 1, "task": task} for i, task in enumerate(ordered_tasks())]

table = pa.Table.from_pylist(rows)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
