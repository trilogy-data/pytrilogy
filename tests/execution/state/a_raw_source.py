#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///

import sys
import pyarrow as pa


def emit(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    table = pa.table(
        {
            "id": pa.array(list(range(1, 11)), type=pa.int64()),
            "value": pa.array([f"a_value_{i}" for i in range(1, 11)], type=pa.string()),
            "source": pa.array(["a"] * 10, type=pa.string()),
        }
    )
    emit(table)
