#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow"]
# ///
import sys
import pyarrow as pa

if __name__ == "__main__":
    table = pa.table(
        {
            "tree_id": pa.array([], type=pa.string()),
            "city": pa.array([], type=pa.string()),
            "usbos_source": pa.array([], type=pa.string()),
        }
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)
