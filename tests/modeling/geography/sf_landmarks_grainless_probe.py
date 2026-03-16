#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow", "requests", "pytrilogy"]
# ///

import sys
from datetime import datetime, timezone

import pyarrow as pa

from trilogy.io import emit


def main() -> pa.Table:
    table = pa.table(
        {
            "data_updated_through": pa.array(
                [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            )
        }
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)
    return table


if __name__ == "__main__":
    emit(main)
