#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = ["pyarrow"]
# ///
import sys
from datetime import UTC, datetime

import pyarrow as pa

if __name__ == "__main__":
    table = pa.table(
        {
            "data_updated_through": pa.array(
                [datetime.now(tz=UTC)], type=pa.timestamp("us", tz="UTC")
            ),
        }
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)
