#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
# TODO: restore pytrilogy dependency and use trilogy.io.emit once wheel includes .scripts/build_backend.py
# See sf_landmarks_grainless_probe_with_trilogy.py for the full version.

import sys
from datetime import datetime, timezone

import pyarrow as pa


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
    main()
