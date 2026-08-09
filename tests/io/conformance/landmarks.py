#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow", "pytrilogy"]
# ///
"""The python twin of ``crates/trilogy-io/examples/landmarks.rs``.

Same data, same claims. The conformance test runs both through the same flags
and requires identical output -- that is what makes the command line, rather
than either library, the actual contract.
"""

import pyarrow as pa

from trilogy.io import run

STATES = ["CA", "NY", "TX", "WA"]
TOTAL = 100

SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string()), ("state", pa.string())])


def landmarks(limit: int | None = None) -> pa.Table:
    count = min(limit if limit is not None else TOTAL, TOTAL)
    ids = list(range(count))
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "name": pa.array([f"landmark-{i}" for i in ids], type=pa.string()),
            "state": pa.array([STATES[i % 4] for i in ids], type=pa.string()),
        }
    )


if __name__ == "__main__":
    raise SystemExit(run(landmarks, schema=SCHEMA))
