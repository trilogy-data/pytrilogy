#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow", "pytrilogy"]
# ///
"""The python twin of ``crates/trilogy-io/examples/catalog.rs``.

One program serving two sources, chosen by ``--source``: ``landmarks`` is the
single-source conformance fixture, ``states`` a small second table.
"""

import sys
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))

from landmarks import SCHEMA, STATES, landmarks

from trilogy.io import run, source

STATES_SCHEMA = pa.schema(
    [
        pa.field("state", pa.string(), nullable=False),
        pa.field("position", pa.int64(), nullable=False),
    ]
)


@source(schema=STATES_SCHEMA)
def states() -> pa.Table:
    return pa.table(
        {"state": STATES, "position": list(range(len(STATES)))}, schema=STATES_SCHEMA
    )


if __name__ == "__main__":
    raise SystemExit(
        run({"landmarks": source(landmarks, schema=SCHEMA), "states": states})
    )
