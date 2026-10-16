#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy"]
#
# # Resolved from this checkout so the fixture exercises the current contract.
# [tool.uv.sources]
# pytrilogy = { path = "../../../", editable = true }
# ///
"""Fixture for a program serving several sources (``file `x.py#name```)."""

import pyarrow as pa

from trilogy.io import run, source

REGION_SCHEMA = pa.schema([("region_id", pa.int64()), ("region", pa.string())])
STORE_SCHEMA = pa.schema([("store_id", pa.int64()), ("region_id", pa.int64())])


@source(schema=REGION_SCHEMA)
def regions() -> list[dict]:
    return [{"region_id": 1, "region": "east"}, {"region_id": 2, "region": "west"}]


@source(schema=STORE_SCHEMA)
def stores() -> list[dict]:
    return [{"store_id": i, "region_id": 1 + i % 2} for i in range(5)]


if __name__ == "__main__":
    raise SystemExit(run({"regions": regions, "stores": stores}))
