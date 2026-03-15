#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow", "requests"]
# ///

import sys
from datetime import datetime, timezone

import pyarrow as pa
import requests

DATASET_ID = "qexa-qpj6"
METADATA_URL = f"https://data.cityofnewyork.us/api/views/{DATASET_ID}.json"


def fetch_rows_updated_at() -> datetime:
    r = requests.get(METADATA_URL)
    r.raise_for_status()
    meta = r.json()

    ts = meta.get("rowsUpdatedAt")
    if ts is None:
        raise RuntimeError("Dataset metadata missing rowsUpdatedAt")

    return datetime.fromtimestamp(ts, tz=timezone.utc)


def emit(updated_at: datetime) -> None:
    table = pa.table(
        {
            "city": pa.array(["USNYC"], type=pa.string()),
            "data_updated_through": pa.array(
                [updated_at], type=pa.timestamp("us", tz="UTC")
            ),
        }
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    emit(fetch_rows_updated_at())
