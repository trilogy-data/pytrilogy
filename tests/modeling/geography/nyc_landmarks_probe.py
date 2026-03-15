#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow", "requests", "pytrilogy"]
# ///

from datetime import datetime, timezone

import pyarrow as pa
import requests

from trilogy.io import emit

DATASET_ID = "qexa-qpj6"
METADATA_URL = f"https://data.cityofnewyork.us/api/views/{DATASET_ID}.json"


def fetch_rows_updated_at() -> pa.Table:
    r = requests.get(METADATA_URL)
    r.raise_for_status()
    meta = r.json()

    ts = meta.get("rowsUpdatedAt")
    if ts is None:
        raise RuntimeError("Dataset metadata missing rowsUpdatedAt")

    return pa.table(
        {
            "city": pa.array(["USNYC"], type=pa.string()),
            "data_updated_through": pa.array(
                [datetime.fromtimestamp(ts, tz=timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )


if __name__ == "__main__":
    emit(fetch_rows_updated_at)
