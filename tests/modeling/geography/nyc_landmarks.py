#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow", "requests"]
# ///

import io
import sys

import pyarrow as pa
import pyarrow.csv as pv
import requests

DATASET_ID = "qexa-qpj6"
DATASET_URL = (
    f"https://data.cityofnewyork.us/api/views/{DATASET_ID}/rows.csv?accessType=DOWNLOAD"
)


def download_csv() -> io.BytesIO:
    r = requests.get(DATASET_URL, stream=True)
    r.raise_for_status()

    buf = io.BytesIO()
    for chunk in r.iter_content(chunk_size=1024 * 1024):
        if chunk:
            buf.write(chunk)
    buf.seek(0)
    return buf


def cast_columns(table: pa.Table) -> pa.Table:
    # Normalize column names to lowercase for consistent mapping
    table = table.rename_columns([c.lower() for c in table.schema.names])

    # Normalize ID → "landmark_id" with "nyc-" prefix
    id_col = next(
        (c for c in table.schema.names if c in ("lp_number", "lm_id", "objectid")),
        None,
    )
    if id_col is not None:
        ids = table[id_col].to_pylist()
        prefixed = pa.array(
            [f"nyc-{v}" if v is not None else None for v in ids],
            type=pa.string(),
        )
        idx = table.schema.get_field_index(id_col)
        table = table.set_column(idx, id_col, prefixed)
        table = table.rename_columns(
            [("landmark_id" if c == id_col else c) for c in table.schema.names]
        )

    # Normalize name → "name" (dataset uses "scen_lm_na")
    name_col = next(
        (c for c in table.schema.names if c in ("scen_lm_na", "lm_name", "name")),
        None,
    )
    if name_col is not None and name_col != "name":
        table = table.rename_columns(
            [("name" if c == name_col else c) for c in table.schema.names]
        )

    return table


def load_arrow_table(csv_bytes: io.BytesIO) -> pa.Table:
    table = pv.read_csv(
        csv_bytes,
        convert_options=pv.ConvertOptions(strings_can_be_null=True),
    )
    return cast_columns(table)


def add_city_column(table: pa.Table) -> pa.Table:
    return table.append_column(
        "city", pa.array(["USNYC"] * table.num_rows, type=pa.string())
    )


def emit(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    csv_bytes = download_csv()
    table = load_arrow_table(csv_bytes)
    table = add_city_column(table)
    emit(table)
