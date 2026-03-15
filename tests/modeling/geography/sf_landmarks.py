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

DATASET_ID = "rzic-39gi"
DATASET_URL = (
    f"https://data.sfgov.org/api/views/{DATASET_ID}/rows.csv?accessType=DOWNLOAD"
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


def load_arrow_table(csv_bytes: io.BytesIO) -> pa.Table:
    return pv.read_csv(
        csv_bytes,
        convert_options=pv.ConvertOptions(strings_can_be_null=True),
    )


def add_city_prefix(table: pa.Table) -> pa.Table:
    # Prefix OBJECTID with "sf-" to make landmark_id globally unique
    if "OBJECTID" in table.schema.names:
        ids = table["OBJECTID"].to_pylist()
        prefixed = pa.array(
            [f"sf-{v}" if v is not None else None for v in ids],
            type=pa.string(),
        )
        table = table.set_column(
            table.schema.get_field_index("OBJECTID"), "OBJECTID", prefixed
        )
    return table


def add_city_column(table: pa.Table) -> pa.Table:
    return table.append_column(
        "city", pa.array(["USSFO"] * table.num_rows, type=pa.string())
    )


def emit(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    csv_bytes = download_csv()
    table = load_arrow_table(csv_bytes)
    table = add_city_prefix(table)
    table = add_city_column(table)
    emit(table)
