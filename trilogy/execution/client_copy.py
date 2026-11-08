"""`copy into` a data file for a dialect with no server-side `COPY`: the result
set is fetched and written here, in the shape DuckDB's `COPY` writes (CSV with
a header, JSON as one array of objects, Parquet)."""

from __future__ import annotations

import csv
import datetime
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

from trilogy.core.enums import IOType
from trilogy.core.models.datasource import StorageValue


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _csv_value(value: Any) -> Any:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _arrow_table(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> Any:
    import pyarrow as pa

    # Column-wise, so a zero-row result still carries its header.
    return pa.table(
        {name: pa.array([row[i] for row in rows]) for i, name in enumerate(columns)}
    )


def write_rows(
    path: str,
    target_type: IOType,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    storage: Mapping[str, StorageValue] | None = None,
) -> None:
    if target_type == IOType.JSON:
        records = [dict(zip(columns, row)) for row in rows]
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(records, handle, default=_json_default)
        return
    if target_type == IOType.CSV:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows([_csv_value(v) for v in row] for row in rows)
        return
    if target_type == IOType.PARQUET:
        from pyarrow import parquet

        parquet.write_table(_arrow_table(columns, rows), path, **dict(storage or {}))
        return
    raise NotImplementedError(f"Unsupported IO Type {target_type}")
