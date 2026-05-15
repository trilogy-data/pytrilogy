"""Throwaway harness for testing SQL shape variants against the tpc_ds parquet set.

Usage: python _shape_lab.py
   or import sql_time(...) from another script.

Loads the parquet directory once into an in-memory duckdb, then accepts
arbitrary SQL strings to time. Run each statement N times and report the
median (warm cache) so noise is contained.
"""

from __future__ import annotations

import statistics
import time
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
IMPORT_PATH = HERE / "memory"


def make_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(f"IMPORT DATABASE '{IMPORT_PATH}';")
    con.execute("SET enable_progress_bar=false;")
    return con


def sql_time(con: duckdb.DuckDBPyConnection, sql: str, n: int = 5) -> dict:
    times: list[float] = []
    rowcount = 0
    for _ in range(n):
        t0 = time.perf_counter()
        res = con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
        rowcount = len(res)
    return {
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "rows": rowcount,
        "all": times,
    }


def run_variant(con, label: str, sql: str, n: int = 5) -> dict:
    info = sql_time(con, sql, n=n)
    print(
        f"[{label:30s}] median={info['median']*1000:8.2f}ms  "
        f"min={info['min']*1000:8.2f}ms  max={info['max']*1000:8.2f}ms  rows={info['rows']}"
    )
    return info


if __name__ == "__main__":
    con = make_conn()
    print("connection ready")
    print("tables:", con.execute("show tables").fetchall())
