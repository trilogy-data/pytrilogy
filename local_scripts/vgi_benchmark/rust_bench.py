"""Benchmark a rust datasource: the current file transport vs VGI.

Needs the binaries from ``rust/`` (``cargo build --release``) and an interpreter
with duckdb, haybarn and vgi-python:

    python local_scripts/vgi_benchmark/rust_bench.py --rows 1000 1000000 10000000

A compiled source cannot be a datasource address yet, so this drives DuckDB
directly with the SQL the engine would render:

- ``current``: the engine's own ``uv_run`` macro on stock duckdb. On Windows a
  shellfs command runs the retry wrapper, which stages the binary's Arrow stream
  to a file that ``read_arrow`` then scans; on POSIX it pipes straight into
  ``read_arrow``.
- ``vgi_bridge``: haybarn + the ``trilogy.dialect.duckdb_vgi`` bridge, which
  runs the binary per scan and streams its stdout.
- ``vgi_resident``: haybarn + ``resident.exe``, a hand-written pooled VGI worker
  that generates the same rows in-process.
- ``trilogy_io_resident``: the ``source`` binary itself served with ``--vgi``
  (trilogy-io's ``vgi`` feature), which is what the engine attaches for a rust
  datasource under the vgi transport.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

RELEASE = Path(__file__).parent / "rust" / "target" / "release"
EXE = ".exe" if sys.platform == "win32" else ""
SOURCE = (RELEASE / f"source{EXE}").as_posix()
RESIDENT = (RELEASE / f"resident{EXE}").as_posix()
QUERIES = {
    "full_scan": "select count(id), sum(value), count(label) from {relation}",
    "one_column": "select sum(value) from {relation}",
}


def current(rows: int, staging: Path) -> tuple[object, str]:
    import duckdb

    from trilogy.dialect.duckdb import get_python_datasource_setup_sql
    from trilogy.staging import StagingConfig

    con = duckdb.connect()
    con.execute(
        get_python_datasource_setup_sql(
            True, sys.platform == "win32", staging=StagingConfig(path=str(staging))
        )
    )
    return con, f"uv_run('{SOURCE}', args := '--limit {rows}')"


def vgi_bridge(rows: int, staging: Path) -> tuple[object, str]:
    import haybarn

    from trilogy.dialect.duckdb import get_vgi_setup_sql

    con = haybarn.connect()
    con.execute(
        get_vgi_setup_sql(Path(sys.executable).as_posix(), sys.platform == "win32")
    )
    return con, f"trilogy_io.main.script('{SOURCE}', '--limit {rows}')"


def vgi_resident(rows: int, staging: Path) -> tuple[object, str]:
    import haybarn

    con = haybarn.connect()
    con.execute("LOAD vgi")
    con.execute(f"ATTACH 'resident' (TYPE vgi, LOCATION '{RESIDENT}')")
    return con, f"resident.main.src({rows})"


def trilogy_io_resident(rows: int, staging: Path) -> tuple[object, str]:
    import haybarn

    from trilogy.dialect.duckdb import VGI_SOURCE_CATALOG

    con = haybarn.connect()
    con.execute("LOAD vgi")
    con.execute(
        f"ATTACH '{VGI_SOURCE_CATALOG}' AS src (TYPE vgi, LOCATION '{SOURCE} --vgi')"
    )
    return con, f"src.main.source('--limit {rows}')"


MODES = {
    "current": current,
    "vgi_bridge": vgi_bridge,
    "vgi_resident": vgi_resident,
    "trilogy_io_resident": trilogy_io_resident,
}


def run_case(mode: str, rows: int, reps: int) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        started = time.perf_counter()
        con, relation = MODES[mode](rows, Path(tmp))
        result: dict = {"setup_s": time.perf_counter() - started}
        for name, query in QUERIES.items():
            sql = query.format(relation=relation)
            timings = []
            for _ in range(reps + 1):
                t = time.perf_counter()
                con.execute(sql).fetchall()  # type: ignore[attr-defined]
                timings.append(time.perf_counter() - t)
            result[name] = {
                "first_s": timings[0],
                "warm_median_s": statistics.median(timings[1:]),
                "warm_min_s": min(timings[1:]),
            }
        result["staged_arrow_bytes"] = sum(
            f.stat().st_size for f in Path(tmp).rglob("*.arrow")
        )
        con.close()  # type: ignore[attr-defined]
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows", type=int, nargs="+", default=[1_000, 1_000_000, 10_000_000]
    )
    parser.add_argument("--modes", nargs="+", default=list(MODES))
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--output")
    parser.add_argument("--child", nargs=2)
    args = parser.parse_args()
    if args.child:
        mode, rows = args.child
        print(json.dumps(run_case(mode, int(rows), args.reps)))
        return
    results = []
    for rows in args.rows:
        for mode in args.modes:
            proc = subprocess.run(
                [sys.executable, __file__, "--child", mode, str(rows)]
                + ["--reps", str(args.reps)],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                sys.stderr.write(proc.stderr)
                raise SystemExit(f"{mode} at {rows} rows failed")
            case = json.loads(proc.stdout.strip().splitlines()[-1])
            case.update(rows=rows, mode=mode)
            results.append(case)
            print(
                f"{mode:>12} rows={rows:>10,} setup={case['setup_s']:.3f}s "
                + " ".join(
                    f"{q}: first={case[q]['first_s']:.3f}s "
                    f"warm={case[q]['warm_median_s']:.4f}s"
                    for q in QUERIES
                )
                + f" staged={case['staged_arrow_bytes'] / 1e6:.1f}MB",
                flush=True,
            )
    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
