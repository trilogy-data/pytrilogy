"""Benchmark DuckDB python datasource transports: the uv_run macro vs the VGI bridge.

Needs an interpreter with pytrilogy[vgi] (haybarn + vgi-python) importable:

    python local_scripts/vgi_benchmark/bench.py --rows 1000 1000000 --reps 5

Each (transport, size) runs in a fresh subprocess so worker pools, extension
loads and OS caches from one transport cannot flatter the other.
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

SOURCE_TEMPLATE = """# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
import sys

import pyarrow as pa

ROWS = {rows}
BATCH = 65536

ids = pa.array(range(BATCH), pa.int64())
batch = pa.RecordBatch.from_arrays(
    [ids, pa.array([i * 0.5 for i in range(BATCH)]), pa.array([f"label_{{i % 1000}}" for i in range(BATCH)])],
    names=["id", "value", "label"],
)
with pa.ipc.new_stream(sys.stdout.buffer, batch.schema) as writer:
    left = ROWS
    while left > 0:
        writer.write_batch(batch.slice(0, min(left, BATCH)))
        left -= BATCH
"""

MODEL = """
key id int;
property id.value float;
property id.label string;
datasource src (id: id, value: value, label: label) grain (id) file `{path}`;
"""

QUERIES = {
    "full_scan": "select count(id) -> n, sum(value) -> total, count(label) -> labels;",
    "one_column": "select sum(value) -> total;",
}


def write_source(directory: Path, rows: int) -> Path:
    path = directory / f"src_{rows}.py"
    path.write_text(SOURCE_TEMPLATE.format(rows=rows), encoding="utf-8")
    return path


def staged_bytes(directory: Path) -> int:
    return sum(f.stat().st_size for f in directory.rglob("*.arrow"))


def run_case(transport: str, source: Path, reps: int, staging: Path) -> dict:
    from trilogy import Dialects, Environment
    from trilogy.dialect.config import DuckDBConfig
    from trilogy.staging import StagingConfig

    started = time.perf_counter()
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(),
        conf=DuckDBConfig(
            enable_python_datasources=True, python_datasource_transport=transport
        ),
        staging=StagingConfig(path=str(staging)),
    )
    executor.parse_text(MODEL.format(path=source.as_posix()))
    setup = time.perf_counter() - started
    result: dict = {"setup_s": setup}
    for name, query in QUERIES.items():
        timings = []
        for _ in range(reps + 1):
            t = time.perf_counter()
            executor.execute_text(query)[-1].fetchall()
            timings.append(time.perf_counter() - t)
        result[name] = {
            "first_s": timings[0],
            "warm_median_s": statistics.median(timings[1:]),
            "warm_min_s": min(timings[1:]),
        }
    result["staged_arrow_bytes"] = staged_bytes(staging)
    executor.close()
    return result


def child(args: argparse.Namespace) -> None:
    with tempfile.TemporaryDirectory() as staging:
        out = run_case(args.transport, Path(args.source), args.reps, Path(staging))
    print(json.dumps(out))


def parent(args: argparse.Namespace) -> None:
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        for rows in args.rows:
            source = write_source(directory, rows)
            # Prime uv's environment cache so neither transport pays for it.
            subprocess.run(
                ["uv", "run", "--no-project", "--quiet", str(source)],
                stdout=subprocess.DEVNULL,
                check=True,
            )
            for transport in args.transports:
                proc = subprocess.run(
                    [
                        sys.executable,
                        __file__,
                        "--child",
                        "--transport",
                        transport,
                        "--source",
                        str(source),
                        "--reps",
                        str(args.reps),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if proc.returncode != 0:
                    sys.stderr.write(proc.stderr)
                    raise SystemExit(f"{transport} at {rows} rows failed")
                case = json.loads(proc.stdout.strip().splitlines()[-1])
                case.update(rows=rows, transport=transport)
                results.append(case)
                print(summarize(case), flush=True)
    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2), encoding="utf-8")


def summarize(case: dict) -> str:
    return (
        f"{case['transport']:>5} rows={case['rows']:>10,} setup={case['setup_s']:.3f}s "
        + " ".join(
            f"{q}: first={case[q]['first_s']:.3f}s warm={case[q]['warm_median_s']:.3f}s"
            for q in QUERIES
        )
        + f" staged={case['staged_arrow_bytes'] / 1e6:.1f}MB"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows", type=int, nargs="+", default=[1_000, 100_000, 1_000_000, 10_000_000]
    )
    parser.add_argument("--transports", nargs="+", default=["shell", "vgi"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--output")
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--transport")
    parser.add_argument("--source")
    args = parser.parse_args()
    child(args) if args.child else parent(args)


if __name__ == "__main__":
    main()
