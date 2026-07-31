"""Drive a partitioned refresh from a state file.

The loop an external orchestrator would run:

  1. probe   -> `trilogy state` writes state/state.json
  2. plan    -> the stale partitions in that file ARE the work list
  3. fan out -> one `trilogy run build_partition.preql --param ...` per slice,
                each writing its own scoped delta to state/deltas/<id>.json
  4. merge   -> `trilogy state-merge` folds the deltas back into state.json

Nothing here knows anything trilogy-specific beyond the CLI and the snapshot
JSON — that is the point. Run with --help for the flags.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except (AttributeError, ValueError, NotImplementedError):
        pass

HERE = Path(__file__).parent
STATE = HERE / "state" / "state.json"
DELTAS = HERE / "state" / "deltas"
WAREHOUSE = HERE / "warehouse.duckdb"
LOCK = HERE / "state" / "warehouse.lock"

TRILOGY = [sys.executable, "-m", "trilogy.scripts.trilogy"]


@dataclass(frozen=True)
class Slice:
    """One unit of fan-out, read straight out of the state file."""

    asset: str
    datasource: str
    partition_id: str
    values: dict[str, str]
    reason: str


def run(args: list[str], echo: bool = False) -> str:
    """Run a child process, always capturing: workers run concurrently, so
    letting them write straight to the console would interleave their output."""
    result = subprocess.run(
        args,
        cwd=HERE,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    output = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(args[2:6])} failed:\n{output}")
    if echo:
        print(output.rstrip())
    return output


def probe() -> None:
    run([*TRILOGY, "state", "model.preql", "-o", str(STATE)], echo=True)


def work_list() -> list[Slice]:
    snapshot = json.loads(STATE.read_text(encoding="utf-8"))
    return [
        Slice(
            asset=asset["address"],
            datasource=ds["datasource_id"],
            partition_id=partition["partition_id"],
            values=partition["values"],
            reason=partition["stale_reason"] or "",
        )
        for asset in snapshot["assets"]
        for ds in asset["datasources"]
        for partition in ds["partitions"]
        if partition["status"] == "stale"
    ]


def warehouse_lock(timeout: float = 120.0):
    """A crude exclusive lock around the *warehouse* write.

    DuckDB opens an on-disk database with a single-writer lock, so concurrent
    `trilogy run` processes against one file would fail to connect. That is a
    property of the warehouse, not of partitioned state: the fan-out below is
    genuinely concurrent, and on an engine that admits concurrent writers
    (BigQuery, Snowflake, Postgres) you delete this and nothing else changes.
    The state plane never needs it — each worker owns its own delta file.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(str(LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return
        except FileExistsError:
            if time.monotonic() > deadline:
                raise TimeoutError(f"warehouse lock held longer than {timeout}s")
            time.sleep(0.05)


def build(slice_: Slice, param: str, serialize: bool) -> str:
    """Refresh one partition and write a delta scoped to it."""
    value = next(iter(slice_.values.values()))
    delta = DELTAS / f"{slice_.partition_id.replace('/', '__')}.json"
    args = [
        *TRILOGY,
        "run",
        "build_partition.preql",
        "--param",
        f"{param}={value}",
        "--state-file",
        str(delta),
        "--state-partition",
        slice_.partition_id,
    ]
    if serialize:
        warehouse_lock()
        try:
            run(args)
        finally:
            LOCK.unlink(missing_ok=True)
    else:
        run(args)
    return str(delta)


def merge(deltas: list[str]) -> None:
    run([*TRILOGY, "state-merge", str(STATE), *deltas], echo=True)


def reset(late_day: str | None) -> None:
    for path in (WAREHOUSE, STATE, LOCK):
        path.unlink(missing_ok=True)
    for path in DELTAS.glob("*.json"):
        path.unlink()
    seed = [sys.executable, str(HERE / "seed.py")]
    if late_day:
        seed += ["--late-day", late_day]
    run(seed, echo=True)
    run([*TRILOGY, "run", "create.preql"])
    print("reset: empty partitioned warehouse + fresh source feed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop the warehouse and state, reseed the source, recreate the table",
    )
    parser.add_argument(
        "--late-day",
        default=None,
        help="With --reset: reseed with late-arriving rows on this ISO date",
    )
    parser.add_argument("--workers", type=int, default=4, help="Fan-out width")
    parser.add_argument(
        "--param",
        default="load_date",
        help="Script parameter the partition value is passed as",
    )
    parser.add_argument(
        "--no-warehouse-lock",
        action="store_true",
        help="Skip the DuckDB single-writer lock (use on a concurrent warehouse)",
    )
    args = parser.parse_args()

    DELTAS.mkdir(parents=True, exist_ok=True)
    if args.reset:
        reset(args.late_day)

    print("\n== 1. probe ==")
    probe()

    print("\n== 2. plan ==")
    pending = work_list()
    if not pending:
        print("no stale partitions — nothing to do")
        return 0
    for item in pending:
        print(f"  {item.asset} / {item.datasource} {item.partition_id}: {item.reason}")

    print(f"\n== 3. fan out ({len(pending)} slice(s), {args.workers} worker(s)) ==")
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        deltas = list(
            pool.map(
                lambda item: build(item, args.param, not args.no_warehouse_lock),
                pending,
            )
        )
    print(f"  {len(deltas)} delta(s) in {time.monotonic() - started:.1f}s")

    print("\n== 4. merge ==")
    merge(deltas)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
