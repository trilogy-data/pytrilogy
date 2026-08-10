"""Bounded reproduction for q35's all_sales customer-role timeout.

Run from the repository root. A timeout is the reproduced failure and exits 0.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import time
from pathlib import Path
from queue import Empty

from evals.common.scoring import make_scoring_engine

QUERY = """\
import raw.all_sales as al;

select
    al.channel as c,
    al.billing_customer.sk as b,
    al.ship_customer.sk as s
limit 20;
"""

DEFAULT_WORKSPACE = Path("evals/tpcds_agent/results/20260808-151955_enriched/workspace")


def run_query(workspace: str, events: mp.Queue) -> None:
    root = Path(workspace).resolve()
    engine = make_scoring_engine(root / "tpcds.duckdb", root, "tpcds")
    events.put(("generating", time.perf_counter()))
    sql = engine.generate_sql(QUERY)[-1]
    events.put(("executing", len(sql)))
    rows = list(engine.execute_raw_sql(sql).fetchall())
    events.put(("complete", len(rows)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--timeout", type=float, default=20.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ctx = mp.get_context("spawn")
    events = ctx.Queue()
    process = ctx.Process(target=run_query, args=(str(args.workspace), events))
    process.start()
    deadline = time.monotonic() + args.timeout
    observed: list[tuple[str, int | float]] = []
    while process.is_alive() and time.monotonic() < deadline:
        try:
            event = events.get(timeout=min(0.25, deadline - time.monotonic()))
            observed.append(event)
            print(event, flush=True)
        except Empty:
            pass
    if process.is_alive():
        process.terminate()
        process.join(5)
        print(f"REPRODUCED: query exceeded the bounded {args.timeout:g}s timeout")
        print(f"last stage: {observed[-1][0] if observed else 'engine startup'}")
        return 0
    process.join()
    while not events.empty():
        event = events.get()
        observed.append(event)
        print(event)
    print(f"NOT REPRODUCED: child exited {process.exitcode} within the timeout")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
