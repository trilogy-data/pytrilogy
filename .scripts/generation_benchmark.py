"""Time SQL generation per benchmark query, and compare two runs.

`check_benchmark_growth.py` guards the size of a plan; nothing guarded how long
the plan took to make, and one TPC-DS query went from 0.2s to 6.6s across ~25
releases unnoticed. This times `generate_sql` for every TPC-DS and TPC-H query
(fresh environment each, no database) plus the dashboard suite's compile path,
keeping the minimum of `--repeat` runs, and hashes the SQL.

    python .scripts/generation_benchmark.py --out head.json
    python .scripts/generation_benchmark.py --out base.json --trilogy-root ../base
    python .scripts/generation_benchmark.py --compare base.json head.json

`--trilogy-root` imports `trilogy` from another checkout while still reading
the queries from this one, so base and head time the same inputs. `--merge`
folds a run into an existing `--out` file, keeping each query's faster time:
a second pass for a comparison a noisy runner failed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
MODELING = ROOT / "tests" / "modeling"
QUERY_FILE = re.compile(r"query\d+.*\.preql$")

# A query fails when it is slower than RATIO x base AND by more than FLOOR
# seconds: a shared runner doubles a 20ms generation on its own.
RATIO = 2.0
FLOOR = 0.15
# The whole run is held tighter than any one query, since noise averages out.
TOTAL_RATIO = 1.3
# Both pages' default views: enough to weigh the per-query environment copy.
DASHBOARD_GROUPS = {("USBOS", "base"), ("ALL", "base")}


def _timed(function: Callable[[], str], repeat: int) -> tuple[float, str]:
    best, sql = float("inf"), ""
    for _ in range(repeat):
        start = perf_counter()
        sql = function()
        best = min(best, perf_counter() - start)
    return best, sql


def _file_cases(suite: str) -> dict[str, Callable[[], str]]:
    from trilogy import Dialects
    from trilogy.constants import CONFIG
    from trilogy.core.models.environment import Environment

    folder = MODELING / suite
    # a full collection per executor close would be timed as someone's query
    CONFIG.collect_on_close = False

    def generate(text: str) -> str:
        executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=folder)
        )
        try:
            return executor.generate_sql(text)[-1]
        finally:
            executor.close()

    def case(text: str) -> Callable[[], str]:
        return lambda: generate(text)

    return {
        f"{suite}/{path.stem}": case(path.read_text())
        for path in sorted(folder.glob("*.preql"))
        if QUERY_FILE.match(path.name)
    }


def _dashboard_cases() -> dict[str, Callable[[], str]]:
    # `trilogy` first, from wherever `--trilogy-root` put it; the harness is
    # always this checkout's
    import trilogy  # noqa: F401

    sys.path.insert(0, str(ROOT))
    from tests.modeling.tree_reporting import harness
    from tests.modeling.tree_reporting.generate_fixtures import write_dashboard_fixtures

    write_dashboard_fixtures()
    return {
        f"dashboard/{case['city']}-{case['id']}": (
            lambda case=case: harness.generate_sql(case)
        )
        for case in harness.QUERIES["cases"]
        if (case["city"], case["group"]) in DASHBOARD_GROUPS
    }


def run(repeat: int, suites: list[str]) -> dict[str, dict[str, float | str]]:
    cases: dict[str, Callable[[], str]] = {}
    for suite in suites:
        cases.update(_dashboard_cases() if suite == "dashboard" else _file_cases(suite))
    results: dict[str, dict[str, float | str]] = {}
    for label, function in cases.items():
        try:
            seconds, sql = _timed(function, repeat)
        except Exception as error:
            print(f"{label}: skipped ({type(error).__name__})")
            continue
        results[label] = {
            "seconds": round(seconds, 4),
            "sql": hashlib.sha256(sql.encode()).hexdigest()[:16],
        }
    return results


def merged(first: dict[str, dict], second: dict[str, dict]) -> dict[str, dict]:
    """Both runs' queries, each at its faster time."""
    out = {**first, **second}
    for label in set(first) & set(second):
        if first[label]["seconds"] < second[label]["seconds"]:
            out[label] = first[label]
    return out


def slower(base: float, head: float) -> bool:
    return head > base * RATIO and head - base > FLOOR


def compare(base: dict[str, dict], head: dict[str, dict]) -> int:
    shared = sorted(set(base) & set(head))
    if not shared:
        print("no shared queries to compare")
        return 2
    base_total = sum(base[label]["seconds"] for label in shared)
    head_total = sum(head[label]["seconds"] for label in shared)
    changed = sum(base[label]["sql"] != head[label]["sql"] for label in shared)
    print(
        f"{len(shared)} queries: {base_total:.2f}s -> {head_total:.2f}s "
        f"({head_total / base_total:.2f}x), {changed} with different SQL"
    )
    failures = [
        f"{label}: {base[label]['seconds']:.3f}s -> {head[label]['seconds']:.3f}s"
        for label in shared
        if slower(base[label]["seconds"], head[label]["seconds"])
    ]
    if head_total > base_total * TOTAL_RATIO:
        failures.append(f"total: {base_total:.2f}s -> {head_total:.2f}s")
    for failure in failures:
        print(f"SLOWER {failure}")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path)
    parser.add_argument("--trilogy-root", type=Path)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument(
        "--suites", nargs="+", default=["tpc_ds_duckdb", "tpc_h", "dashboard"]
    )
    parser.add_argument("--merge", action="store_true")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("BASE", "HEAD"))
    args = parser.parse_args()
    if args.compare:
        base, head = (json.loads(path.read_text()) for path in args.compare)
        return compare(base, head)
    sys.path.insert(0, str((args.trilogy_root or ROOT).resolve()))
    results = run(args.repeat, args.suites)
    total = sum(float(entry["seconds"]) for entry in results.values())
    print(f"{len(results)} queries, {total:.2f}s of generation")
    if args.out:
        if args.merge and args.out.exists():
            results = merged(json.loads(args.out.read_text()), results)
        args.out.write_text(json.dumps(results, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
