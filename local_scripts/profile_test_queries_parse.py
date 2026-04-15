"""
Profile the parse+compile path used by tests/modeling/tpc_ds/test_queries.py::test_one.

We want a focused view on the v2 parser's contribution vs. compile-time costs,
so we capture two profiles:
  1. parse_only — just parse_text on the combined preql text (imports + select)
  2. full       — parse + generate_sql (mirrors test_one)

Both dump .prof files next to this script so they can be re-analyzed with
`python -m pstats`.
"""

from __future__ import annotations

import cProfile
import io
import pstats
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))

from trilogy import Dialects, parse  # noqa: E402
from trilogy.core.models.environment import Environment  # noqa: E402

TPC_DS_ROOT = root / "tests" / "modeling" / "tpc_ds"

QUERY_TEXT = (TPC_DS_ROOT / "query3.preql").read_text()


def _build_env() -> Environment:
    return Environment(working_path=TPC_DS_ROOT)


def profile_parse_only(iterations: int) -> cProfile.Profile:
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(iterations):
        env = _build_env()
        parse(QUERY_TEXT,env)
    profiler.disable()
    return profiler


def profile_full(iterations: int) -> cProfile.Profile:
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(iterations):
        env = _build_env()
        exc = Dialects.DUCK_DB.default_executor(environment=env)
        exc.generate_sql(QUERY_TEXT)
    profiler.disable()
    return profiler


def dump(profiler: cProfile.Profile, outfile: Path, label: str) -> None:
    profiler.dump_stats(str(outfile))
    buf = io.StringIO()
    ps = pstats.Stats(profiler, stream=buf).strip_dirs().sort_stats("tottime")
    ps.print_stats(30)
    print(f"\n=== {label}: top 30 by tottime ===")
    print(buf.getvalue())

    buf = io.StringIO()
    ps = pstats.Stats(profiler, stream=buf).strip_dirs().sort_stats("cumulative")
    ps.print_stats(30)
    print(f"=== {label}: top 30 by cumulative ===")
    print(buf.getvalue())


def main() -> None:
    warm = _build_env()
    parse(QUERY_TEXT,warm)

    iterations = 10
    print(f"Profiling parse_only x{iterations}...")
    t0 = time.perf_counter()
    prof_parse = profile_parse_only(iterations)
    print(f"parse_only wall: {time.perf_counter() - t0:.3f}s")
    dump(prof_parse, Path(__file__).with_name("parse_only.prof"), "parse_only")

    iterations_full = 5
    print(f"\nProfiling full (parse + generate_sql) x{iterations_full}...")
    t0 = time.perf_counter()
    prof_full = profile_full(iterations_full)
    print(f"full wall: {time.perf_counter() - t0:.3f}s")
    dump(prof_full, Path(__file__).with_name("parse_full.prof"), "full")


if __name__ == "__main__":
    main()
