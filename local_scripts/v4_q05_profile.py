"""Split q05's source-search seconds between cover ENUMERATION and the
per-cover connect/reduce/solution loop, then profile the hot functions.

Usage: python local_scripts/v4_q05_profile.py [queryNN]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"
SPLIT = {"enumerate": 0.0, "post": 0.0, "covers": 0, "searches": 0}


def patch() -> None:
    real_enumerate = ns._enumerate_covers
    real_search = ns.search_sources

    def enumerate_covers(network):
        t0 = time.perf_counter()
        covers, limit = real_enumerate(network)
        SPLIT["enumerate"] += time.perf_counter() - t0
        SPLIT["covers"] += len(covers)
        return covers, limit

    def search_sources(network):
        t0 = time.perf_counter()
        before = SPLIT["enumerate"]
        result = real_search(network)
        SPLIT["post"] += time.perf_counter() - t0 - (SPLIT["enumerate"] - before)
        SPLIT["searches"] += 1
        return result

    ns._enumerate_covers = enumerate_covers
    ns.search_sources = search_sources
    import trilogy.core.processing.v4_helper.source_planning as sp

    sp.search_sources = search_sources


def make_engine() -> Executor:
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{WORKING / 'memory'}';")
    return engine


def main() -> None:
    query = sys.argv[1] if len(sys.argv) > 1 else "query05"
    patch()
    engine = make_engine()
    engine.environment = Environment(working_path=WORKING)
    text = (WORKING / f"{query}.preql").read_text()
    profiler = cProfile.Profile()
    t0 = time.perf_counter()
    profiler.enable()
    engine.generate_sql(text)
    profiler.disable()
    total = time.perf_counter() - t0
    print(
        f"{query}: {total:.2f}s total, enumerate={SPLIT['enumerate']:.2f}s, "
        f"post={SPLIT['post']:.2f}s, covers={SPLIT['covers']}, "
        f"searches={SPLIT['searches']}"
    )
    stats = pstats.Stats(profiler)
    stats.sort_stats("tottime").print_stats(18)


if __name__ == "__main__":
    main()
