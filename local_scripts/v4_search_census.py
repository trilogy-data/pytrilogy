"""Census of the v4 network search over the TPC-DS + TPC-H corpora.

Per ROOT source request: which planner answered, how many enumeration states
and covers the obligation search visited, whether it truncated, and how long
it took. Answers "is any of this dead?" and "where does generation time go?"
by instrumentation rather than by reading code.

Usage: python local_scripts/v4_search_census.py [tpcds|tpch|both]
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.core.processing.v4_helper import source_planning as sp
from trilogy.dialect.config import DuckDBConfig

ROOT = Path(__file__).parent.parent
SUITES = {
    "tpcds": ROOT / "tests" / "modeling" / "tpc_ds_duckdb",
    "tpch": ROOT / "tests" / "modeling" / "tpc_h",
}

STATS: Counter = Counter()
# (query, states, covers, truncated, seconds)
SEARCHES: list[tuple[str, int, int, bool, float]] = []
CURRENT = ""


def patch() -> None:
    real_enumerate = ns._enumerate_covers
    real_search = ns.search_sources
    real_network_source = sp._network_source
    real_direct = sp._direct_source
    real_cross = sp._cross_component_source
    pinned = {
        "coalescing_axis": sp._plan_coalescing_axis,
        "complete_where": sp._plan_complete_where_source,
        "finer_filter_rollup": sp._plan_finer_filter_rollup,
    }
    state: dict[str, int] = {"states": 0, "covers": 0}

    def enumerate_covers(network):
        covers, truncated = real_enumerate(network)
        state["covers"] = len(covers)
        # visited-state count is internal; approximate by re-deriving the
        # branching factor is not possible, so record cover count only.
        return covers, truncated

    def search_sources(network):
        t0 = time.perf_counter()
        result = real_search(network)
        dt = time.perf_counter() - t0
        SEARCHES.append(
            (CURRENT, len(network.candidates), state["covers"], result.truncated, dt)
        )
        STATS["search_calls"] += 1
        if result.truncated:
            STATS[
                f"search_truncated_{result.limit.value if result.limit else '?'}"
            ] += 1
        # A decline for lack of BUDGET is not a decline for lack of a solution;
        # the fallbacks cannot tell, so the census must.
        if result.exhausted:
            STATS["search_exhausted"] += 1
        elif result.solution is None:
            STATS["search_declined"] += 1
        return result

    def network_source(request, defer_single_scan: bool = True):
        decision = real_network_source(request, defer_single_scan)
        if decision is None:
            STATS["network_none"] += 1
        elif decision.bridge is None:
            STATS["network_single_scan"] += 1
        else:
            STATS["network_bridge"] += 1
        return decision

    def direct_source(request, accept_partial):
        out = real_direct(request, accept_partial)
        STATS["direct_hit" if out is not None else "direct_miss"] += 1
        return out

    def cross_component(request):
        out = real_cross(request)
        if out is not None:
            STATS["cross_component"] += 1
        return out

    def make_pinned(name, fn):
        def wrapper(request):
            out = fn(request)
            if out is not None:
                STATS[f"pinned_{name}"] += 1
            return out

        return wrapper

    ns._enumerate_covers = enumerate_covers
    ns.search_sources = search_sources
    sp.search_sources = search_sources
    sp._network_source = network_source
    sp._direct_source = direct_source
    sp._cross_component_source = cross_component
    for name, fn in pinned.items():
        setattr(sp, f"_plan_{name}", make_pinned(name, fn))
    sp._plan_coalescing_axis = make_pinned("coalescing_axis", pinned["coalescing_axis"])
    sp._plan_complete_where_source = make_pinned(
        "complete_where", pinned["complete_where"]
    )
    sp._plan_finer_filter_rollup = make_pinned(
        "finer_filter_rollup", pinned["finer_filter_rollup"]
    )


def make_engine(working: Path) -> Executor:
    env = Environment(working_path=working)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{working / 'memory'}';")
    return engine


def main() -> None:
    global CURRENT
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    patch()
    for suite in ["tpcds", "tpch"] if which == "both" else [which]:
        working = SUITES[suite]
        engine = make_engine(working)
        for f in sorted(working.glob("query*.preql")):
            CURRENT = f"{suite}/{f.stem}"
            engine.environment = Environment(working_path=working)
            t0 = time.perf_counter()
            try:
                engine.generate_sql(f.read_text())
            except Exception as e:
                STATS[f"gen_error_{type(e).__name__}"] += 1
                print(f"{CURRENT} ERROR {type(e).__name__}", flush=True)
                continue
            print(f"{CURRENT} {time.perf_counter() - t0:.2f}s", flush=True)
    print("\n== planner census ==")
    for key, value in sorted(STATS.items()):
        print(f"{key}\t{value}")
    print("\n== slowest searches ==")
    for row in sorted(SEARCHES, key=lambda r: -r[4])[:20]:
        print(
            f"{row[0]}\tcands={row[1]}\tcovers={row[2]}\ttrunc={row[3]}\t{row[4]:.2f}s"
        )
    total = sum(r[4] for r in SEARCHES)
    print(f"\ntotal search seconds: {total:.1f} over {len(SEARCHES)} searches")


if __name__ == "__main__":
    main()
