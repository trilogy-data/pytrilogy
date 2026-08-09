"""How much of the cover enumeration is wasted work?

For the widest requests, report: covers emitted, how many DISTINCT reduced
source sets they collapse to, and at which cover index the eventual winner was
first reached. Tells us whether a cost bound would buy anything a cheaper
dedup would not.

Usage: python local_scripts/v4_cover_yield.py [queryNN ...]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"


def make_engine() -> Executor:
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{WORKING / 'memory'}';")
    return engine


def report(network: ns.SourceNetwork, label: str) -> None:
    covers, limit = ns._enumerate_covers(network)
    targets = list(network.terminals)
    order: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()
    best: tuple | None = None
    best_at = -1
    disconnected = 0
    sizes: dict[int, int] = {}
    for index, cover in enumerate(covers):
        sizes[len(cover)] = sizes.get(len(cover), 0) + 1
        if not ns._is_connected(network, cover):
            disconnected += 1
            continue
        key = tuple(sorted(ns._reduce(network, cover, targets)))
        if key in seen:
            continue
        seen.add(key)
        order.append(key)
        solution = ns._solution_for(network, frozenset(key), targets)
        rank = (solution.cost.axes(), solution.sources)
        if best is None or rank < best:
            best, best_at = rank, index
    print(
        f"{label}: covers={len(covers)} limit={limit.value if limit else '-'} "
        f"disconnected={disconnected} distinct_reduced={len(seen)} "
        f"winner_first_seen_at_cover={best_at} sizes={dict(sorted(sizes.items()))}"
    )


def main() -> None:
    queries = sys.argv[1:] or ["query05"]
    engine = make_engine()
    for query in queries:
        seen_networks: list[ns.SourceNetwork] = []
        real_search = ns.search_sources

        def search(network, _seen=seen_networks, _real=real_search):
            _seen.append(network)
            return _real(network)

        import trilogy.core.processing.v4_helper.source_planning as sp

        sp.search_sources = search
        engine.environment = Environment(working_path=WORKING)
        engine.generate_sql((WORKING / f"{query}.preql").read_text())
        sp.search_sources = real_search
        widest = sorted(seen_networks, key=lambda n: -len(n.terminals))[:2]
        for index, network in enumerate(widest):
            report(network, f"{query}#{index} terminals={len(network.terminals)}")


if __name__ == "__main__":
    main()
