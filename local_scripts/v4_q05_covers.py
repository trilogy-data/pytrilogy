"""What are q05's 4096 covers made of, and why does reduction discard them?"""

from __future__ import annotations

import sys
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"


def main() -> None:
    query = sys.argv[1] if len(sys.argv) > 1 else "query05"
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{WORKING / 'memory'}';")
    seen: list[ns.SourceNetwork] = []
    real_search = ns.search_sources
    import trilogy.core.processing.v4_helper.source_planning as sp

    def search(network):
        seen.append(network)
        return real_search(network)

    sp.search_sources = search
    engine.environment = Environment(working_path=WORKING)
    engine.generate_sql((WORKING / f"{query}.preql").read_text())
    sp.search_sources = real_search

    network = max(seen, key=lambda n: len(n.terminals))
    covers, _ = ns._enumerate_covers(network)
    base = min(covers, key=len)
    biggest = max(covers, key=len)
    targets = list(network.terminals)
    print(f"terminals ({len(targets)}): {targets}")
    print(f"\nbase cover ({len(base)}): {sorted(base)}")
    print(f"optional sources ({len(biggest - base)}): {sorted(biggest - base)}")
    print(f"\nreduce(base)    = {sorted(ns._reduce(network, base, targets))}")
    print(f"reduce(biggest) = {sorted(ns._reduce(network, biggest, targets))}")
    profile = ns._binding_profile(network, base, targets)
    print(f"\nbase binding profile: {profile}")
    print(f"partial terminals in base: {[a for a, v in profile.items() if v < 2]}")
    obligations = ns._pending_obligations(network, base)
    print(f"pending obligations at base: {[(o.kind, o.subject) for o in obligations]}")
    # which obligation minted each optional source?
    for node in sorted(biggest - base):
        candidate = network.candidates[node]
        print(f"  {node}: grain={sorted(candidate.grain)}")


if __name__ == "__main__":
    main()
