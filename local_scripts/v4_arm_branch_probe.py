"""Which obligation kind puts the individual partition ARMS into q05's covers?

The cover enumeration emits 4096 covers that all reduce to one 7-source answer
(see local_scripts/v4_cover_yield.py). The extra sources are the individual arms
of the partition families whose UNION candidate is already in the base cover.
This reports, per obligation kind, how often an arm appears as a satisfier and
how often it is the ONLY way to discharge that obligation.

Usage: python local_scripts/v4_arm_branch_probe.py [queryNN]
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.dialect.config import DuckDBConfig

WORKING = Path(__file__).parent.parent / "tests" / "modeling" / "tpc_ds_duckdb"


def arm_nodes(network: ns.SourceNetwork) -> dict[str, str]:
    """Arm candidate node -> the union candidate node that subsumes it."""
    by_identifier: dict[str, str] = {}
    for node, candidate in network.candidates.items():
        datasource = candidate.datasource
        if datasource is None or isinstance(datasource, BuildUnionDatasource):
            continue
        by_identifier[datasource.identifier] = node
    out: dict[str, str] = {}
    for node, candidate in network.candidates.items():
        datasource = candidate.datasource
        if not isinstance(datasource, BuildUnionDatasource):
            continue
        for child in datasource.children:
            arm = by_identifier.get(child.identifier)
            if arm is not None:
                out[arm] = node
    return out


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
    arms = arm_nodes(network)
    print(f"{query}: {len(arms)} arm candidates subsumed by union candidates")
    for arm, union in sorted(arms.items()):
        print(f"  {arm}  <=  {union}")

    # Replay the enumeration, recording every obligation an arm could discharge.
    offered: Counter = Counter()
    arm_only: Counter = Counter()
    union_also: Counter = Counter()
    visited: set[frozenset[str]] = set()
    stack: list[frozenset[str]] = [frozenset()]
    states = 0
    while stack and states < 40000:
        chosen = stack.pop()
        if chosen in visited:
            continue
        visited.add(chosen)
        states += 1
        pending = ns._pending_obligations(network, chosen)
        if not pending:
            continue
        first = min(pending, key=lambda o: (len(o.satisfiers), o.identity))
        arm_satisfiers = [n for n in first.satisfiers if n in arms]
        if arm_satisfiers:
            offered[first.kind] += 1
            unions = {arms[n] for n in arm_satisfiers}
            if unions & set(first.satisfiers):
                union_also[first.kind] += 1
            if len(arm_satisfiers) == len(first.satisfiers):
                arm_only[first.kind] += 1
        for node in first.satisfiers:
            stack.append(chosen | {node})

    print(f"\nstates replayed: {states}")
    print("\nobligations BRANCHED ON whose satisfiers include an arm:")
    for kind, count in offered.most_common():
        print(
            f"  {kind:<11} {count:>6}   arm-only: {arm_only[kind]:>6}   "
            f"subsuming union also a satisfier: {union_also[kind]:>6}"
        )


if __name__ == "__main__":
    main()
