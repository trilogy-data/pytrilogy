"""A/B the arm prune against the CURRENT tree, not against the goldens.

The prune (`_prune_subsumed_arms`) now lives in the tree, so this disables it —
by handing `build_source_network` an empty arm map — for the baseline leg and
leaves it on for the second. The whole corpus is generated twice in ONE process
and the two outputs diffed against each other, so the only variable is the
prune. The checked-in goldens can lag the tree (a concurrent planner change),
which makes "N changed vs golden" the wrong gate for judging it.

Usage: python local_scripts/v4_arm_prune_ab.py
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
REAL_ARMS = ns._subsumed_arms
REAL_ENUMERATE = ns._enumerate_covers
COVERS: list[int] = []


def no_arms(candidates) -> dict[str, str]:
    return {}


def counting_enumerate(network):
    covers, limit = REAL_ENUMERATE(network)
    COVERS.append(len(covers))
    return covers, limit


def sweep() -> dict[str, str]:
    env = Environment(working_path=WORKING)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{WORKING / 'memory'}';")
    out: dict[str, str] = {}
    for f in sorted(WORKING.glob("query*.preql")):
        engine.environment = Environment(working_path=WORKING)
        try:
            out[f.stem] = "\n---\n".join(engine.generate_sql(f.read_text()))
        except Exception as e:
            out[f.stem] = f"ERR:{type(e).__name__}: {e}\n"
    return out


def main() -> None:
    ns._enumerate_covers = counting_enumerate
    ns._subsumed_arms = no_arms
    base = sweep()
    base_covers = (max(COVERS), sum(COVERS))
    COVERS.clear()
    ns._subsumed_arms = REAL_ARMS
    pruned = sweep()
    print(
        f"\ncovers   max {base_covers[0]:>5} -> {max(COVERS):>5}   "
        f"total {base_covers[1]:>7,} -> {sum(COVERS):>7,}"
    )
    changed = [q for q in base if base[q] != pruned[q]]
    delta = sum(len(pruned[q]) - len(base[q]) for q in changed)
    print(f"plans    {len(base) - len(changed)} identical, {len(changed)} changed")
    for q in changed:
        print(f"  {q}: {len(pruned[q]) - len(base[q]):+d} chars")
    if changed:
        print(f"net size change on changed queries: {delta:+d} chars")


if __name__ == "__main__":
    main()
