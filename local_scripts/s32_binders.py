"""Who binds an address, and how? Usage:

.venv/Scripts/python.exe local_scripts/s32_binders.py <query.preql> <address> [...]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.processing.v4_helper.network_search import build_source_network

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)


def main(argv: list[str]) -> int:
    path = Path(argv[0]) if Path(argv[0]).exists() else TPCDS_ROOT / argv[0]
    addresses = argv[1:]
    env = Environment(working_path=path.parent)
    env.parse(path.read_text())
    benv = env.materialize_for_select()
    graph = generate_graph(benv)
    terminals = [benv.concepts[a] for a in addresses if a in benv.concepts]
    missing = [a for a in addresses if a not in benv.concepts]
    if missing:
        print(f"not in environment: {missing}")
    network = build_source_network(terminals, benv, graph)
    for address in addresses:
        key = network.equivalence.get(address, address)
        print(f"\n{address}  (class {key})")
        for node in network.binders(key):
            candidate = network.candidates[node]
            binding = candidate.bindings[key]
            grain = ",".join(sorted(candidate.datasource.grain.components)) or "*"
            print(
                f"  {node:55} {binding.strength.value:8}"
                f" stored={binding.stored!s:5} grain=({grain})"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
