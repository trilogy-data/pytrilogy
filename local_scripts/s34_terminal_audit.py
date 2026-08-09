"""Do the network's terminals match what the ladder actually searches on?

    .venv/Scripts/python.exe local_scripts/s34_terminal_audit.py [query ...]

For every ROOT source request in the corpus, compares `build_source_network`'s
terminal set against `_search_concepts_for_bridge` minus SINGLE_ROW and
`__preql_internal` — the set the ladder's Steiner walk used to be seeded with —
both mapped through the network's equivalence classes. A dropped terminal is a lost requirement; an added one is a
spurious join axis. Reports every request where the two disagree.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Dialects, Environment
from trilogy.core.enums import Granularity
from trilogy.core.processing.v4_helper import source_planning
from trilogy.core.processing.v4_helper.network_search import build_source_network

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)

STATS: Counter[str] = Counter()
REPORTS: list[str] = []
CURRENT = ""


def ladder_terminals(request: source_planning.SourceRequest) -> set[str]:
    return {
        concept.address
        for concept in source_planning._search_concepts_for_bridge(request)
        if concept.granularity != Granularity.SINGLE_ROW
        and "__preql_internal" not in concept.address
    }


def audit_plan_source(request: source_planning.SourceRequest):
    concepts = source_planning._search_concepts_for_bridge(request)
    network = build_source_network(
        concepts, request.environment, request.graph, request.conditions
    )
    expected = {
        network.equivalence.get(address, address)
        for address in ladder_terminals(request)
    }
    actual = set(network.terminals)
    STATS["requests"] += 1
    if expected == actual:
        STATS["match"] += 1
    else:
        STATS["diverge"] += 1
        dropped = sorted(expected - actual)
        added = sorted(actual - expected)
        REPORTS.append(
            f"{CURRENT}\n  terminals: {', '.join(sorted(actual))}"
            f"\n  dropped: {dropped}\n  added:   {added}"
        )
    return _ORIGINAL(request)


_ORIGINAL = source_planning.plan_source


def main(argv: list[str]) -> int:
    global CURRENT
    source_planning.plan_source = audit_plan_source
    from trilogy.core.processing.v4_node_generators import root as v4_root

    v4_root.plan_source = audit_plan_source  # type: ignore[attr-defined]

    queries = (
        [Path(a) if Path(a).exists() else TPCDS_ROOT / a for a in argv]
        if argv
        else sorted(TPCDS_ROOT.glob("query*.preql"))
    )
    for path in queries:
        CURRENT = path.name
        env = Environment(working_path=path.parent)
        executor = Dialects.DUCK_DB.default_executor(environment=env)
        try:
            executor.generate_sql(path.read_text())
        except Exception as exc:
            STATS["query_error"] += 1
            print(f"  ERROR {path.name}: {type(exc).__name__}: {str(exc)[:120]}")
    for report in REPORTS:
        print(report)
    print("\n=== summary ===")
    for key, value in sorted(STATS.items()):
        print(f"{key:12} {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
