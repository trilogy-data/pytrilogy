"""Does the network see every key two sources can actually join on?

    .venv/Scripts/python.exe local_scripts/s34_key_audit.py [query ...]

`SourceNetwork.join_keys` is a plain intersection of binding addresses modulo the
pseudonym equivalence classes. This probe asks whether the DECLARED relations
(the domain graph's ⊑/≡ edges, i.e. `merge` and scoped `subset/union join`) add
any traversal key that intersection misses: for every declared edge, it checks
whether the two endpoints survive as DISTINCT addresses among the request's
candidate bindings. Any hit is a join axis the search cannot see.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Dialects, Environment
from trilogy.core.domain_graph import EdgeProvenance
from trilogy.core.processing.v4_helper import source_planning
from trilogy.core.processing.v4_helper.network_search import build_source_network

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)

STATS: Counter[str] = Counter()
REPORTS: set[str] = set()
CURRENT = ""


def audit_plan_source(request: source_planning.SourceRequest):
    concepts = source_planning._search_concepts_for_bridge(request)
    network = build_source_network(
        concepts, request.environment, request.graph, request.conditions
    )
    STATS["requests"] += 1
    bound = {address for c in network.candidates.values() for address in c.bindings}
    for edge in request.environment.domain_graph.edges:
        if edge.provenance is not EdgeProvenance.DECLARED:
            continue
        left = network.equivalence.get(edge.source, edge.source)
        right = network.equivalence.get(edge.target, edge.target)
        if left == right:
            STATS["edge_already_unified"] += 1
            continue
        if left in bound and right in bound:
            STATS["edge_split_and_bound"] += 1
            REPORTS.add(f"{CURRENT}  {edge.relation.name}  {left}  <->  {right}")
        else:
            STATS["edge_split_unbound"] += 1
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
    for report in sorted(REPORTS):
        print(report)
    print("\n=== summary ===")
    for key, value in sorted(STATS.items()):
        print(f"{key:24} {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
