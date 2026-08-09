"""Who actually answers each ROOT request when the network search is ON?

    .venv/Scripts/python.exe local_scripts/s34_path_census.py [query ...]

The ladder is gone, so this reports how the surviving arms split: `network` (the
search picked a multi-source cover), `direct-source` (the search said ONE SCAN and
`_direct_source` rendered it), and `nobody` (the search DECLINED and nothing else
could answer). Any `nobody` row is a request that used to be caught silently by
the ladder and is now a hard failure — that is the work.
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.core.processing.v4_helper import source_planning

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)

STATS: Counter[str] = Counter()
EXAMPLES: dict[str, list[str]] = defaultdict(list)
CURRENT = ""
# One frame per in-flight plan_source call; recursion gets its own.
FRAMES: list[dict[str, object]] = []

_PLAN_SOURCE = source_planning.plan_source
_NETWORK_PLAN = source_planning._network_source
_DIRECT = source_planning._direct_source


def traced_network_plan(request):
    plan = _NETWORK_PLAN(request)
    if FRAMES:
        FRAMES[-1]["network_planned"] = plan is not None
    return plan


def traced_direct(request, attempt):
    node = _DIRECT(request, attempt)
    if FRAMES and node is not None:
        FRAMES[-1]["last"] = "direct-source"
    return node


def decline_reason(request) -> str:
    """Re-run the search's own gates to name the arm that gave up."""
    concepts = source_planning._search_concepts_for_bridge(request)
    network = ns.build_source_network(
        concepts, request.environment, request.graph, request.conditions
    )
    result = ns.search_sources(network)
    if result.solution is None:
        if result.unreachable:
            return "unreachable terminal"
        return "no cover"
    if (
        len(result.solution.sources) == 1
        and not network.candidates[result.solution.sources[0]].is_union
    ):
        return "single scan, deferred by design"
    return "emitter rejected the network plan"


def census_plan_source(request):
    FRAMES.append({"network_planned": False, "last": None})
    try:
        node = _PLAN_SOURCE(request)
    finally:
        frame = FRAMES.pop()
    if node is None:
        STATS["nobody: request unplannable"] += 1
        return node
    last = frame["last"]
    if last is None:
        STATS["network"] += 1
        return node
    reason = decline_reason(request)
    key = f"{last}  <- network: {reason}"
    STATS[key] += 1
    if len(EXAMPLES[key]) < 5:
        EXAMPLES[key].append(f"{CURRENT} [{', '.join(sorted(_addresses(request)))}]")
    return node


def _addresses(request) -> set[str]:
    return {c.address for c in request.outputs}


def main(argv: list[str]) -> int:
    global CURRENT
    CONFIG.use_v4_discovery = True
    source_planning._network_source = traced_network_plan
    source_planning._direct_source = traced_direct
    source_planning.plan_source = census_plan_source
    from trilogy.core.processing.v4_node_generators import root as v4_root

    v4_root.plan_source = census_plan_source  # type: ignore[attr-defined]

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
    total = sum(v for k, v in STATS.items() if k != "query_error")
    print("\n=== who answered each ROOT request ===")
    for key, value in sorted(STATS.items(), key=lambda kv: (-kv[1], kv[0])):
        share = f"{100 * value / total:5.1f}%" if total else ""
        print(f"{value:5} {share}  {key}")
        for example in EXAMPLES.get(key, []):
            print(f"                  {example[:150]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
