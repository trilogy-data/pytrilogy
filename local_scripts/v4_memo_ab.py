"""A/B the s51 search memos in ONE process, so the comparison is not exposed to
run-to-run machine variance.

Leg A neuters exactly the memo tables added in s51 (the pre-existing
`join_keys` / `binders` memos stay, as they did before), leg B runs as shipped.
The ALGORITHM is identical in both legs — only whether a pure result is
recomputed — so any difference is memoization and nothing else.

NOTE: `_reach_cache` no longer exists — the per-source forward closure it held
was replaced by the network's functional adjacency plus one reverse walk in
`chain_completers`, so there is no per-source reach to memoize.

Usage: python local_scripts/v4_memo_ab.py [tpcds|tpch|both]
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.setrecursionlimit(20000)

import trilogy.core.processing.v4_helper.source_planning as sp
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.dialect.config import DuckDBConfig

ROOT = Path(__file__).parent.parent
SUITES = {
    "tpcds": ROOT / "tests" / "modeling" / "tpc_ds_duckdb",
    "tpch": ROOT / "tests" / "modeling" / "tpc_h",
}

SHIPPED = {
    "full_binders": ns.SourceNetwork.full_binders,
    "chain_completers": ns.SourceNetwork.chain_completers,
    "functional_into": ns.SourceNetwork.functional_into,
    "row_complete": ns.SourceNetwork.row_complete,
}
SHIPPED_PENDING = ns._pending_obligations
SHIPPED_SEARCH = sp._memoized_search


def _uncached_full_binders(self, address):
    return frozenset(
        node for node, c in self.candidates.items() if c.binds_fully(address)
    )


def _uncached_chain_completers(self, address):
    full = _uncached_full_binders(self, address)
    return frozenset(
        node
        for node in self.candidates
        if node in full or full & ns._functional_reach(self, node)
    )


def set_memos(enabled: bool) -> None:
    if enabled:
        for name, method in SHIPPED.items():
            setattr(ns.SourceNetwork, name, method)
        ns._pending_obligations = SHIPPED_PENDING
        sp._memoized_search = SHIPPED_SEARCH
        return
    sp._memoized_search = lambda network, history: sp.search_sources(network)
    ns.SourceNetwork.full_binders = _uncached_full_binders  # type: ignore[assignment]
    ns.SourceNetwork.chain_completers = _uncached_chain_completers  # type: ignore[assignment]
    ns.SourceNetwork.functional_into = (  # type: ignore[assignment]
        lambda self, origin, target: ns._functional_into(self, origin, target)
    )
    ns.SourceNetwork.row_complete = (  # type: ignore[assignment]
        lambda self, node: ns._row_complete(self.candidates[node])
    )
    ns._pending_obligations = ns._compute_pending_obligations


def run(which: str, label: str) -> None:
    searched = {"count": 0, "seconds": 0.0}
    real_search = ns.search_sources

    def timed(network):
        t0 = time.perf_counter()
        out = real_search(network)
        searched["seconds"] += time.perf_counter() - t0
        searched["count"] += 1
        return out

    sp.search_sources = timed
    total = 0.0
    queries = 0
    for suite in ["tpcds", "tpch"] if which == "both" else [which]:
        working = SUITES[suite]
        env = Environment(working_path=working)
        engine: Executor = Dialects.DUCK_DB.default_executor(
            environment=env, conf=DuckDBConfig()
        )
        engine.execute_raw_sql(f"IMPORT DATABASE '{working / 'memory'}';")
        for f in sorted(working.glob("query*.preql")):
            engine.environment = Environment(working_path=working)
            t0 = time.perf_counter()
            engine.generate_sql(f.read_text())
            total += time.perf_counter() - t0
            queries += 1
    sp.search_sources = real_search
    print(
        f"{label:14s} generation={total:6.1f}s over {queries} queries   "
        f"search={searched['seconds']:6.1f}s over {searched['count']} calls"
    )


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    for label, enabled in (("memos OFF", False), ("memos ON", True)):
        set_memos(enabled)
        run(which, label)


if __name__ == "__main__":
    main()
