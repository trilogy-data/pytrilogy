"""Prototype A: model-level connector-path ambiguity standard.

Standard: a pair of key equivalence classes is AMBIGUOUS when no single
datasource co-locates them and the minimal (shortest datasource-hop)
join paths between them carry incomparable connector-key sets (no set is a
subset of all others — the detect_ambiguity_and_raise rule). A hop between
two datasources joins on ALL their shared key classes, so a composite-grain
snowflake hop is ONE path, not one path per key. Longer paths are treated as
dominated by shorter ones. Computed statically from the model's datasource
declarations instead of from repeated discovery searches.

Usage:
  .venv/Scripts/python.exe local_scripts/proto_ambiguity.py [model ...]
Models default to the full registry below.
"""

from __future__ import annotations

import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from trilogy import Environment
from trilogy.core.enums import Purpose
from trilogy.core.models.build_environment import BuildEnvironment

# Alternatives tracked per node during shortest-path DAG traversal; more than
# this reports as ">=cap" rather than enumerating.
SET_CAP = 8

DS_PREFIX = "ds~"


def _find(parent: dict[str, str], node: str) -> str:
    while parent.get(node, node) != node:
        parent[node] = parent.get(parent[node], parent[node])
        node = parent[node]
    return node


def _union(parent: dict[str, str], left: str, right: str) -> None:
    a, b = _find(parent, left), _find(parent, right)
    if a == b:
        return
    lo, hi = sorted((a, b))
    parent[hi] = lo


@dataclass
class KeyGraph:
    """Datasource-level join graph: nodes are datasources, an edge joins two
    datasources on ALL the key classes they share."""

    rep: dict[str, str]
    binders: dict[str, frozenset[str]]  # ds name -> bound key classes
    by_class: dict[str, frozenset[str]]  # key class -> ds names binding it
    edges: dict[str, dict[str, frozenset[str]]]  # ds -> ds -> shared classes
    # key class -> classes of its members' declared keys/grain (FK identity)
    key_closure: dict[str, frozenset[str]] = field(default_factory=dict)
    # per-class memo of connector alternatives (see connector_sets_from)
    _source_cache: dict[str, dict[str, list[frozenset[str]]]] = field(
        default_factory=dict, repr=False
    )

    def classes(self) -> list[str]:
        return sorted(self.by_class)


# trilogyt materialization caches (`overwrite ds<hash> ... from select`): a
# datasource derived from a query over the model can shortcut paths but can
# never introduce a NEW relation, so it must not create ambiguity alternatives.
# Prototype proxy: the hash-name convention. Real impl: a derived flag on
# overwrite/persist datasources.
_DERIVED_DS = re.compile(r"\bds[0-9a-f]{32,}")


def build_key_graph(benv: BuildEnvironment) -> KeyGraph:
    key_addresses: set[str] = set()
    for concept in benv.concepts.values():
        if concept.purpose == Purpose.KEY and "__preql_internal" not in concept.address:
            key_addresses.add(concept.address)
    for datasource in benv.datasources.values():
        for address in datasource.grain.components:
            if "__preql_internal" not in address:
                key_addresses.add(address)
    parent: dict[str, str] = {a: a for a in key_addresses}
    for address in key_addresses:
        concept = benv.concepts.get(address)
        if concept is None:
            continue
        canonical = concept.canonical_address
        if canonical and canonical in parent:
            _union(parent, address, canonical)
        for pseudonym in concept.pseudonyms:
            if pseudonym in parent:
                _union(parent, address, pseudonym)
    rep = {a: _find(parent, a) for a in key_addresses}
    binders: dict[str, frozenset[str]] = {}
    by_class: dict[str, set[str]] = {}
    for name, datasource in sorted(benv.datasources.items()):
        if _DERIVED_DS.search(datasource.name) or _DERIVED_DS.search(name):
            continue
        classes = frozenset(
            rep[c.address] for c in datasource.output_concepts if c.address in rep
        )
        if not classes:
            continue
        binders[name] = classes
        for cls in classes:
            by_class.setdefault(cls, set()).add(name)
    edges: dict[str, dict[str, frozenset[str]]] = {name: {} for name in binders}
    names = sorted(binders)
    for i, left in enumerate(names):
        for right in names[i + 1 :]:
            shared = binders[left] & binders[right]
            if shared:
                edges[left][right] = shared
                edges[right][left] = shared
    key_closure: dict[str, set[str]] = {}
    for address, cls in rep.items():
        concept = benv.concepts.get(address)
        if concept is None:
            continue
        related = set(concept.keys or ()) | set(concept.grain.components)
        for other in related:
            other_rep = rep.get(other)
            if other_rep is not None and other_rep != cls:
                key_closure.setdefault(cls, set()).add(other_rep)
    return KeyGraph(
        rep=rep,
        binders=binders,
        by_class={cls: frozenset(ds) for cls, ds in by_class.items()},
        edges=edges,
        key_closure={cls: frozenset(v) for cls, v in key_closure.items()},
    )


def connector_sets_from(kg: KeyGraph, source: str) -> dict[str, list[frozenset[str]]]:
    """For every key class reachable from `source`'s binder datasources: the
    distinct connector-key sets over all SHORTEST datasource-hop paths.
    [frozenset()] means co-located. Multi-source BFS from every binder of
    `source`, accumulating hop-label unions along the shortest-path DAG,
    capped at SET_CAP alternatives per node. Endpoint classes are excluded
    from the reported sets by the caller via `_strip`."""
    cached = kg._source_cache.get(source)
    if cached is not None:
        return cached
    starts = kg.by_class.get(source, frozenset())
    dist: dict[str, int] = {ds: 0 for ds in starts}
    order: list[str] = sorted(starts)
    frontier = list(order)
    while frontier:
        nxt: list[str] = []
        for node in frontier:
            for neighbor in kg.edges.get(node, {}):
                if neighbor not in dist:
                    dist[neighbor] = dist[node] + 1
                    order.append(neighbor)
                    nxt.append(neighbor)
        frontier = nxt
    sets: dict[str, set[frozenset[str]]] = {ds: {frozenset()} for ds in starts}
    for node in order:
        if node in starts:
            continue
        collected: set[frozenset[str]] = set()
        for neighbor, shared in kg.edges.get(node, {}).items():
            if dist.get(neighbor) != dist[node] - 1:
                continue
            for s in sets.get(neighbor, ()):
                collected.add(s | shared)
                if len(collected) >= SET_CAP:
                    break
            if len(collected) >= SET_CAP:
                break
        sets[node] = collected
    out: dict[str, list[frozenset[str]]] = {}
    for target, ds_names in kg.by_class.items():
        if target == source:
            continue
        reachable = [ds for ds in ds_names if ds in dist]
        if not reachable:
            continue
        best = min(dist[ds] for ds in reachable)
        alternatives: set[frozenset[str]] = set()
        for ds in reachable:
            if dist[ds] == best:
                alternatives |= sets[ds]
        out[target] = sorted(
            {s - {source, target} for s in alternatives},
            key=lambda s: (len(s), sorted(s)),
        )
    kg._source_cache[source] = out
    return out


def undominated(alternatives: list[frozenset[str]]) -> list[frozenset[str]]:
    """The detect_ambiguity_and_raise rule: clean iff some connector set is a
    subset of every other (that set is the resolution); ambiguous otherwise."""
    for candidate in alternatives:
        if all(candidate <= other for other in alternatives):
            return [candidate]
    return alternatives


def resolve_alternatives(
    kg: KeyGraph, left: str, right: str, alternatives: list[frozenset[str]]
) -> list[frozenset[str]]:
    """The full dominance order. After the subset rule, a route whose connectors
    lie inside an endpoint's OWN key closure (its declared keys / grain — the
    FK-canonical path to that concept) beats routes through incidentally
    co-located keys: reaching `store.name` via `store.sk` is the relation the
    model DECLARED; reaching it via a materialized cache's (item, ticket) slice
    is a planner cost choice, not a second meaning."""
    # MEASURED and rejected: an endpoint-key-closure canonical-route rule.
    # Build-time key inference inherits fact-grain keys (sale_date.sk gets
    # keys {item.sk, ticket_number}), which made BOTH adhoc01 routes look
    # canonical while silently resolving the fixture's genuine ambiguity via
    # inferred (not authored) keys. Derived-datasource exclusion in
    # build_key_graph is the principled fix; only the subset rule remains here.
    return undominated(alternatives)


@dataclass
class AmbiguousPair:
    left: str
    right: str
    alternatives: list[frozenset[str]]


def sweep(benv: BuildEnvironment) -> tuple[KeyGraph, list[AmbiguousPair]]:
    kg = build_key_graph(benv)
    out: list[AmbiguousPair] = []
    for source in kg.classes():
        reachable = connector_sets_from(kg, source)
        for target, alternatives in reachable.items():
            if target <= source:
                continue
            surviving = resolve_alternatives(kg, source, target, alternatives)
            if len(surviving) > 1:
                out.append(AmbiguousPair(source, target, surviving))
    return kg, out


# ---------------------------------------------------------------------------
# model registry / CLI

JOIN_RESOLUTION_MODEL = """
key order_id int;
key store_id int;
key product_id int;
key wh_id int;

property order_id.order_timestamp datetime;
property order_id.order_year int;
property store_id.store_name string;
property product_id.product_name string;
property <wh_id, product_id>.inv_qty int;

datasource orders (
    order_id:order_id,
    store_id:store_id,
    order_timestamp:order_timestamp,
    date_part(order_timestamp, year): order_year,
)
grain (order_id)
address orders;

datasource order_products (
    order_id: order_id,
    product_id:product_id,
)
grain(order_id, product_id)
address order_products;

datasource stores (
    store_id:store_id,
    store_name:store_name,
)
grain (store_id)
address stores;

datasource products (
    product_id:product_id,
    product_name:product_name,
)
grain (product_id)
address products;

datasource inventory (
    wh_id:wh_id,
    product_id:product_id,
    inv_qty:inv_qty,
)
grain (wh_id, product_id)
address inventory;

datasource join_store_warehouse (
    store_id:store_id,
    wh_id:wh_id,
)
grain (store_id, wh_id)
address join_store_warehouse;
"""

MODELS: dict[str, tuple[Path | None, str]] = {
    "join_resolution": (None, JOIN_RESOLUTION_MODEL),
    "tpc_h": (
        REPO / "tests/modeling/tpc_h",
        "import lineitem as lineitem;",
    ),
    "tpc_ds_store": (
        REPO / "tests/modeling/tpc_ds_duckdb",
        "import store_sales as store_sales;",
    ),
    "tpc_ds_multi": (
        REPO / "tests/modeling/tpc_ds_duckdb",
        "import store_sales as store_sales;\n"
        "import catalog_sales as catalog_sales;\n"
        "import inventory as inventory;\n"
        "import item as item;\n"
        "import date as date;",
    ),
    "tpc_ds_all_sales": (
        REPO / "tests/modeling/tpc_ds_duckdb",
        "import all_sales as all_sales;",
    ),
    "hackernews": (
        REPO / "tests/modeling/hackernews",
        "import hackernews as hackernews;\nimport github as github;",
    ),
    "stocks": (
        REPO / "tests/modeling/stocks",
        "import entrypoint as entrypoint;",
    ),
}


def load_build_env(root: Path | None, text: str) -> BuildEnvironment:
    env = Environment(working_path=root) if root else Environment()
    env.parse(text)
    return env.materialize_for_select()


def short(address: str) -> str:
    return address.removeprefix("local.")


def report(name: str, benv: BuildEnvironment) -> None:
    started = time.perf_counter()
    kg, pairs = sweep(benv)
    elapsed = time.perf_counter() - started
    print(
        f"\n=== {name}: {len(kg.binders)} datasources, "
        f"{len(kg.classes())} key classes, {len(pairs)} ambiguous pairs "
        f"({elapsed * 1000:.0f}ms)"
    )
    for pair in pairs[:20]:
        alts = " | ".join(
            "{" + ", ".join(short(a) for a in sorted(s)) + "}"
            for s in pair.alternatives
        )
        print(f"  {short(pair.left)} <-> {short(pair.right)}: {alts}")
    if len(pairs) > 20:
        print(f"  ... {len(pairs) - 20} more")


def main() -> None:
    names = sys.argv[1:] or list(MODELS)
    for name in names:
        root, text = MODELS[name]
        started = time.perf_counter()
        benv = load_build_env(root, text)
        build_ms = (time.perf_counter() - started) * 1000
        print(f"[{name}] materialized in {build_ms:.0f}ms", end="")
        report(name, benv)


if __name__ == "__main__":
    main()
