"""Model-level join-path ambiguity standard.

A pair of key equivalence classes is AMBIGUOUS when no single datasource
co-locates them and the shortest datasource-hop join paths between them carry
incomparable connector-key sets (no set is a subset of all others). This is a
static property of the model's datasource declarations — computable before any
query materializes — so discovery never detects or arbitrates ambiguity: it is
raised here, typed, before the source search runs. A request that itself
carries one alternative's connectors has pinned that path (the forced-join
idiom) and is not ambiguous; a request carrying EVERY alternative's connectors
has pinned them all — the routes compose conjunctively (a fact-to-fact blend
over conformed dimensions) and no silent choice remains.

Two structural rules, both measured against the corpus (2026-07-26):
- A hop between two datasources joins on ALL their shared key classes, so a
  composite-grain snowflake hop is ONE path, not one path per shared key.
- Only ``Purpose.KEY`` concepts are path endpoints. A property reached through
  its own key is that key's pair; letting incidentally co-located property
  columns (a materialized cache slice) become endpoints manufactures
  alternatives the model never declared.
"""

from __future__ import annotations

import weakref
from dataclasses import dataclass, field
from itertools import combinations

from trilogy.core.enums import Purpose
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment

# Alternatives tracked per node during shortest-path traversal; a pair with
# more is reported from the first cap-full set (already ambiguous).
SET_CAP = 8


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
    datasources on all the key classes they share."""

    rep: dict[str, str]
    binders: dict[str, frozenset[str]]
    by_class: dict[str, frozenset[str]]
    edges: dict[str, dict[str, frozenset[str]]]
    _source_cache: dict[str, dict[str, list[frozenset[str]]]] = field(
        default_factory=dict, repr=False
    )


def build_key_graph(benv: BuildEnvironment) -> KeyGraph:
    key_addresses = {
        concept.address
        for concept in benv.concepts.values()
        if concept.purpose == Purpose.KEY and "__preql_internal" not in concept.address
    }
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
    return KeyGraph(
        rep=rep,
        binders=binders,
        by_class={cls: frozenset(ds) for cls, ds in by_class.items()},
        edges=edges,
    )


def connector_sets_from(kg: KeyGraph, source: str) -> dict[str, list[frozenset[str]]]:
    """For every key class reachable from `source`'s binder datasources: the
    distinct connector-key sets over all SHORTEST datasource-hop paths.
    ``[frozenset()]`` means co-located. Multi-source BFS, accumulating
    hop-label unions along the shortest-path DAG, capped at SET_CAP
    alternatives per node."""
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
    """Clean iff some connector set is a subset of every other — that set IS
    the resolution; ambiguous
    otherwise. Longer-than-minimal paths never participate: a strictly longer
    route is a dominated plan, not a second meaning."""
    for candidate in alternatives:
        if all(candidate <= other for other in alternatives):
            return [candidate]
    return alternatives


# BuildEnvironments are built per select; hold a few graphs without pinning
# the environments alive. Keyed by id() with a liveness check because pydantic
# models are not hashable.
_GRAPH_CACHE: dict[int, tuple[weakref.ref[BuildEnvironment], KeyGraph]] = {}
_GRAPH_CACHE_LIMIT = 16


def _graph_for(benv: BuildEnvironment) -> KeyGraph:
    hit = _GRAPH_CACHE.get(id(benv))
    if hit is not None and hit[0]() is benv:
        return hit[1]
    kg = build_key_graph(benv)
    if len(_GRAPH_CACHE) >= _GRAPH_CACHE_LIMIT:
        dead = [key for key, (ref, _) in _GRAPH_CACHE.items() if ref() is None]
        for key in dead:
            del _GRAPH_CACHE[key]
        if len(_GRAPH_CACHE) >= _GRAPH_CACHE_LIMIT:
            _GRAPH_CACHE.clear()
    _GRAPH_CACHE[id(benv)] = (weakref.ref(benv), kg)
    return kg


def validate_relation_paths(
    environment: BuildEnvironment, concepts: list[BuildConcept]
) -> None:
    """Raise for any pair of requested key classes whose minimal join paths
    are incomparable and not pinned by the request itself. Static: answered
    from the model's datasource declarations, never from the source search."""
    kg = _graph_for(environment)
    targets = sorted({kg.rep[c.address] for c in concepts if c.address in kg.rep})
    if len(targets) < 2:
        return
    target_set = set(targets)
    for left, right in combinations(targets, 2):
        alternatives = connector_sets_from(kg, left).get(right)
        if not alternatives:
            continue
        surviving = undominated(alternatives)
        if len(surviving) > 1:
            pinned = [s for s in surviving if s <= target_set]
            # One pinned set is the forced-join idiom. ALL sets pinned means
            # every route's connector keys are themselves requested: each
            # equality holds as a request terminal, the routes compose
            # conjunctively (the fact-to-fact blend over conformed
            # dimensions), and no silent path choice remains.
            if len(pinned) == 1 or len(pinned) == len(surviving):
                continue
            raise AmbiguousRelationshipResolutionException(
                message=(
                    f"Ambiguous join paths between {left} and {right}: "
                    f"either of {[sorted(s) for s in surviving]} could relate "
                    "them. Select a key from the intended path to pin it, "
                    "or restructure the model (separate the routes into "
                    "distinct namespaces, or declare an authored join)."
                ),
                parents=[set(s) for s in surviving],
            )


@dataclass(frozen=True)
class AmbiguousModelPair:
    left: str
    right: str
    alternatives: tuple[frozenset[str], ...]


def sweep_model(environment: BuildEnvironment) -> list[AmbiguousModelPair]:
    """Every ambiguous key-class pair in the model, query-independent — the
    model-level form of the standard, for validation tooling."""
    kg = build_key_graph(environment)
    out: list[AmbiguousModelPair] = []
    for source in sorted(kg.by_class):
        for target, alternatives in connector_sets_from(kg, source).items():
            if target <= source:
                continue
            surviving = undominated(alternatives)
            if len(surviving) > 1:
                out.append(AmbiguousModelPair(source, target, tuple(surviving)))
    return out
