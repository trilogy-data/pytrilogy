"""Prototype B: inject ALL intervening connector-path keys at plan time.

pytest plugin (PYTHONPATH=local_scripts, `pytest -p proto_inject_paths_plugin`).
Wraps v4's `_search_concepts_for_bridge`: for every pair of terminal key
classes, compute the static minimal connector-path alternatives (same engine
as proto_ambiguity). Exactly one undominated set -> inject its keys as
terminals, so the search never faces the choice. Multiple incomparable sets
not pinned by the request itself -> raise the typed ambiguity error BEFORE
any source search runs. Unifies with the existing authored-join /
property-key terminal injection.
"""

from __future__ import annotations

import os
from itertools import combinations

# 1 = raise on incomparable paths but inject nothing (validator-only variant)
VALIDATE_ONLY = os.environ.get("PROTO_VALIDATE_ONLY") == "1"
# bisect modes: "off" = pure passthrough wrapper; "graph" = build key graph
# then passthrough; unset = full analysis
MODE = os.environ.get("PROTO_MODE", "")

from proto_ambiguity import (
    KeyGraph,
    build_key_graph,
    connector_sets_from,
    resolve_alternatives,
)

from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.v4_helper import source_planning

_original = source_planning._search_concepts_for_bridge
# benv id -> (benv strong ref, graph, class rep -> concrete member addresses)
_cache: dict[int, tuple[BuildEnvironment, KeyGraph, dict[str, list[str]]]] = {}
STATS = {"requests": 0, "injected": 0, "raised": 0}


def _graph_for(benv: BuildEnvironment) -> tuple[KeyGraph, dict[str, list[str]]]:
    hit = _cache.get(id(benv))
    if hit is not None and hit[0] is benv:
        return hit[1], hit[2]
    kg = build_key_graph(benv)
    members: dict[str, list[str]] = {}
    for address, rep in kg.rep.items():
        if address in benv.concepts:
            members.setdefault(rep, []).append(address)
    for addresses in members.values():
        addresses.sort()
    _cache[id(benv)] = (benv, kg, members)
    return kg, members


def _patched(request) -> list[BuildConcept]:
    concepts = _original(request)
    if MODE == "off":
        return concepts
    benv = request.environment
    kg, members = _graph_for(benv)
    if MODE == "graph":
        return concepts
    targets = sorted({kg.rep[c.address] for c in concepts if c.address in kg.rep})
    if len(targets) < 2:
        return concepts
    STATS["requests"] += 1
    target_set = set(targets)
    additions: set[str] = set()
    for left, right in combinations(targets, 2):
        alternatives = connector_sets_from(kg, left).get(right)
        if not alternatives:
            continue
        surviving = resolve_alternatives(kg, left, right, alternatives)
        if len(surviving) > 1:
            # the request itself pins a path when it already carries one
            # alternative's connectors (the forced-join idiom)
            pinned = [s for s in surviving if s <= target_set]
            if len(pinned) == 1:
                surviving = pinned
        if len(surviving) > 1:
            STATS["raised"] += 1
            raise AmbiguousRelationshipResolutionException(
                message=(
                    f"Ambiguous join paths between {left} and {right}: "
                    f"{[sorted(s) for s in surviving]}. Add a pinning key or "
                    "restructure the model."
                ),
                parents=[set(s) for s in surviving],
            )
        additions |= surviving[0] - target_set
    if VALIDATE_ONLY or not additions:
        return concepts
    STATS["injected"] += 1
    print(
        f"[proto_inject_paths] targets={targets} injected={sorted(additions)}",
        flush=True,
    )
    out = list(concepts)
    seen = {c.address for c in concepts}
    for rep in sorted(additions):
        for address in members.get(rep, ()):
            if address not in seen:
                out.append(benv.concepts[address])
                seen.add(address)
                break
    return out


def pytest_configure(config) -> None:
    source_planning._search_concepts_for_bridge = _patched


def pytest_unconfigure(config) -> None:
    source_planning._search_concepts_for_bridge = _original


def pytest_terminal_summary(terminalreporter) -> None:
    terminalreporter.write_line(
        f"[proto_inject_paths] multi-terminal requests={STATS['requests']} "
        f"injected={STATS['injected']} raised={STATS['raised']}"
    )
