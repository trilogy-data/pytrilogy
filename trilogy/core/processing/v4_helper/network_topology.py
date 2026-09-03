"""How a set of chosen sources hangs together: which pairs join, whether the
cover is one piece, and which declared relations it actually materializes.

Shared deliberately by the two stages above it. The obligation search demands a
structure and the cost charges for its absence, so a predicate stated twice
would let the search discharge a requirement the cost still charges for. One
definition, two readers.
"""

from __future__ import annotations

from itertools import combinations

from trilogy.core.processing.v4_helper.network_model import SourceNetwork, find, union


def carries(network: SourceNetwork, node: str, keys: frozenset[str]) -> bool:
    """This node reads a declared relation's side: it binds that side's own
    keys."""
    return keys <= network.binding_keys(node)


def materializes(
    network: SourceNetwork, node: str, keys: frozenset[str], canonical: str
) -> bool:
    """This node can PRODUCE the authored equality on that side: it carries the
    side AND the merged key. See `JoinRequirement`.

    One definition on purpose: `unpaired_join_keys` charges for a side this is
    false of and the `paired` obligation demands one this is true of. If the two
    spellings drifted, the search would discharge an obligation the cost still
    charges for."""
    bound = network.binding_keys(node)
    return keys <= bound and canonical in bound


def unpaired_join_keys(network: SourceNetwork, sources: frozenset[str]) -> int:
    """Declared-relation sides the cover sources but never materializes the
    merged key on. See `JoinRequirement`: a side the solution does not touch at
    all imposes nothing, but a side it reads through a carrier that cannot
    produce the key has dropped the authored equality."""
    unpaired = 0
    for requirement in network.join_requirements:
        for keys in requirement.sides():
            if not any(carries(network, node, keys) for node in sources):
                continue
            if not any(
                materializes(network, node, keys, requirement.canonical)
                for node in sources
            ):
                unpaired += 1
    return unpaired


def joined_pairs(
    network: SourceNetwork, sources: frozenset[str]
) -> tuple[tuple[str, str], ...]:
    # Memoized: `_reduce` re-asks per drop candidate of every cover, and the
    # rest-sets repeat heavily across covers.
    cached = network._pair_cache.get(sources)
    if cached is None:
        joined = network._partners()[0]
        cached = tuple(
            (left, right)
            for left, right in combinations(sorted(sources), 2)
            if right in joined[left]
        )
        network._pair_cache[sources] = cached
    return cached


def components(
    network: SourceNetwork, sources: frozenset[str]
) -> tuple[frozenset[str], ...]:
    """Components of the cover under "shares any binding", the same predicate
    `blend_joins`' pair scan reads, taken from the partner table since this
    runs per enumeration state. `union` roots a class at its minimum
    member, so iterating sorted sources yields the components in
    ascending-minimum order. Memoized per source set (see `joined_pairs`)."""
    cached = network._component_cache.get(sources)
    if cached is None:
        parent: dict[str, str] = {node: node for node in sources}
        joined = network._partners()[0]
        pieces = len(sources)
        for node in sorted(sources):
            if pieces == 1:
                break
            for partner in joined[node] & sources:
                if union(parent, node, partner):
                    pieces -= 1
        groups: dict[str, set[str]] = {}
        for node in sorted(sources):
            groups.setdefault(find(parent, node), set()).add(node)
        cached = tuple(frozenset(group) for group in groups.values())
        network._component_cache[sources] = cached
    return cached


def is_connected(network: SourceNetwork, sources: frozenset[str]) -> bool:
    """Pure connectivity CHECK: a disconnected cover is not this search's to
    answer. Every join-justified addition happens as an obligation, including
    multi-hop functional chains, which `labelable` walks (grainless targets get
    an inferred KEY grain so the chain terminus is judgeable). A declined
    request falls to the typed homes: `_direct_source`,
    `_cross_component_source` for lineage-related pieces (the population-safe
    cross product), or the loud disconnected-model error."""
    return len(sources) <= 1 or len(components(network, sources)) == 1


def blend_joins(network: SourceNetwork, sources: frozenset[str]) -> int:
    """Blend edges in a MINIMUM-BLEND SPANNING TREE of the cover.

    Connectivity alone is too weak a requirement: it is satisfied by any shared
    key, so a dimension can attach to the fact side through a 3-valued
    discriminator while the key that identifies it sits unclaimed on another
    candidate. The spanning structure must instead be built out of the keys that
    IDENTIFY rows, and the cost is how many joins are left with no such key.

    Minimised over spanning trees rather than summed over pairs, which is what
    makes it un-launderable: an extra source adds a node the tree must span, so
    it can only lower this by supplying a functional PATH (co-locating the key),
    never by widening the yardstick the way a per-source key-completeness count
    could.

    Memoized per source set (see `joined_pairs`)."""
    cached = network._blend_cache.get(sources)
    if cached is not None:
        return cached
    parent: dict[str, str] = {node: node for node in sources}
    pairs = joined_pairs(network, sources)
    functional = network._partners()[1]
    for left, right in pairs:
        if right in functional[left]:
            union(parent, left, right)
    blends = 0
    for left, right in pairs:
        if find(parent, left) != find(parent, right):
            union(parent, left, right)
            blends += 1
    network._blend_cache[sources] = blends
    return blends
