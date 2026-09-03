"""v4-native source network search.

One labeled search over the datasource/concept network. Partiality and
condition fit are EDGE LABELS the single search reasons about, not global modes
chosen before searching, so no phase can commit to a source through an edge a
later phase discards.

Model: sourcing a request is weighted set cover with join connectivity, solved
as an OBLIGATION-DRIVEN search. Coverage ("some source binds this terminal") is
only one obligation kind; the correctness invariants quantify per source and
per relation, so choosing a source can create further obligations: its rows
must be labelable with every requested value that some in-cover lookup could
supply, a declared relation whose side it carries must be materialized on that
side, its grain must be co-locatable when only non-functional joins reach it,
and a coalescing axis it binds is only complete once every member arm has a
carrier. The search branches on each pending obligation's satisfiers until none
remain, so every way of discharging a requirement is a candidate solution and
the cost order, not greedy repair order, picks among them. Obligations are
monotone (adding a source never un-discharges one) and only minted when a
satisfier exists (a requirement nothing could satisfy is the request itself,
not a defect), which is what keeps a fact-to-fact blend with no finer key
legal. Ambiguity is not this search's concern: incomparable model join paths
are a typed error raised by `model_ambiguity.validate_relation_paths` before
any search runs, so the search always picks the cost-order winner.

This file is stages B and C: enumerate, reduce, cost, choose. The vocabulary
is `network_model`, the labeling is `network_build` (the only module that reads
build models), the shared structure questions are `network_topology`, and the
requirements are `network_obligations`.

See docs/v4_network_discovery_design.md. The search is pure: it selects sources
and reports why, but builds no StrategyNodes.
"""

from __future__ import annotations

from _preql_import_resolver import enumerate_network_covers

from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.v4_helper.network_build import build_source_network
from trilogy.core.processing.v4_helper.network_model import (
    COVER_LIMIT,
    STATE_LIMIT,
    SearchLimit,
    SearchResult,
    SolutionCost,
    SourceCandidate,
    SourceNetwork,
    SourceSolution,
)
from trilogy.core.processing.v4_helper.network_obligations import pending_obligations
from trilogy.core.processing.v4_helper.network_topology import (
    blend_joins,
    components,
    is_connected,
    joined_pairs,
    unpaired_join_keys,
)


def _enumerate_covers(
    network: SourceNetwork,
) -> tuple[list[frozenset[str]], SearchLimit | None]:
    """The enumeration walk, run in Rust (`network_search.rs` in the parser
    crate): the per-state machinery is bitset arithmetic there, while the
    network's labels cross the boundary once per search as plain strings and
    bools. `_enumerate_covers_py` is the executable spec the port is held to:
    semantics live in its docstring, and the parity test in
    `test_v4_network_search.py` pins the two together. The budget globals are
    read at call time so test monkeypatching keeps working."""
    covers, limit = enumerate_network_covers(
        list(network.terminals),
        [
            (
                node,
                [
                    (address, binding.partial)
                    for address, binding in candidate.bindings.items()
                ],
                list(candidate.grain),
                candidate.condition.partial_is_full,
            )
            for node, candidate in network.candidates.items()
        ],
        [
            (representative, [list(nodes) for nodes in family])
            for representative, family in network.axis_families.items()
        ],
        [
            (
                requirement.canonical,
                list(requirement.left_keys),
                list(requirement.right_keys),
            )
            for requirement in network.join_requirements
        ],
        list(network.subsumed_arms.items()),
        COVER_LIMIT,
        STATE_LIMIT,
    )
    return (
        [frozenset(cover) for cover in covers],
        SearchLimit(limit) if limit else None,
    )


def _enumerate_covers_py(
    network: SourceNetwork,
) -> tuple[list[frozenset[str]], SearchLimit | None]:
    """One pass, obligation-driven: branch on the scarcest pending obligation's
    satisfiers until none remain, then emit the cover. Coverage and the
    structural invariants are discharged by the same machinery, so every
    alternative discharge (which carrier, which side hop, which co-locator)
    becomes a distinct emitted cover for the cost order to judge.

    A terminal covered only partially is a discharged obligation (the cover is
    an answer) AND a soft branch point: a candidate binding it fully avoids the
    completion join, so that cover is emitted too and the partiality and
    completion cost axes decide between them. Termination: every branch adds a
    source, obligations are monotone, and the candidate pool is finite. The
    same source set reached along different discharge orders is one state.

    The walk is LEVEL-ORDER over state size, with in-walk dominance: a state
    containing an already-emitted cover with an identical binding profile is a
    tower `_reduce` would only strip back down, so it is not expanded. Level
    order is what arms the prune: every k-source cover is emitted before any
    (k+1)-source state pops, so a dominated state always sees its dominator.
    Profile inequality keeps every upgrade path alive: a soft-branch state
    binds a terminal MORE fully than the cover it extends, so it never reads
    as dominated by it."""
    targets = list(network.terminals)
    covers: list[frozenset[str]] = []
    emitted: list[tuple[frozenset[str], dict[str, int]]] = []
    visited: set[frozenset[str]] = set()
    level: list[frozenset[str]] = [frozenset()]
    while level:
        # Within a level, first-pushed pops first: the push order fixes which
        # covers survive truncation, so it must stay deterministic.
        next_level: list[frozenset[str]] = []
        for chosen in level:
            if chosen in visited:
                continue
            if len(covers) >= COVER_LIMIT:
                return covers, SearchLimit.COVERS
            if len(visited) >= STATE_LIMIT:
                return covers, SearchLimit.STATES
            visited.add(chosen)
            if emitted:
                profile = _binding_profile(network, chosen, targets)
                if any(
                    prior < chosen and profile == prior_profile
                    for prior, prior_profile in emitted
                ):
                    continue
            pending = pending_obligations(network, chosen)
            if pending:
                # Scarcest obligation first: the smallest branch factor, and a
                # deterministic discharge order.
                first = min(pending, key=lambda o: (len(o.satisfiers), o.identity))
                for node in first.satisfiers:
                    next_level.append(chosen | {node})
                continue
            # No dedupe needed: `visited` already admits each state exactly once.
            covers.append(chosen)
            emitted.append((chosen, _binding_profile(network, chosen, targets)))
            for address in network.terminals:
                full = network.full_binders(address)
                if full & chosen:
                    continue
                # `binders` is sorted; see the determinism note above.
                for node in network.binders(address):
                    if node in full:
                        next_level.append(chosen | {node})
        level = next_level
    return covers, None


def _bound_level(network: SourceNetwork, sources: frozenset[str], address: str) -> int:
    """How well this cover binds one address: 2 fully, 1 partially, 0 not at
    all. A family-required coalescing axis is full only once EVERY member arm
    has a carrier (`axis_complete`), never off one arm's scan, which would
    silently drop the other arms' rows."""
    if address in network.axis_families:
        if network.axis_complete(sources, address):
            return 2
    elif network.full_binders(address) & sources:
        return 2
    if any(network.candidates[node].binds(address) for node in sources):
        return 1
    return 0


def _binding_profile(
    network: SourceNetwork, sources: frozenset[str], targets: list[str]
) -> dict[str, int]:
    """Two covers with the same profile answer the request identically.

    Memoized per source set: `_reduce` re-asks for the same rest-sets across
    covers. Callers only compare profiles, never mutate them."""
    key = (sources, tuple(targets))
    cached = network._profile_cache.get(key)
    if cached is None:
        cached = {
            address: _bound_level(network, sources, address) for address in targets
        }
        network._profile_cache[key] = cached
    return cached


def _reduce(
    network: SourceNetwork, sources: frozenset[str], targets: list[str]
) -> frozenset[str]:
    """Drop every source the rest of the cover makes redundant. A source that
    binds nothing the others do not already bind, and is not holding the cover
    together, contributes only its join, which can restrict rows (an inner join
    onto a narrower population) or fan them out. That is a wrong-rows change, not
    a costlier plan, so a non-minimal cover is INVALID rather than dominated.

    "Binds nothing the others do not" is a claim about VALUES, and a source can
    be load-bearing without providing one: as the only carrier materializing a
    declared key on its side, or the only lookup labeling another source's
    rows. A drop is therefore refused when the remainder answers the request
    differently (binding profile), disconnects, OPENS an obligation (the same
    invariants the search built the cover to satisfy; the full cover has none
    open, since `_enumerate_covers` only emits a state once its pending set is
    empty), or trades a functional join for a blend nothing can repair."""
    profile = _binding_profile(network, sources, targets)
    blends = blend_joins(network, sources)
    current = set(sources)
    # Try the least valuable source first: an IMPLIED_EXACT source is the
    # pre-filtered read of exactly the requested rows, so when two sources are
    # mutually redundant it is the one to keep.
    for node in sorted(
        sources,
        key=lambda n: (network.candidates[n].condition.partial_is_full, n),
    ):
        if len(current) <= 1:
            break
        rest = frozenset(current - {node})
        if _binding_profile(network, rest, targets) != profile:
            continue
        if len(components(network, rest)) > 1:
            continue
        if pending_obligations(network, rest):
            continue
        if blend_joins(network, rest) > blends:
            continue
        current = set(rest)
    return frozenset(current)


def _assign(
    network: SourceNetwork, sources: frozenset[str], targets: list[str]
) -> dict[str, frozenset[str]]:
    """Give each terminal to a source that binds it: a full binding first, then
    one that reads it at its own grain rather than fanning it out, then a stable
    name: the provider choice must not depend on iteration order, and the
    solution's cost must not depend on an arbitrary provider pick."""
    assignments: dict[str, set[str]] = {node: set() for node in sources}
    for address in targets:
        full = network.full_binders(address)
        options = sorted(
            (node for node in sources if network.candidates[node].binds(address)),
            key=lambda node: (
                node not in full,
                network.fans_out(node, frozenset({address})),
                node,
            ),
        )
        if options:
            assignments[options[0]].add(address)
    return {node: frozenset(addresses) for node, addresses in assignments.items()}


def _stored_key(candidate: SourceCandidate, address: str) -> bool:
    binding = candidate.bindings.get(address)
    return binding is not None and binding.stored


def _solution_for(
    network: SourceNetwork, sources: frozenset[str], targets: list[str]
) -> SourceSolution:
    assignments = _assign(network, sources, targets)
    join_keys: dict[tuple[str, str], frozenset[str]] = {}
    connectors: set[str] = set()
    derived_joins = 0
    target_set = set(targets)
    for left, right in joined_pairs(network, sources):
        keys = network.join_keys(left, right)
        join_keys[(left, right)] = keys
        connectors |= keys - target_set
        for key in keys:
            # A link key is never a stored column on either side (it is reached
            # through a connector subplan), so it always counts as a derived join.
            if not all(
                _stored_key(network.candidates[node], key) for node in (left, right)
            ):
                derived_joins += 1
    partial_terminals = {
        address for address in targets if _bound_level(network, sources, address) < 2
    }
    completions = {
        address
        for address in partial_terminals
        # A family-required axis is always completable: its carriers exist.
        if address in network.axis_families or network.full_binders(address)
    }
    joined_on = {
        node: frozenset(
            key
            for (left, right), keys in join_keys.items()
            if node in (left, right)
            for key in keys
        )
        for node in sources
    }
    cost = SolutionCost(
        unpaired_join_keys=unpaired_join_keys(network, sources),
        partial_terminals=len(partial_terminals),
        completions=len(completions),
        blend_joins=blend_joins(network, sources),
        # A source that provides nothing survived reduction only as a bridge, so
        # its join keys ARE its contribution and are the right yardstick.
        fanout_sources=sum(
            1
            for node in sources
            if network.fans_out(node, assignments[node] or joined_on[node])
        ),
        sources=len(sources),
        connectors=len(connectors),
        derived_joins=derived_joins,
    )
    return SourceSolution(
        sources=tuple(sorted(sources)),
        assignments=assignments,
        join_keys=join_keys,
        partial_terminals=frozenset(partial_terminals),
        completions=frozenset(completions),
        connectors=frozenset(connectors),
        cost=cost,
    )


def _split_terminals(network: SourceNetwork, targets: list[str]) -> frozenset[str]:
    """Terminals provably un-co-locatable: every emitted cover must be one
    join-component (`search_sources` discards the rest), and a cover's joins
    are a subgraph of the candidate pool's, so when no single component of the
    WHOLE pool holds a binder for every terminal, no connected cover exists.
    Returned as the best component's missing terminals, so a request over two
    unmergeable fact aliases is decided in one union-find pass instead of a
    full state-budget walk.

    A certificate, not a heuristic: empty means only that this proof does not
    apply, never that a solution exists."""
    pool = components(network, frozenset(network.candidates))
    if len(pool) <= 1:
        return frozenset()
    best: frozenset[str] | None = None
    for comp in pool:
        missing = frozenset(
            address
            for address in targets
            if network.binder_set(address).isdisjoint(comp)
        )
        if not missing:
            return frozenset()
        if best is None or len(missing) < len(best):
            best = missing
    return best or frozenset()


def _seed_cover(network: SourceNetwork, targets: list[str]) -> frozenset[str] | None:
    """Top-down fallback: reduce a terminal-covering pool component straight
    to a minimal cover, without walking. Consulted only when the walk found
    nothing (so a truncated budget is not a dead end) and never competing with
    the walk's own solutions: a top-down reduction is one drop-order local
    minimum, not the cost-order winner.

    Validated by the walk's own emit standard (no pending obligations, one
    component): obligations are INVISIBLE at the top of the lattice
    (satisfiers are defined as additions, and with everything chosen there is
    nothing to add), so an unvalidated reduction can keep a stranded
    labelable the walk would never emit. `pending_obligations` on the reduced
    set sees the frontier again and catches exactly that."""
    pool = components(network, frozenset(network.candidates))
    for comp in pool:
        if any(network.binder_set(a).isdisjoint(comp) for a in targets):
            continue
        seed = _reduce(network, comp, targets)
        if pending_obligations(network, seed):
            continue
        if not is_connected(network, seed):
            continue
        return seed
    return None


def search_sources(network: SourceNetwork) -> SearchResult:
    """Stages B + C: discharge obligations, connect, reduce, take the
    cost-order winner. The lexicographic minimum is always non-dominated, so
    no frontier is kept; incomparable MODEL paths were rejected before the
    search by `model_ambiguity.validate_relation_paths`."""
    targets = list(network.terminals)
    unreachable = frozenset(a for a in targets if not network.binders(a))
    if unreachable:
        return SearchResult(unreachable=unreachable)
    split = _split_terminals(network, targets)
    if split:
        return SearchResult(split=split)
    covers, limit = _enumerate_covers(network)
    solutions: list[SourceSolution] = []
    seen: set[tuple[str, ...]] = set()
    # (reduced solution, its profile): a later cover that CONTAINS a reduced
    # solution and answers the request identically only re-derives it (the
    # extra sources are exactly what `_reduce` exists to strip), so it is
    # skipped before paying the reduction. The enumeration emits every
    # alternative discharge, which on a wide join graph is mostly supersets of
    # the same few minimal covers.
    reduced: list[tuple[frozenset[str], dict[str, int]]] = []
    for cover in covers:
        if not is_connected(network, cover):
            continue
        profile = _binding_profile(network, cover, targets)
        if any(
            prior <= cover and profile == prior_profile
            for prior, prior_profile in reduced
        ):
            continue
        connected = _reduce(network, cover, targets)
        key = tuple(sorted(connected))
        if key in seen:
            continue
        seen.add(key)
        reduced.append((connected, _binding_profile(network, connected, targets)))
        solutions.append(_solution_for(network, connected, targets))
    if not solutions:
        # Probe top-down only when the walk came up empty (typically budget
        # truncation): a validated seed is a concrete answer where the
        # exhausted fall-through can only guess. Lazy on purpose: probing up
        # front costs a full-component `_reduce` per search, more than the
        # walk it would prune on a healthy request. Reported as truncated when
        # the budget was hit, so `_report_truncation` still says the solution
        # may not be cost-minimal.
        seed = _seed_cover(network, targets)
        if seed is not None:
            return SearchResult(
                solution=_solution_for(network, seed, targets), limit=limit
            )
        return SearchResult(limit=limit)
    best = min(solutions, key=lambda s: (s.cost.axes(), s.sources))
    return SearchResult(solution=best, limit=limit)


def plan_network_sources(
    terminals: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    conditions: BuildWhereClause | None = None,
) -> SearchResult:
    return search_sources(
        build_source_network(terminals, environment, graph, conditions)
    )
