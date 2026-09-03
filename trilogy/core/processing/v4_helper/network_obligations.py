"""What a partial cover still OWES, in one vocabulary.

Coverage asks whether SOME source binds an address; the correctness invariants
ask whether EVERY chosen source can play its role. Stating both kinds as
obligations lets one search discharge them together; see `ObligationKind` for
the kinds and `network_search._enumerate_covers` for the branching.
"""

from __future__ import annotations

from trilogy.core.processing.v4_helper.network_model import (
    Obligation,
    ObligationKind,
    SourceNetwork,
)
from trilogy.core.processing.v4_helper.network_topology import (
    carries,
    components,
    materializes,
)


def _label_chain_state(
    network: SourceNetwork, source: str, terminal: str, chosen: frozenset[str]
) -> tuple[bool, tuple[str, ...]]:
    """Walk the in-cover functional chains off `source`; discharged when the
    walk reaches a full binder of `terminal`. Otherwise the satisfiers are the
    walk's FRONTIER: out-of-cover candidates one functional hop from any walked
    node that can still complete a chain (they bind the terminal fully, or
    reach a full binder through their own lookup chain). First hops off
    `source` alone are NOT a valid satisfier set: a chain intermediate that
    binds no terminal of its own mints no labelable obligation once chosen, so
    the next hop must come from THIS obligation, or the state strands with
    every satisfier already in the cover and the enumeration emits nothing."""
    # Only row-complete walked nodes originate further hops (`_row_complete`):
    # a row-partial intermediate would narrow the labeled population.
    full = network.full_binders(terminal)
    origins = [source]
    walked = {source}
    stack = [source]
    while stack:
        for node in network.functional_successors(stack.pop()):
            if node in walked or node not in chosen:
                continue
            if node in full:
                return True, ()
            walked.add(node)
            if network.row_complete(node):
                origins.append(node)
                stack.append(node)
    # "Can this node end a chain for the terminal" is cover-independent and
    # usually a small set, so the scan iterates it directly. Sorted, because
    # the satisfier order decides which covers survive truncation, and a
    # sorted subset filters identically to filtering `sorted_candidates` by
    # membership. "One hop from any walked origin" is one set intersection,
    # not a pairwise test per origin.
    completers = network.chain_completers(terminal)
    walked_origins = set(origins)
    frontier = tuple(
        node
        for node in sorted(completers)
        if node not in chosen and network.functional_predecessors(node) & walked_origins
    )
    return False, frontier


def pending_obligations(
    network: SourceNetwork, chosen: frozenset[str]
) -> tuple[Obligation, ...]:
    """Memoized `compute_pending_obligations`. Pure over the immutable network,
    and the same source set is asked about many times: once per enumeration
    state, then again by `_reduce` for every drop candidate of every surviving
    cover. The memo is handed out directly; no caller mutates it."""
    cached = network._obligation_cache.get(chosen)
    if cached is None:
        cached = tuple(compute_pending_obligations(network, chosen))
        network._obligation_cache[chosen] = cached
    return cached


def compute_pending_obligations(
    network: SourceNetwork, chosen: frozenset[str]
) -> list[Obligation]:
    """Every unsatisfied requirement of this cover, each with its satisfiers.

    This is the quantification asymmetry the obligation model resolves:
    coverage asks whether SOME source binds an address, but the structural
    invariants ask whether EVERY chosen source can play its role, questions a
    coverage-only enumeration cannot branch on, because it stops the moment an
    address is bound. Stating both kinds in one vocabulary lets one search
    discharge them together, with every alternative discharge visible to the
    cost order.

    Each obligation is minted only when a satisfier exists. A side no chosen
    source carries imposes nothing; a terminal no candidate could label a
    source's rows with is the request's own shape (a fact-to-fact blend over
    conformed dimensions stays legal); a coalescing axis with a carrier-less
    member is owned by the machinery that materializes that member."""
    out: list[Obligation] = []
    # cover: some source must bind each terminal (partial suffices here; the
    # upgrade to a full binder is a soft branch in `_enumerate_covers`).
    for address in network.terminals:
        if not network.binder_set(address).isdisjoint(chosen):
            continue
        satisfiers = network.binders(address)
        if satisfiers:
            out.append(Obligation(ObligationKind.COVER, (address,), satisfiers))
    # axis: a requested coalescing axis is the union of its members' domains,
    # so every member arm needs a carrier before the class counts as bound.
    for representative, family in sorted(network.axis_families.items()):
        for index, nodes in enumerate(family):
            if not any(node in chosen for node in nodes):
                out.append(
                    Obligation(ObligationKind.AXIS, (representative, str(index)), nodes)
                )
    # paired: a declared relation side the cover carries must materialize the
    # merged key against that side's own keys (see JoinRequirement).
    for requirement in network.join_requirements:
        for keys in requirement.sides():
            if not any(carries(network, node, keys) for node in chosen):
                continue
            if any(
                materializes(network, node, keys, requirement.canonical)
                for node in chosen
            ):
                continue
            satisfiers = tuple(
                sorted(
                    (
                        node
                        for node in network.candidates
                        if materializes(network, node, keys, requirement.canonical)
                    ),
                    # the dimension these keys identify, before any wider scan
                    # that merely happens to carry both
                    key=lambda node: (
                        not network.candidates[node].grain <= keys,
                        node,
                    ),
                )
            )
            if satisfiers:
                out.append(
                    Obligation(
                        ObligationKind.PAIRED,
                        (requirement.canonical, *sorted(keys)),
                        satisfiers,
                    )
                )
    functional = network._partners()[1]
    for source in sorted(chosen):
        candidate = network.candidates[source]
        # labelable: a source contributing terminals must be able to label its
        # OWN rows with each requested terminal, through a single in-cover
        # functional hop, whenever some candidate could supply that lookup.
        # Directional and single-hop: a lookup off the source's own keys can
        # restrict or tag its rows but never multiply them, and the terminal
        # must be bound in the source's own key-class terms.
        bound = network.bound_terminals(source)
        if bound:
            for terminal in network.terminals:
                if terminal in bound:
                    continue
                # `source` completes a chain for a terminal it does not bind
                # exactly when its reach holds a full binder: the precondition
                # for minting the obligation at all, and cover-independent.
                if source not in network.chain_completers(terminal):
                    continue
                labeled, satisfiers = _label_chain_state(
                    network, source, terminal, chosen
                )
                if labeled:
                    continue
                if satisfiers:
                    out.append(
                        Obligation(
                            ObligationKind.LABELABLE, (source, terminal), satisfiers
                        )
                    )
        # colocated: a source none of whose in-cover joins covers its grain is
        # attached by nothing that identifies its rows. When some candidate
        # binds that grain AND joins functionally to another cover member,
        # adding it turns the blend into a lookup; a candidate functional only
        # toward the blended source itself would move the blend, not close it.
        if len(chosen) >= 2 and candidate.grain:
            others = chosen - {source}
            if functional[source].isdisjoint(others):
                satisfiers = tuple(
                    sorted(
                        (
                            extra
                            for extra in network.candidates
                            if extra not in chosen
                            and candidate.grain <= network.binding_keys(extra)
                            and not functional[extra].isdisjoint(others)
                        ),
                        key=lambda extra: (len(network.binding_keys(extra)), extra),
                    )
                )
                if satisfiers:
                    out.append(
                        Obligation(ObligationKind.COLOCATED, (source,), satisfiers)
                    )
    # connected: a cover in pieces must be joined up. As an obligation every
    # alternative bridge enters the cost order, and multi-hop paths (a
    # drill-down chain to a finer-grain terminal) build hop by hop through the
    # fixpoint. Deliberately LAST and only when nothing else is pending:
    # mid-enumeration disconnection is transient (the next coverage binder may
    # connect the pieces), and branching on it there reshapes covers that were
    # never broken. Candidates that directly merge two components are
    # preferred; otherwise any component-adjacent candidate extends a path.
    # Minted only when a satisfier exists; a truly unbridgeable split falls to
    # the typed fallbacks.
    if not out and len(chosen) > 1:
        comps = components(network, chosen)
        if len(comps) > 1:
            adjacency: dict[str, int] = {}
            joined = network._partners()[0]
            for node in network.candidates:
                if node in chosen:
                    continue
                touched = sum(1 for comp in comps if not joined[node].isdisjoint(comp))
                if touched:
                    adjacency[node] = touched
            mergers = tuple(
                sorted(node for node, count in adjacency.items() if count >= 2)
            )
            satisfiers = mergers or tuple(sorted(adjacency))
            if satisfiers:
                subject = tuple(sorted(min(comp) for comp in comps))
                out.append(Obligation(ObligationKind.CONNECTED, subject, satisfiers))
    return prune_subsumed_arms(network, out)


def prune_subsumed_arms(
    network: SourceNetwork, obligations: list[Obligation]
) -> list[Obligation]:
    """Drop a partition arm from a satisfier list that ALSO offers the union
    subsuming it (see `_subsumed_arms` for why the union serves at least as
    well). Applied to every obligation kind here, in one place, so no kind can
    silently miss it.

    Without this the enumeration branches once per arm and once for the union on
    the same obligation, so every subset of N arms becomes a distinct cover that
    `_reduce` then collapses back to the same answer. The enumeration cannot
    see this itself: it branches on satisfiers before any cost is computed.

    The rule keys on the arm's union being offered for the SAME obligation, not
    on arms being worse than unions in general. An arm with no subsuming union
    among the satisfiers is untouched, so an obligation is never left without a
    satisfier and a request only one arm can answer still reaches it."""
    arms = network.subsumed_arms
    if not arms:
        return obligations
    out: list[Obligation] = []
    for obligation in obligations:
        present = set(obligation.satisfiers)
        kept = tuple(
            node
            for node in obligation.satisfiers
            if node not in arms or arms[node] not in present
        )
        out.append(
            obligation
            if len(kept) == len(obligation.satisfiers)
            else Obligation(obligation.kind, obligation.subject, kept)
        )
    return out
