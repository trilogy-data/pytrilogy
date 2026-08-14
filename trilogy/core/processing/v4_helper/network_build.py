"""Stage A: label the datasource/concept network once, from the environment and
the reference graph.

The ONLY module in the network search that reads build models. Everything above
it reasons over addresses, node names and set relations alone, which is what
makes "the search is pure — it selects sources and reports why, but builds no
StrategyNodes" a structural property rather than a promise.

No QUALITATIVE pruning here: a datasource the conditions disqualify is labeled
SENSITIVE, not removed, so the search can explain itself. Join-REACHABILITY is
the one exception (`_relevant_nodes`): a datasource no chain of shared bindings
connects to any requested address can appear in no cover, no obligation and no
explanation, and labeling a wide environment's every scan costs more than the
search it feeds (s66: 168 candidates labeled for a 3-terminal request).
"""

from __future__ import annotations

from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.graph_models import (
    ReferenceGraph,
    datasource_has_filter_sensitive_aggregate,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    condition_implies,
    merge_conditions,
)
from trilogy.core.processing.node_generators.common import (
    relevant_authored_join_pairs,
)
from trilogy.core.processing.node_generators.presence_probe import (
    is_presence_probe,
    member_binding_datasources,
    probe_member_address,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.v4_helper.network_coalescing import (
    axis_families,
    downgrade_axis_bindings,
    pin_unoffered_probes,
    probe_owners,
)
from trilogy.core.processing.v4_helper.network_model import (
    CONNECTOR_NODE_PREFIX,
    Binding,
    BindingStrength,
    ConditionFit,
    JoinRequirement,
    SourceCandidate,
    SourceNetwork,
    datasource_identifiers,
    find,
    node_address,
    union,
)


def _equivalence_map(
    environment: BuildEnvironment,
    addresses: set[str],
    pseudonym_pairs: frozenset[tuple[str, str]] = frozenset(),
) -> dict[str, str]:
    """Collapse pseudonym twins onto one representative so a merged key counts as
    one join axis. Only addresses reachable in this request participate.

    `environment.concepts` alone cannot see a derived merge key's twins: after
    `merge ka into kb` both real addresses carry the surviving side's lineage,
    while each side's own variant lives in the graph under its canonical
    (`_virt_*`) address — the a-side's under `alias_origin_lookup`'s entry. The
    graph's pseudonym edges relate those canonical nodes, so they are passed in
    as extra union pairs; without them the two scans' bindings share no class
    and the sources disconnect."""
    parent: dict[str, str] = {}
    for address in addresses:
        parent.setdefault(address, address)
        concept = environment.concepts.get(address)
        if concept is None:
            continue
        # A concept's own canonical (`_virt_*`) address is the SAME concept in
        # the graph's spelling — a request asks for `ss.is_returned` while the
        # scan's edge emits `ss._virt_comp_*` (q84's terminal that nothing
        # bound). Presence probes are the deliberate exception: the
        # `_virt_presence_*` identity pins side membership and must never
        # collapse onto the `_virt_func_*` class every member binds.
        canonical = concept.canonical_address
        if (
            canonical
            and canonical != address
            and canonical in addresses
            and not is_presence_probe(address)
            and not is_presence_probe(canonical)
        ):
            parent.setdefault(canonical, canonical)
            union(parent, address, canonical)
        for pseudonym in concept.pseudonyms:
            if pseudonym in addresses:
                parent.setdefault(pseudonym, pseudonym)
                union(parent, address, pseudonym)
    for left, right in pseudonym_pairs:
        parent.setdefault(left, left)
        parent.setdefault(right, right)
        union(parent, left, right)
    return {address: find(parent, address) for address in parent}


def _graph_pseudonym_pairs(graph: ReferenceGraph) -> frozenset[tuple[str, str]]:
    return frozenset(
        (node_address(left), node_address(right))
        for left, right in graph.pseudonyms
        if left.startswith("c~") and right.startswith("c~")
    )


def _condition_addresses(conditions: BuildWhereClause) -> set[str]:
    return {c.address for c in conditions.row_arguments}


def _condition_fit(
    datasource: BuildDatasource | BuildUnionDatasource,
    emitted: set[str],
    full: set[str],
    conditions: BuildWhereClause | None,
) -> ConditionFit:
    if conditions is None:
        return ConditionFit.NEUTRAL
    non_partial_for = datasource.non_partial_for
    if non_partial_for is not None and condition_implies(
        conditions.conditional, non_partial_for.conditional
    ):
        return ConditionFit.IMPLIED_EXACT
    if isinstance(
        datasource, BuildDatasource
    ) and datasource_has_filter_sensitive_aggregate(datasource, conditions):
        return ConditionFit.SENSITIVE
    required = _condition_addresses(conditions)
    if not required:
        return ConditionFit.NEUTRAL
    if required <= full and not conditions.existence_arguments:
        return ConditionFit.APPLIES
    if required.isdisjoint(emitted):
        return ConditionFit.UNAFFECTED
    return ConditionFit.DEFERRED


def _emitted_addresses(graph: ReferenceGraph, node: str) -> set[str]:
    """Every address the scan can produce — stored columns plus values it can
    derive inline from complete columns (the graph already carries both as
    datasource edges)."""
    return {
        node_address(neighbor)
        for neighbor in graph.neighbors(node)
        if neighbor.startswith("c~")
    }


def _probe_offers(
    graph: ReferenceGraph, emitted_by_node: dict[str, set[str]]
) -> dict[str, set[str]]:
    """Presence-probe address -> the datasource identifiers this GRAPH offers it
    off. Distinct from who may legitimately carry it: the graph's offer is what
    the search can actually select."""
    out: dict[str, set[str]] = {}
    for node, datasource in graph.datasources.items():
        if node not in emitted_by_node:
            continue
        for address in emitted_by_node[node]:
            if is_presence_probe(address):
                out.setdefault(address, set()).update(
                    datasource_identifiers(datasource)
                )
    return out


def _may_bind(
    datasource: BuildDatasource | BuildUnionDatasource,
    address: str,
    owners: dict[str, frozenset[str]],
) -> bool:
    pinned = owners.get(address)
    if pinned is None:
        return True
    if isinstance(datasource, BuildUnionDatasource):
        return any(child.identifier in pinned for child in datasource.children)
    return datasource.identifier in pinned


def _candidate(
    node: str,
    datasource: BuildDatasource | BuildUnionDatasource,
    emitted: set[str],
    stored: set[str],
    conditions: BuildWhereClause | None,
    equivalence: dict[str, str],
) -> SourceCandidate:
    """Label one scan. The only thing a caller decides is which addresses the
    scan emits and which of those are STORED columns rather than inline
    derivations; everything else follows from the datasource."""
    partial = {concept.address for concept in datasource.partial_concepts}
    return SourceCandidate(
        node=node,
        datasource=datasource,
        bindings=_bindings_for(emitted, partial, stored, equivalence),
        condition=_condition_fit(datasource, emitted, emitted - partial, conditions),
        is_union=isinstance(datasource, BuildUnionDatasource),
        grain=_grain_classes(datasource, equivalence),
    )


def _candidate_for(
    node: str,
    datasource: BuildDatasource | BuildUnionDatasource,
    node_emitted: set[str],
    conditions: BuildWhereClause | None,
    equivalence: dict[str, str],
    owners: dict[str, frozenset[str]],
) -> SourceCandidate | None:
    emitted = {
        address for address in node_emitted if _may_bind(datasource, address, owners)
    }
    if not emitted:
        return None
    return _candidate(
        node,
        datasource,
        emitted,
        stored={column.concept.address for column in datasource.columns},
        conditions=conditions,
        equivalence=equivalence,
    )


def _grain_classes(
    datasource: BuildDatasource | BuildUnionDatasource, equivalence: dict[str, str]
) -> frozenset[str]:
    components = set(datasource.grain.components)
    if not components:
        # A datasource with no DECLARED grain still has a row identity when it
        # binds KEY concepts — infer it, so the directed lookup test
        # (`functional_into`) can judge chains INTO it instead of refusing.
        # Strictly more conservative than the empty grain it replaces: the
        # undirected `joins_functionally` already treats an empty grain as
        # trivially covered by ANY shared key.
        components = {
            column.concept.address
            for column in datasource.columns
            if column.concept.purpose == Purpose.KEY
        }
    return frozenset(equivalence.get(address, address) for address in components)


def _bindings_for(
    emitted: set[str],
    partial: set[str],
    stored: set[str],
    equivalence: dict[str, str],
) -> dict[str, Binding]:
    return {
        equivalence.get(address, address): Binding(
            address=address,
            strength=(
                BindingStrength.PARTIAL if address in partial else BindingStrength.FULL
            ),
            stored=address in stored,
        )
        for address in sorted(emitted)
    }


def _union_candidates(
    terminals: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    equivalence: dict[str, str],
) -> dict[str, SourceCandidate]:
    """A partition family read as one source. Each arm binds the discriminator
    only for its own partition, so only the union binds it fully — without this
    candidate the search would answer a whole-population request from one arm."""
    datasources = [
        datasource
        for datasource in environment.datasources.values()
        if isinstance(datasource, BuildDatasource)
    ]
    out: dict[str, SourceCandidate] = {}
    for group in get_union_sources(datasources, terminals):
        merged = merge_conditions(
            [
                child.non_partial_for.conditional
                for child in group
                if child.non_partial_for is not None
            ]
        )
        union_datasource = BuildUnionDatasource(
            children=group,
            non_partial_for=(
                BuildWhereClause(conditional=merged) if merged is not None else None
            ),
        )
        emitted = {column.concept.address for column in union_datasource.columns}
        if not emitted:
            continue
        node = "ds~" + "-".join(child.name for child in group)
        out[node] = _candidate(
            node,
            union_datasource,
            emitted,
            # a union's columns are the arms' stored columns, so all of it is
            stored=emitted,
            conditions=conditions,
            equivalence=equivalence,
        )
    return out


def _subsumed_arms(candidates: dict[str, SourceCandidate]) -> dict[str, str]:
    """Arm node -> the union candidate whose children include it, for arms that
    are redundant wherever that union is also on offer.

    A union candidate is the same family of columns over the whole partition
    population; an arm is one partition and is row-partial for the family's
    grain. So anywhere an arm can serve — as a coverer, a labeling hop, a
    co-locator — the union serves at least as well, without carrying the arm's
    partiality into the cover.

    Excluded: an arm the request's WHERE implies exactly (`IMPLIED_EXACT`). The
    predicate has already narrowed the request to that arm's own rows, so the
    union is strictly worse there — it re-reads the partitions the filter
    removes. Matching on identifiers, not identity, because the union's children
    are the environment's datasources while the candidates are the graph's."""
    by_identifier: dict[str, str] = {}
    for node, candidate in candidates.items():
        if isinstance(candidate.datasource, BuildDatasource):
            by_identifier[candidate.datasource.identifier] = node
    out: dict[str, str] = {}
    for node, candidate in candidates.items():
        datasource = candidate.datasource
        if not isinstance(datasource, BuildUnionDatasource):
            continue
        for child in datasource.children:
            arm = by_identifier.get(child.identifier)
            if arm is not None and not candidates[arm].condition.partial_is_full:
                out[arm] = node
    return out


def _terminal_addresses(terminals: list[BuildConcept]) -> list[str]:
    """Single-row / abstract-grain concepts join by cross product, never by a
    key, so they must not drive connectivity — sourcing them is a cross join the
    caller adds. Internal addresses are never terminals."""
    return sorted(
        {
            concept.address
            for concept in terminals
            if concept.granularity != Granularity.SINGLE_ROW
            and "__preql_internal" not in concept.address
        }
    )


def _decomposable(
    address: str,
    environment: BuildEnvironment,
    sourced: set[str],
    equivalence: dict[str, str],
    seen: frozenset[str] = frozenset(),
) -> bool:
    """No source emits this address — but if it is an expression over terminals
    the search is ALREADY sourcing, it is computed inline once they are joined,
    so it is not itself a sourcing requirement. The parents must be requested,
    not merely bindable: dropping a derived terminal whose parent nothing asks
    for would lose the requirement instead of relocating it. Only BASIC lineage
    qualifies — an aggregate, a window, a filter or a rowset output is its own
    opaque unit and anchors a join."""
    if address in sourced:
        return True
    if address in seen:
        return False
    if is_presence_probe(address):
        # A presence probe is a deliberately opaque single-arg COALESCE over a
        # coalescing key-group member, and build-time canonical substitution
        # rewrote that argument to the group canonical — which EVERY member's
        # datasource binds identically. So its lineage always looks decomposable
        # and inlining it computes "did this side match?" off whichever source
        # won, which is the exact collapse the probe exists to prevent. It is
        # pinned to its own side by `_datasource_renders_probe`.
        return False
    concept = environment.concepts.get(address)
    if concept is None or concept.derivation is not Derivation.BASIC:
        return False
    lineage = concept.lineage
    if lineage is None:
        return False
    arguments = [
        equivalence.get(c.address, c.address) for c in lineage.concept_arguments
    ]
    return bool(arguments) and all(
        _decomposable(argument, environment, sourced, equivalence, seen | {address})
        for argument in arguments
    )


def connector_join_keys(alias: str, origin: BuildConcept) -> set[str]:
    """The join axis a derived connector must advertise on top of its grain.

    A connector exists to RELATE two sides, so it has to offer an address other
    than the merged key it provides. Nearly every non-BASIC derivation already
    does — an aggregate's grain is its `by`, a window's is its partition — and
    for those this is empty. The row-multiplying family is the exception: parse
    deliberately gives `unnest`/`date_spine` no grain at all (row identity is
    the input's grain times the element, which no Grain can spell), so the
    concept falls back to self-grain and the merge then rewrites that onto the
    merged class. Such a grain says only "I am the merged key": the two sides
    share no binding, the cover disconnects, and the plan degrades to a cross
    join (`ecoregion_info` RIGHT JOIN `trees` on 1=1, every tree paired with
    every ecoregion).

    `keys` is where parse put the input axis, and it survives the canonical
    rewrite the grain did not. Empty for a keyless spine (`unnest([1,2,3])`),
    which has no axis to offer and needs none."""
    provided = {alias, origin.address, origin.canonical_address} | origin.pseudonyms
    if set(origin.grain.components) - provided:
        return set()
    return set(origin.keys or ()) - provided


def _connector_candidates(
    environment: BuildEnvironment, equivalence: dict[str, str]
) -> dict[str, SourceCandidate]:
    """A candidate per derived connector: a merge key whose real lineage is a
    non-BASIC (recursive / aggregate / window) origin in `alias_origin_lookup`,
    while `environment.concepts` holds a demoted lineage-less twin.

    No scan emits such a key (only BASIC lineage computes inline), so the two
    sides it relates share no binding and the cover disconnects — the one
    capability the ladder's Steiner walk had (it traversed lineage edges) that
    datasource↔concept bindings lack. The connector's subplan is real, though:
    `_derived_connector_nodes` materializes the origin carrying the merged key
    AND its own grain keys, and joins on them. This candidate binds exactly
    that contract — the key's equivalence class plus the origin's grain keys —
    so the `connected` obligation can select the connector instead of giving
    up, and the emitter already knows how to render the choice."""
    out: dict[str, SourceCandidate] = {}
    for alias, origin in sorted(environment.alias_origin_lookup.items()):
        if origin.lineage is None or origin.derivation is Derivation.BASIC:
            continue
        provided = {alias, origin.address, origin.canonical_address}
        input_keys = connector_join_keys(alias, origin)
        grain = frozenset(
            equivalence.get(component, component)
            for component in (*origin.grain.components, *input_keys)
        )
        bindings: dict[str, Binding] = {}
        for address in sorted(provided | set(origin.grain.components) | input_keys):
            bindings[equivalence.get(address, address)] = Binding(
                address=alias if address in provided else address,
                strength=BindingStrength.FULL,
                stored=False,
                injected=True,
            )
        if not bindings:
            continue
        node = f"{CONNECTOR_NODE_PREFIX}{alias}"
        out[node] = SourceCandidate(
            node=node,
            datasource=None,
            bindings=bindings,
            condition=ConditionFit.NEUTRAL,
            is_union=False,
            grain=grain,
        )
    return out


def _join_requirements(
    terminals: list[BuildConcept],
    environment: BuildEnvironment,
    equivalence: dict[str, str],
) -> tuple[JoinRequirement, ...]:
    """The declared relations this request actually traverses, in the search's
    address space. Same source of truth as the ladder's terminal injection, so
    the two agree on which relations need discovery help at all."""
    out: list[JoinRequirement] = []
    for pair in relevant_authored_join_pairs(terminals, environment):
        canonical = equivalence.get(pair.canonical.address, pair.canonical.address)
        keys = []
        for member in (pair.left, pair.right):
            resolved = set()
            for address in member.keys or ():
                key = environment.concepts.get(address)
                own = key.address if key is not None else address
                resolved.add(equivalence.get(own, own))
            keys.append(frozenset(resolved))
        out.append(
            JoinRequirement(canonical=canonical, left_keys=keys[0], right_keys=keys[1])
        )
    return tuple(out)


def _address_grains(
    environment: BuildEnvironment,
    addresses: set[str],
    equivalence: dict[str, str],
) -> dict[str, frozenset[str]]:
    out: dict[str, frozenset[str]] = {}
    for address in addresses:
        concept = environment.concepts.get(address)
        if concept is None:
            continue
        out[equivalence.get(address, address)] = frozenset(
            equivalence.get(component, component)
            for component in concept.grain.components
        )
    return out


def _relevant_nodes(
    graph: ReferenceGraph,
    emitted_by_node: dict[str, set[str]],
    addresses: list[str],
    environment: BuildEnvironment,
    equivalence: dict[str, str],
    extra_sets: list[frozenset[str]],
) -> set[str]:
    """Datasource nodes some cover could contain: the join-components of the
    pool touching a requested address, over CANONICAL emitted addresses —
    computed before labeling, because a candidate's binding keys are exactly
    its canonicalized emitted addresses (minus probe-ownership removals), so
    address-reachability over this bipartite graph over-approximates every
    join any cover could make. `extra_sets` carries the union/connector
    candidates' binding keys — a derived connector can bridge scans that share
    no address. Presence-probe carriers are seeded by node: their binding is
    INJECTED by `pin_unoffered_probes`, never emitted by the graph."""
    canonical: dict[str, set[str]] = {
        node: {equivalence.get(a, a) for a in emitted}
        for node, emitted in emitted_by_node.items()
    }
    address_nodes: dict[str, set[str]] = {}
    for node, canon in canonical.items():
        for address in canon:
            address_nodes.setdefault(address, set()).add(node)
    carrier_ids: set[str] = set()
    for address in addresses:
        if not is_presence_probe(address):
            continue
        member = probe_member_address(address, environment)
        if member is None:
            continue
        carrier_ids.update(
            c.identifier for c in member_binding_datasources(member, environment)
        )
    stack: list[str] = [
        node
        for node, datasource in graph.datasources.items()
        if node in canonical
        and carrier_ids.intersection(datasource_identifiers(datasource))
    ]
    # bridge sets are join EDGES, not selectable nodes: reaching any address of
    # one reaches them all
    bridges = [set(extra) for extra in extra_sets]
    seen_addresses: set[str] = set()
    frontier = {equivalence.get(a, a) for a in addresses}
    included: set[str] = set()
    while stack or frontier:
        while frontier:
            address = frontier.pop()
            if address in seen_addresses:
                continue
            seen_addresses.add(address)
            stack.extend(address_nodes.get(address, ()))
            for bridge in bridges:
                if address in bridge:
                    frontier |= bridge - seen_addresses
        while stack:
            node = stack.pop()
            if node in included:
                continue
            included.add(node)
            frontier |= canonical[node] - seen_addresses
            if frontier:
                break
    return included


def build_source_network(
    terminals: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    conditions: BuildWhereClause | None = None,
) -> SourceNetwork:
    addresses = _terminal_addresses(terminals)
    all_addresses = set(addresses)
    # The one place a node's emitted set is derived; `_probe_offers` and
    # `_candidate_for` read it rather than re-walking the graph's neighbors
    # (three walks per node otherwise, and the graph does not change here).
    emitted_by_node: dict[str, set[str]] = {}
    for node in graph.datasources:
        if node in graph:
            emitted_by_node[node] = _emitted_addresses(graph, node)
            all_addresses |= emitted_by_node[node]
    equivalence = _equivalence_map(
        environment, all_addresses, _graph_pseudonym_pairs(graph)
    )
    owners = probe_owners(
        environment,
        all_addresses,
        _probe_offers(graph, emitted_by_node),
        {
            identifier
            for node, datasource in graph.datasources.items()
            if node in graph
            for identifier in datasource_identifiers(datasource)
        },
    )
    union_candidates = {
        node: union_candidate
        for node, union_candidate in _union_candidates(
            terminals, environment, conditions, equivalence
        ).items()
        if not union_candidate.condition.disqualifying
    }
    connector_candidates = _connector_candidates(environment, equivalence)
    relevant = _relevant_nodes(
        graph,
        emitted_by_node,
        addresses,
        environment,
        equivalence,
        [
            frozenset(candidate.bindings)
            for table in (union_candidates, connector_candidates)
            for candidate in table.values()
        ],
    )
    candidates: dict[str, SourceCandidate] = {}
    for node, datasource in sorted(graph.datasources.items()):
        if node not in relevant:
            continue
        candidate = _candidate_for(
            node, datasource, emitted_by_node[node], conditions, equivalence, owners
        )
        if candidate is not None and not candidate.condition.disqualifying:
            candidates[node] = candidate
    for node, union_candidate in union_candidates.items():
        candidates.setdefault(node, union_candidate)
    for node, connector in connector_candidates.items():
        candidates.setdefault(node, connector)
    candidates = pin_unoffered_probes(addresses, candidates, environment, equivalence)
    bound = {address for c in candidates.values() for address in c.bindings}
    requested = [equivalence.get(a, a) for a in addresses]
    sourced = {address for address in requested if address in bound}
    searched = [
        address
        for address in requested
        if address in sourced
        or not _decomposable(address, environment, sourced, equivalence)
    ]
    address_grain = _address_grains(environment, all_addresses, equivalence)
    families = axis_families(
        searched, candidates, environment, equivalence, address_grain, conditions
    )
    candidates = downgrade_axis_bindings(families, candidates)
    return SourceNetwork(
        terminals=tuple(searched),
        candidates=candidates,
        equivalence=equivalence,
        address_grain=address_grain,
        join_requirements=_join_requirements(terminals, environment, equivalence),
        axis_families=families,
        subsumed_arms=_subsumed_arms(candidates),
    )
