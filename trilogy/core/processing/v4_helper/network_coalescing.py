"""Stage A's coalescing concerns: presence probes and union-join axis families.

Both exist because a `full`/`union` join makes the unified axis the union of
its members' domains. A cover that reads such an axis off ONE arm silently
drops the other arms' rows, and a probe read off the wrong side answers "did
this side match?" with the other side's row. So membership has to be pinned
per member (the probes) and completeness has to be a property of the whole
cover (the families); neither is discoverable by cover enumeration, which
stops branching the moment one arm binds the class.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.presence_probe import (
    coalescing_axis_group,
    is_presence_probe,
    member_binding_datasources,
    probe_member_address,
)
from trilogy.core.processing.v4_helper.network_model import (
    Binding,
    BindingStrength,
    SourceCandidate,
    datasource_identifiers,
)


def probe_owners(
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    addresses: set[str],
    offered_by: dict[str, set[str]],
    datasource_ids: set[str],
) -> dict[str, frozenset[str]]:
    """A presence probe is pinned to the datasource carrying ITS OWN member.

    Build-time canonical substitution rewrites the probe's lineage argument to
    the key group's canonical, which every member's datasource binds
    identically, so the reference graph offers the probe off BOTH sides, and
    reading it off the wrong one answers "did this side match?" with the other
    side's row. `_datasource_renders_probe` resolves and pins the same carrier,
    so the search must select for it or the two disagree about which scan the
    probe rides."""
    out: dict[str, frozenset[str]] = {}
    for address in addresses:
        if not is_presence_probe(address):
            continue
        member = probe_member_address(address, environment)
        if member is None:
            continue
        # `member_binding_datasources` orders best-presence-population first and
        # the probe node takes candidates[0]; anything else is a different scan.
        carriers = member_binding_datasources(member, datasources)
        carrier_ids = {c.identifier for c in carriers[:1]}
        pinned = carrier_ids & offered_by.get(address, set())
        if not pinned:
            # The graph offers the probe only off the COMPLEMENT side (a
            # dimension spanning the member's whole domain), where a probe
            # read is a tautology and its filter a silent no-op. When the
            # carrier is in this graph, restrict to it anyway;
            # `pin_unoffered_probes` supplies the binding the graph lacks.
            # Only when the carrier is absent is there nothing to pin to; then
            # leave the graph's own binders rather than make the probe
            # unsourceable and lose its filter.
            if carrier_ids & datasource_ids:
                out[address] = frozenset(carrier_ids)
            continue
        out[address] = frozenset(pinned)
    return out


def pin_unoffered_probes(
    addresses: list[str],
    candidates: dict[str, SourceCandidate],
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    equivalence: dict[str, str],
) -> dict[str, SourceCandidate]:
    """Bind a requested presence probe the graph offers off NO candidate to its
    carrier: the datasource physically carrying the member's authored column,
    the same one `_datasource_renders_probe` pins.

    The carrier computes the probe inline (a single-arg COALESCE over a column
    it binds), so the binding is real even though the graph never minted the
    edge. Without it the probe is unreachable, the search declines, and the
    probe's filter silently drops (reads as "no restriction"). Only the
    no-binder case is touched: when the graph offers the probe anywhere,
    `probe_owners` already arbitrates who may carry it.

    Returns a NEW table rather than mutating in place: the injected bindings
    must be visible to every later stage-A step, and returning them makes that
    ordering a data dependency instead of a comment."""
    out = dict(candidates)
    for address in addresses:
        if not is_presence_probe(address):
            continue
        representative = equivalence.get(address, address)
        if any(c.binds(representative) for c in out.values()):
            continue
        member = probe_member_address(address, environment)
        if member is None:
            continue
        carriers = member_binding_datasources(member, datasources)
        if not carriers:
            continue
        carrier_ids = {carriers[0].identifier}
        concept = environment.concepts.get(address)
        # Each argument admits any spelling of its equivalence class: the
        # argument is the key group's CANONICAL (an unnest spine, a rowset
        # output), while the carrier binds the authored member under a
        # `_virt_merge_*` canonical: one class, several addresses.
        arguments: list[frozenset[str]] = []
        for argument in (
            concept.lineage.concept_arguments
            if concept is not None and concept.lineage is not None
            else []
        ):
            spellings = {argument.address, argument.canonical_address}
            argument_concept = environment.concepts.get(argument.address)
            if argument_concept is not None:
                spellings.add(argument_concept.canonical_address)
                spellings |= set(argument_concept.pseudonyms)
            arguments.append(frozenset(equivalence.get(s, s) for s in spellings))
        for node, candidate in sorted(out.items()):
            if candidate.datasource is None:
                continue
            if not datasource_identifiers(candidate.datasource) & carrier_ids:
                continue
            # The carrier must bind the probe's argument to compute it inline.
            bound = frozenset(candidate.bindings)
            if not arguments or not all(spellings & bound for spellings in arguments):
                continue
            bindings = dict(candidate.bindings)
            bindings[representative] = Binding(
                address=address,
                strength=BindingStrength.FULL,
                stored=False,
                injected=True,
            )
            out[node] = replace(candidate, bindings=bindings)
    return out


def _axis_arm_pinned(
    terminals: list[str],
    axis_classes: set[str],
    condition_classes: set[str],
    carrier_grains: list[frozenset[str]],
    address_grain: dict[str, frozenset[str]],
) -> bool:
    """The request's rows are one arm's own: some non-axis OUTPUT terminal
    lives at an arm carrier's row grain, so the axis keys ride that arm's rows
    by design (the arm-scoped aggregate-parent shape) and a downstream assembly
    coalesces the arms. Reading the axis arm-locally is then the request's
    meaning, not a selection defect.

    Grain EQUALITY, not subset: a dimension attribute keyed by one component of
    a composite fact grain is axis enrichment, not arm content. Condition
    columns (and their grain keys) never pin: a filter restricts the axis
    population, it does not redefine the rows as one arm's; a side-pinned
    presence probe's own row key must not turn an axis anti-join into a
    single-arm read."""
    for address in terminals:
        if (
            address in axis_classes
            or address in condition_classes
            or is_presence_probe(address)
        ):
            continue
        grain = address_grain.get(address)
        if not grain:
            continue
        if any(grain == carrier_grain for carrier_grain in carrier_grains):
            return True
    return False


def requested_axis_groups(
    terminals: list[str], environment: BuildEnvironment, equivalence: dict[str, str]
) -> dict[str, set[str]]:
    """Requested coalescing axis classes, mapped to their member addresses."""
    groups: dict[str, set[str]] = {}
    for address in terminals:
        found = coalescing_axis_group(address, environment)
        if found is None:
            continue
        canonical, members = found
        groups[equivalence.get(canonical, canonical)] = set(members)
    return groups


def _row_lineage(concepts: Sequence[BuildConcept], axis: set[str]) -> set[str]:
    """Addresses the outputs read per row of the axis: an aggregate not grouped
    by the axis (a grand total) reads its inputs without their keys."""
    seen: set[str] = set()
    stack = list(concepts)
    while stack:
        concept = stack.pop()
        if concept.address in seen:
            continue
        seen.add(concept.address)
        if concept.lineage is None:
            continue
        if concept.derivation == Derivation.AGGREGATE and not (
            concept.grain.components & axis
        ):
            continue
        stack.extend(concept.lineage.concept_arguments)
    return seen


def _bound_addresses(datasource: BuildDatasource) -> set[str]:
    return {
        address
        for column in datasource.columns
        for address in (column.concept.address, column.concept.canonical_address)
    }


def axis_arms_delivered(
    outputs: Sequence[BuildConcept],
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
) -> bool:
    """Every arm of each projected coalescing axis is some output's own: the
    statement reads (directly or under an aggregate) a column only that arm's
    carriers bind. The contributor reading it brings the arm's keys, so each
    request may read the axis off its own arm (`_axis_arm_pinned`) and the
    assembly coalesces them. An arm no output reads is delivered only by
    assembling the axis where it is requested."""
    groups = requested_axis_groups([c.address for c in outputs], environment, {})
    if not groups:
        return True
    axis = {a for canonical, members in groups.items() for a in {canonical, *members}}
    read = _row_lineage(outputs, axis) - axis
    for members in groups.values():
        bound = {
            member: set().union(
                *(
                    _bound_addresses(carrier)
                    for carrier in member_binding_datasources(member, datasources)
                )
            )
            for member in members
        }
        for member, addresses in bound.items():
            if not addresses:
                continue
            others = set().union(*(v for k, v in bound.items() if k != member))
            if not (addresses - others) & read:
                return False
    return True


def axis_families(
    terminals: list[str],
    candidates: dict[str, SourceCandidate],
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
    equivalence: dict[str, str],
    address_grain: dict[str, frozenset[str]],
    conditions: BuildWhereClause | None,
    arm_local: bool = False,
) -> dict[str, tuple[tuple[str, ...], ...]]:
    """Requested coalescing axis classes that must be family-assembled, mapped
    to per-member carrier candidates.

    A coalescing (`full`/`union` join) declaration makes the unified axis the
    union of the members' domains: its value is the COALESCE of every member's
    own column, so no single arm's scan can bind it fully, and a cover reading
    it off one arm silently drops the other arms' rows. Cover enumeration can
    never discover this on its own (it stops branching the moment one arm
    binds the class), so the family is recorded here as a requirement shape and
    the `axis` obligation asks for the missing arms explicitly.

    A group with a member no candidate carries (a rowset member) is left out:
    the search cannot complete it, and the rowset machinery that owns those
    members already assembles the axis downstream. When the request is
    `arm_local` (an aggregate's input, which the aggregate's consumer
    reassembles), arm-pinned requests are likewise left out (see
    `_axis_arm_pinned`); a statement's own rows never are, since nothing above
    them coalesces the other arms."""
    groups = requested_axis_groups(terminals, environment, equivalence)
    if not groups:
        return {}
    axis_classes = {
        equivalence.get(spelling, spelling)
        for representative, members in groups.items()
        for spelling in {representative, *members}
    }
    condition_classes: set[str] = set()
    if conditions is not None:
        for concept in conditions.row_arguments:
            for address in (concept.address, *concept.grain.components):
                condition_classes.add(equivalence.get(address, address))
    out: dict[str, tuple[tuple[str, ...], ...]] = {}
    for representative, members in sorted(groups.items()):
        family: list[tuple[str, ...]] = []
        for member in sorted(members):
            identifiers = {
                carrier.identifier
                for carrier in member_binding_datasources(member, datasources)
            }
            nodes = tuple(
                node
                for node, candidate in sorted(candidates.items())
                if candidate.datasource is not None
                and datasource_identifiers(candidate.datasource) & identifiers
            )
            if not nodes:
                family = []
                break
            family.append(nodes)
        if not family:
            continue
        carrier_grains = [
            candidates[node].grain
            for nodes in family
            for node in nodes
            if candidates[node].grain
        ]
        if arm_local and _axis_arm_pinned(
            terminals, axis_classes, condition_classes, carrier_grains, address_grain
        ):
            continue
        out[representative] = tuple(family)
    return out


def downgrade_axis_bindings(
    families: dict[str, tuple[tuple[str, ...], ...]],
    candidates: dict[str, SourceCandidate],
) -> dict[str, SourceCandidate]:
    """An arm's binding of a family-required axis class is PARTIAL: it spans
    only its own member's domain. Full is a property of the COVER
    (`axis_complete`), never of one scan."""
    out = dict(candidates)
    for representative in families:
        for node, candidate in out.items():
            binding = candidate.bindings.get(representative)
            if binding is None or binding.partial:
                continue
            bindings = dict(candidate.bindings)
            bindings[representative] = replace(
                binding, strength=BindingStrength.PARTIAL
            )
            out[node] = replace(candidate, bindings=bindings)
    return out


def _reads_only_axis(
    address: str,
    environment: BuildEnvironment,
    axes: set[str],
    equivalence: dict[str, str],
) -> bool:
    # a probe asks "did THIS arm match?": arm-local by design
    if is_presence_probe(address):
        return False
    concept = environment.concepts.get(address) or environment.canonical_concepts.get(
        address
    )
    if concept is None or concept.derivation is not Derivation.BASIC:
        return False
    if concept.lineage is None or not concept.lineage.concept_arguments:
        return False
    for argument in concept.lineage.concept_arguments:
        representative = equivalence.get(argument.address, argument.address)
        if representative in axes:
            continue
        if not _reads_only_axis(argument.address, environment, axes, equivalence):
            return False
    return True


def drop_axis_scalar_bindings(
    axes: set[str],
    candidates: dict[str, SourceCandidate],
    environment: BuildEnvironment,
    equivalence: dict[str, str],
) -> dict[str, SourceCandidate]:
    """A scalar of a requested coalescing axis alone is a function of the
    COALESCED key: one arm's scan computing it inline leaves it NULL on the
    other arms' rows. Unbind it, so it is computed above the arms' merge."""
    if not axes:
        return candidates
    out = dict(candidates)
    for node, candidate in candidates.items():
        dropped = [
            representative
            for representative, binding in candidate.bindings.items()
            if not binding.stored
            and representative not in axes
            and _reads_only_axis(binding.address, environment, axes, equivalence)
        ]
        if dropped:
            bindings = {k: v for k, v in candidate.bindings.items() if k not in dropped}
            out[node] = replace(candidate, bindings=bindings)
    return out
