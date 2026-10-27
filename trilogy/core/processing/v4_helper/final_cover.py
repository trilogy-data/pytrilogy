"""The FINAL merge's cover: which group supplies each requested concept.

Elected from stage-2 facts (group attrs, the group graph and the statement's
extent ownership), never from built nodes, so the join plan is stated over
the contributors that actually merge. See `docs/join_axis_audit_2026_10.md`.
"""

from collections import defaultdict
from collections.abc import Collection, Sequence

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment

from .constants import GROUPING_DERIVATIONS
from .edges import EdgeKind, EdgeMap, edges_of_kind
from .models import ExtentOwnership, GroupAttrs


def exposes(group: GroupAttrs, environment: BuildEnvironment) -> frozenset[str]:
    """What a group's rows present to FINAL: its outputs; a row stream also
    the grain it is computed at, and the key of a property grain (`label`
    keyed by `tree_id`): the columns it carries its rows by."""
    outputs = frozenset(group.output_concepts)
    if group.derivation in GROUPING_DERIVATIONS or group.derivation in (
        Derivation.ROOT,
        Derivation.ROWSET,
    ):
        return outputs
    keys = {
        key
        for address in group.grain_components
        if (c := environment.concepts.get(address)) is not None
        and c.purpose == Purpose.PROPERTY
        for key in c.keys or ()
    }
    return outputs | group.grain_components | keys


def coalescing_mates(environment: BuildEnvironment, address: str) -> frozenset[str]:
    """The other members of the COALESCING key group `address` belongs to.

    Only a coalescing relation (`union`/`full`) fuses its members onto one axis
    that the merge emits under every member's own alias, which is what lets a
    mate answer for the address. A subset or global-merge member keeps its own
    column name, so reading it under the other member's address dangles."""
    coalescing = environment.domain_graph.coalescing_relation_members()
    if address not in coalescing:
        return frozenset()
    for group in environment.scoped_join_relations():
        if address in group:
            return frozenset(group - {address}) & coalescing
    return frozenset()


def output_covers(output: BuildConcept, concept: BuildConcept) -> bool:
    """Whether a node output supplies `concept`, directly or via pseudonym.

    A struct field selected as `unnest_array.a` parses to the bare key
    `local.a` but is produced under its derivable pseudonym `unnest_array.a`;
    the CTE layer maps the two by pseudonym, so coverage matching must too."""
    return (
        output.address == concept.address
        or output.address in concept.pseudonyms
        or concept.address in output.pseudonyms
    )


def elect_cover(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    groups: Collection[str],
    mandatory_list: Sequence[BuildConcept],
    environment: BuildEnvironment,
    ownership: ExtentOwnership,
    rendered: dict[str, frozenset[str]] | None = None,
) -> dict[str, list[BuildConcept]]:
    """For each mandatory concept, the most-downstream of `groups` exposing it
    (more ancestors among `groups` = further downstream). Returns `{gid:
    [concepts that group provides]}` in discovery order, so the merge renders
    with a stable join layout.

    A group carrying the concept's authored scoped-join MATE counts as exposing
    it: the merge coalesces the two members onto one axis, and the mate is
    surfaced under the concept's own address once the merge resolves.

    A ``~``-licensed key comes from its elected owner and nowhere else: that
    group is the one built with permission to manufacture the key's extension
    rows, so any other candidate exposes only the members the fact bound.

    `rendered`, when given, is what each group's built node renders: a group
    stage 2 says exposes a concept its node drops (a recursive CTE claiming
    a column it cannot pass through) is no candidate for it."""
    per_group: dict[str, list[BuildConcept]] = defaultdict(list)
    coalescing = environment.domain_graph.coalescing_relation_members()
    exposed = {gid: exposes(attrs[gid], environment) for gid in groups}
    if rendered is not None:
        # a coalescing axis renders under any of its members' names
        exposed = {
            gid: frozenset(
                a
                for a in exposed[gid]
                if ({a} | coalescing_mates(environment, a)) & rendered[gid]
            )
            for gid in groups
        }
    # a side-channel set feeds no rows: reading a host through one is not
    # reading that host's side
    row_graph = group_graph.copy()
    row_graph.remove_edges_from(edges_of_kind(group_edges, EdgeKind.EXISTENCE))
    for concept in mandatory_list:
        addr = concept.address
        mates = coalescing_mates(environment, addr)
        candidates = [gid for gid in groups if exposed[gid] & (mates | {addr})]
        # Only fall back to pseudonym coverage (struct fields produced under
        # their derivable origin address) when nothing provides the concept
        # directly; a plain alias keeps its own contributor.
        if not candidates:
            candidates = [
                gid
                for gid in groups
                if any(
                    output_covers(c, concept)
                    for address in exposed[gid]
                    if (c := environment.concepts.get(address)) is not None
                )
            ]
        if not candidates:
            continue
        # A COALESCING mate carries only its own side's domain: a group that
        # never read the one hosting the concept itself misses that side's rows.
        if addr in coalescing:
            hosts = {gid for gid in candidates if addr in attrs[gid].members}
            if hosts:
                candidates = [
                    gid
                    for gid in candidates
                    if gid in hosts or hosts & nx.ancestors(row_graph, gid)
                ]
        # A dim peeled off a row stream decorates the stream's keys; its own
        # key carries every entity, not the rows the stream's WHERE kept.
        candidates.sort(
            key=lambda gid: (
                sum(1 for a in nx.ancestors(group_graph, gid) if a in groups),
                addr not in attrs[gid].dim_keys,
                addr in attrs[gid].members,
            ),
            reverse=True,
        )
        winner = candidates[0]
        owner = ownership.owner_of(addr)
        # a group that READ the owner carries its rows too, and more besides
        if (
            owner is not None
            and owner in candidates
            and not any(owner in nx.ancestors(group_graph, gid) for gid in candidates)
        ):
            winner = owner
        per_group[winner].append(concept)
    return per_group
