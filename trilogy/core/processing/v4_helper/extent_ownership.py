"""Graph-time election of who owns each ``~``-licensed extension span.

A ``~`` binding licenses domain extension: unmatched members of that key's
dimension enter the result once, with everything outside the key's closure
NULL. Nothing in the plan says WHERE those rows come from, so the election
decides it before any node is built: one group per span carries the extension
members, that group's ancestors may pad on the way to it, and every other group
is extent-free, so its joins pair on solid keys and its rows reach the output
through the owner. Without a single owner each branch touching the key pads
its own copy, and the FINAL assembly must either reunite copies null-safely or
discard one through a plain equality.

The ranking (most downstream, then primary membership, then joint coverage of
every span) is the same judgment ``_cover_groups_for_mandatory`` applies to
already-built nodes. The cover consumes this result rather than re-deriving
it, since a predicted election that diverges from the actual one leaves a
contributor dangling at render time.
"""

from __future__ import annotations

from collections.abc import Iterable

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.models.build_environment import BuildEnvironment

from .constants import FINAL_NODE_ID, ROW_STREAM_DERIVATIONS
from .functional_dependency import build_fd_determines
from .models import ExtentOwnership, GroupAttrs, Keyspace, Region


def span_members(
    span: str, addresses: Iterable[str], environment: BuildEnvironment
) -> list[str]:
    """The addresses an extension row of `span` carries: the key itself and
    whatever it functionally determines."""
    return [
        address
        for address in addresses
        if address == span
        or build_fd_determines(environment, {span}, address, include_empty_grain=False)
    ]


def elect_extent_owners(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    spans: frozenset[str],
    keyspace: Keyspace | None = None,
) -> ExtentOwnership:
    """`spans` are the ones the statement asks extension rows of
    (`Keyspace.output_demanded_spans`). A `~` FK that only shows up as a join
    axis is not among them, and with none the whole mechanism is inert (the
    common case: TPC-DS and TPC-H rarely demand one)."""
    keyspace = keyspace or Keyspace()
    # a region with a domain group of its own is demanded by that alone: a
    # derivation absent on it is an output no lookup from the span reaches
    domains = {
        gid: region
        for gid, a in attrs.items()
        if a.extent_spans and (region := keyspace.region_of(a.extent_spans))
    }
    spans = spans.union(*(r.spans for r in domains.values()))
    if not spans:
        return ExtentOwnership()
    exposes: dict[str, frozenset[str]] = {}
    for gid, attr in attrs.items():
        if gid == FINAL_NODE_ID:
            continue
        owned = spans & set(attr.output_concepts)
        if owned:
            exposes[gid] = frozenset(owned)
    if not exposes:
        return ExtentOwnership()

    def rank(gid: str) -> tuple[int, int, int, str]:
        # Most downstream wins: its rows have already absorbed everything
        # upstream, so routing extent there keeps one copy rather than one per
        # branch. Ties break toward the group that OWNS the key (primary
        # membership, the dimension span) over one merely carrying it as a
        # join column, then on id for determinism.
        attr = attrs[gid]
        primary = set(attr.primary_members) | set(attr.secondary_members)
        return (
            len(nx.ancestors(group_graph, gid)),
            len(exposes[gid] & primary),
            len(exposes[gid]),
            gid,
        )

    # Only a span some group actually delivers can be routed. One nobody
    # exposes (a transitive dimension's key reached purely through a join)
    # stays unmanaged and keeps per-branch padding, because suppressing what
    # has no owner deletes the extension rows outright.
    ownable: frozenset[str] = frozenset().union(*exposes.values())
    owner_by_span: dict[str, str] = {}
    # A group exposing every ownable span keeps the families together: split
    # ownership manufactures the same extension member in two branches, which
    # the FINAL merge can only reunite by pairing padding null-safely.
    joint = [gid for gid, owned in exposes.items() if owned == ownable]
    if joint:
        winner = max(joint, key=rank)
        owner_by_span = {span: winner for span in ownable}
    else:
        for span in sorted(ownable):
            candidates = [gid for gid, owned in exposes.items() if span in owned]
            owner_by_span[span] = max(candidates, key=rank)

    # A region with a domain group of its own is sourced there and nowhere else.
    domain_of_span = {
        span: gid for gid, region in domains.items() for span in region.spans & ownable
    }
    owner_by_span.update(domain_of_span)

    permitted: dict[str, frozenset[str]] = {}
    for span, owner in owner_by_span.items():
        if span in domain_of_span:
            # The domain holds the members; whatever reads it extends, except
            # the row streams that must never see an extension row.
            allowed = ({owner} | nx.descendants(group_graph, owner)) - _solid_groups(
                group_graph, attrs, domains[owner], keyspace
            )
        else:
            allowed = {owner} | nx.ancestors(group_graph, owner)
        for gid in allowed - {FINAL_NODE_ID}:
            permitted[gid] = permitted.get(gid, frozenset()) | {span}
    carried = {
        address: gid for gid in domains for address in attrs[gid].primary_members
    }
    return ExtentOwnership(
        spans=ownable,
        owner_by_span=owner_by_span,
        permitted=permitted,
        carried=carried,
    )


def _solid_groups(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    region: Region,
    keyspace: Keyspace,
) -> set[str]:
    """Groups that must not see `region`'s rows: each row-stream derivation
    absent there, and the row stream feeding it. A derived concept is NULL
    where its key's entity is absent, which it can only be if it never reads a
    row padded to stand in for that entity. An aggregate above one is not part
    of its row stream, and may extend."""
    solid: set[str] = set()
    stack = [
        gid
        for gid, a in attrs.items()
        if a.derivation in ROW_STREAM_DERIVATIONS
        and not all(keyspace.defined_on(m, region) for m in a.primary_members)
    ]
    while stack:
        gid = stack.pop()
        if gid in solid:
            continue
        solid.add(gid)
        stack.extend(
            parent
            for parent in group_graph.predecessors(gid)
            if attrs[parent].derivation in ROW_STREAM_DERIVATIONS
            or (
                attrs[parent].derivation == Derivation.ROOT
                and not attrs[parent].extent_spans
            )
        )
    return solid
