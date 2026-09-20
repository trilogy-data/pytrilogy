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
from trilogy.core.processing.join_resolution import licensed_extension_spans

from .constants import FINAL_NODE_ID, ROW_STREAM_DERIVATIONS
from .functional_dependency import build_fd_determines
from .models import ExtentOwnership, GroupAttrs


def demanded_extension_spans(
    attrs: dict[str, GroupAttrs],
    licensed: frozenset[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """Licensed keys whose extension rows this statement actually asks for.

    An extension row exists to carry one dimension member's own attributes, so
    the demand is the key itself in the output or something the key functionally
    determines there. A key that only shows up as a join axis (the `~` FK
    linking two facts under an aggregate nobody groups by it) licenses no
    extension rows, needs no owner, and leaves the whole election inert.
    """
    final = attrs.get(FINAL_NODE_ID)
    if final is None or final.final_contract is None:
        return frozenset()
    return spans_demanded_by(
        licensed, final.final_contract.output_addresses, environment
    )


def spans_demanded_by(
    licensed: frozenset[str],
    outputs: Iterable[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    return frozenset(
        span for span in licensed if span_members(span, outputs, environment)
    )


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
) -> ExtentOwnership:
    spans = demanded_extension_spans(
        attrs, licensed_extension_spans(environment), environment
    )
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
    # A span with a domain bucket of its own is sourced there and nowhere else.
    domains = {
        a.extent_span: gid for gid, a in attrs.items() if a.extent_span in ownable
    }
    owner_by_span.update(domains)

    permitted: dict[str, frozenset[str]] = {}
    for span, owner in owner_by_span.items():
        if span in domains:
            # The domain holds the members; whatever reads it extends, except
            # the row streams that must never see an extension row.
            allowed = ({owner} | nx.descendants(group_graph, owner)) - _solid_groups(
                group_graph, attrs, span, environment
            )
        else:
            allowed = {owner} | nx.ancestors(group_graph, owner)
        for gid in allowed - {FINAL_NODE_ID}:
            permitted[gid] = permitted.get(gid, frozenset()) | {span}
    carried = {
        address: gid
        for gid in domains.values()
        for address in attrs[gid].primary_members
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
    span: str,
    environment: BuildEnvironment,
) -> set[str]:
    """Groups that must not see `span`'s extension rows: each row-stream
    derivation the span does not determine, and the row stream feeding it. A
    derived concept is NULL where its key's entity is absent, which it can only
    be if it never reads a row padded to stand in for that entity. An aggregate
    above one is not part of its row stream, and may extend."""
    solid: set[str] = set()
    stack = [
        gid
        for gid, a in attrs.items()
        if a.derivation in ROW_STREAM_DERIVATIONS
        and not all(
            build_fd_determines(environment, {span}, address, include_empty_grain=True)
            for address in a.primary_members
        )
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
                and not attrs[parent].extent_span
            )
        )
    return solid
