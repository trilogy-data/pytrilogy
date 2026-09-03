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

from trilogy.core import graph as nx
from trilogy.core.models.build import BuildDatasource
from trilogy.core.models.build_environment import BuildEnvironment

from .constants import FINAL_NODE_ID
from .functional_dependency import build_fd_determines
from .models import ExtentOwnership, GroupAttrs


def licensed_extension_spans(environment: BuildEnvironment) -> frozenset[str]:
    """Addresses some datasource binds with a column-level ``~``."""
    return frozenset(
        address
        for datasource in environment.datasources.values()
        if isinstance(datasource, BuildDatasource)
        for address in datasource.column_level_partial_addresses
    )


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
    outputs = final.final_contract.output_addresses
    return frozenset(
        span
        for span in licensed
        if span in outputs
        or any(
            build_fd_determines(environment, {span}, out, include_empty_grain=False)
            for out in outputs
        )
    )


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

    permitted: dict[str, frozenset[str]] = {}
    for span, owner in owner_by_span.items():
        for gid in (owner, *nx.ancestors(group_graph, owner)):
            permitted[gid] = permitted.get(gid, frozenset()) | {span}
    return ExtentOwnership(
        spans=ownable, owner_by_span=owner_by_span, permitted=permitted
    )
