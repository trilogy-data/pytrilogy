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

A region with a domain group of its own is owned by that domain
(`DomainKind.OWN`). The ranking (most downstream, then membership, then joint
coverage of every span) elects an owner for a demanded span whose rows come
from anywhere else: the row stream itself (gcat's composite-key `~` region,
`test_case_key`), the padded plan, or a region only a join of two facts
witnesses (TPC-DS q64's `store_returns` beside `store_sales`), where leaving
the span unmanaged pads every branch (the aggregate reads the padded root, the
fact FULL-joins its returns). It is the same judgment ``_cover_groups_for_mandatory``
applies to already-built nodes. The cover consumes this result rather than
re-deriving it, since a predicted election that diverges from the actual one
leaves a contributor dangling at render time.
"""

from __future__ import annotations

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, FunctionType
from trilogy.core.models.build import BuildConcept, BuildFilterItem, BuildFunction
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.keyspace import Keyspace, Region
from trilogy.core.processing.condition_utility import concepts_implied_non_null

from .constants import FINAL_NODE_ID, ROW_STREAM_DERIVATIONS, DepthLabel
from .models import ExtentOwnership, GroupAttrs


def elect_extent_owners(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    keyspace: Keyspace,
) -> ExtentOwnership:
    """Elect an owner for each span the statement asks extension rows of
    (`Keyspace.output_demanded_spans`). A `~` FK that only shows up as a join
    axis is not among them, and with none the whole mechanism is inert (the
    common case: TPC-DS and TPC-H rarely demand one)."""
    # a region with a domain group of its own is demanded by that alone: a
    # derivation absent on it is an output no lookup from the span reaches
    domains = {
        gid: region
        for gid, a in attrs.items()
        if a.extent_spans and (region := keyspace.region_of(a.extent_spans))
    }
    spans = keyspace.output_demanded_spans.union(*(r.spans for r in domains.values()))
    if not spans:
        return ExtentOwnership()
    exposes: dict[str, frozenset[str]] = {}
    for gid, attr in attrs.items():
        if gid == FINAL_NODE_ID:
            continue
        # a member counts as well as an output: a stream joining on a span
        # the statement never names (`select name, status`) still pads for it
        owned = spans & (set(attr.output_concepts) | set(attr.members))
        if owned:
            exposes[gid] = frozenset(owned)
    if not exposes:
        return ExtentOwnership()

    # Most downstream wins: its rows have already absorbed everything
    # upstream, so routing extent there keeps one copy rather than one per
    # branch. Ties break toward the statement's own row stream over a scan
    # the WHERE reads for itself (its rows reach the output only as a test),
    # then the group that holds the key as a member or a key of its own over
    # one passing it through, then on id for determinism.
    rank = {
        gid: (
            len(nx.ancestors(group_graph, gid)),
            attrs[gid].depth_label not in (DepthLabel.D1, DepthLabel.ROOT_D1),
            len(owned & set(attrs[gid].members)),
            len(owned),
            gid,
        )
        for gid, owned in exposes.items()
    }

    # Only a span some group actually delivers can be routed. One nobody
    # exposes (a transitive dimension's key reached purely through a join)
    # stays unmanaged and keeps per-branch padding, because suppressing what
    # has no owner deletes the extension rows outright.
    ownable: frozenset[str] = frozenset().union(*exposes.values())
    # A region with a domain group of its own is sourced there and nowhere else.
    domain_of_span = {
        span: gid for gid, region in domains.items() for span in region.spans & ownable
    }
    # A domain's span also pads elsewhere when a region without a domain
    # needs it (`returns` completing `lines` on the key a dimension's
    # extension is kept out by): that padding is elected like any other.
    padded_elsewhere: frozenset[str] = frozenset().union(
        *(
            r.spans | r.live_completes
            for r in keyspace.live_regions
            if r not in domains.values()
        )
    )
    elected = ownable - (domain_of_span.keys() - padded_elsewhere)
    owner_by_span: dict[str, str] = {}
    # A group exposing every elected span keeps the families together: split
    # ownership manufactures the same extension member in two branches, which
    # the FINAL merge can only reunite by pairing padding null-safely.
    joint = [gid for gid, owned in exposes.items() if elected <= owned]
    if joint:
        winner = max(joint, key=rank.__getitem__)
        owner_by_span = {span: winner for span in elected}
    else:
        for span in sorted(elected):
            candidates = [gid for gid, owned in exposes.items() if span in owned]
            owner_by_span[span] = max(candidates, key=rank.__getitem__)

    permitted: dict[str, frozenset[str]] = {}
    for span, owner in owner_by_span.items():
        _permit(permitted, span, {owner} | nx.ancestors(group_graph, owner))
    # The domain holds the members; whatever reads it extends, except the row
    # streams that must never see an extension row.
    readers = {
        owner: ({owner} | nx.descendants(group_graph, owner))
        - solid_groups(group_graph, attrs, domains[owner], keyspace, environment)
        for owner in set(domain_of_span.values())
    }
    for span, owner in domain_of_span.items():
        _permit(permitted, span, readers[owner])
    owner_by_span.update(domain_of_span)
    carried = {
        address: gid for gid in domains for address in attrs[gid].primary_members
    }
    return ExtentOwnership(
        owner_by_span=owner_by_span, permitted=permitted, carried=carried
    )


def _permit(permitted: dict[str, frozenset[str]], span: str, groups: set[str]) -> None:
    for gid in groups - {FINAL_NODE_ID}:
        permitted[gid] = permitted.get(gid, frozenset()) | {span}


def null_on_padding(
    value: object, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    """Whether `value` is NULL on a row of `region` however it is planned: it
    cannot be non-null unless something absent there is (`sale_price - cost`).
    Padding already gives the rule's answer, so only a null-opaque derivation
    (CASE, COALESCE, IS NULL, a window) needs the region kept off its row
    stream. CONCAT skips NULL arguments on some dialects."""
    if isinstance(value, BuildConcept):
        if keyspace.defined_on(value.address, region):
            return False
        # a rowset handle is a column of its boundary, NULL on a padded row
        # as a ROOT column is: the body's own keyspace made it so
        if value.derivation in (Derivation.ROOT, Derivation.ROWSET):
            return True
        value = value.lineage
    # `content ? condition` is NULL wherever its content is.
    if isinstance(value, BuildFilterItem):
        return null_on_padding(value.content, region, keyspace, environment)
    if isinstance(value, BuildFunction) and value.operator == FunctionType.CONCAT:
        return False
    return any(
        null_on_padding(environment.concepts[address], region, keyspace, environment)
        for address in concepts_implied_non_null(value)
        if address in environment.concepts
    )


def takes_a_value_on_padding(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    concept = environment.concepts.get(address)
    return (
        concept is not None
        and not keyspace.defined_on(address, region)
        and not null_on_padding(concept, region, keyspace, environment)
    )


def rows_above_a_rollup(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs]
) -> set[str]:
    """Groups reading a ROLLUP/CUBE/GROUPING SETS pass. Its rows are subtotals
    of whatever entered below it, so no region is absent on them: a region's
    rows enter under the pass or not at all."""
    return {
        below
        for gid, a in attrs.items()
        if a.derivation == Derivation.AGGREGATE and a.nulls_grouping_keys
        for below in nx.descendants(group_graph, gid)
    }


def solid_groups(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> set[str]:
    """Groups that must not see `region`'s rows: each row-stream derivation
    absent there that would take a value on a padded row, and the row stream
    feeding it. A derived concept is NULL where its key's entity is absent,
    which it can only be if it never reads a row padded to stand in for that
    entity; one NULL on the padding however it is planned (`null_on_padding`)
    may read the region's rows. An aggregate above one is not part of its row
    stream, and may extend."""
    solid: set[str] = set()
    above_rollup = rows_above_a_rollup(group_graph, attrs)
    stack = [
        gid
        for gid, a in attrs.items()
        if a.derivation in ROW_STREAM_DERIVATIONS
        and gid not in above_rollup
        and any(
            takes_a_value_on_padding(m, region, keyspace, environment)
            for m in a.primary_members
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
                and not attrs[parent].extent_spans
            )
        )
    return solid
