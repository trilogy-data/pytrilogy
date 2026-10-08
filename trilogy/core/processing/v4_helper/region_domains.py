"""Region domains: a live `~` extension region's own rows, carried by a ROOT
group of their own so a derivation absent there never reads padded rows.
Created during grouping, fed to the scalars that evaluate on the region, and
restated at FINAL for the WHERE atoms over it.

A region gets a domain only when demanded (an output is a function of what
its spans reach, or an aggregate counts its rows); otherwise FINAL would add
all-NULL rows past the WHERE. The padded plan is right exactly while nothing
takes a value on a padded row, and it is the smaller plan, so it is kept
until something does. The spans ride hidden on the domain and every bucket
split from it and are the join axis everywhere; only a bucket that MIXES
kinds of row pads. An aggregate over the region computes its named BASIC
arguments on the solid rows first, then the domain merges in, so `count(x)`
never counts a padded row; a COUNT the domain pads is 0, stamped by the merge
that pads it (`CTE.zero_fills_count`).
"""

from collections.abc import Collection, Iterator
from dataclasses import dataclass
from enum import Enum

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_ADDRESS
from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildConceptArgs
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.keyspace import Keyspace, Region

from .concept_graph import _scope_and_phase
from .condition_placement import ConditionPlacement, PlacementReason
from .constants import (
    FINAL_NODE_ID,
    ROW_STREAM_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import EdgeMap, add_edge, edge_kind, remove_edge
from .extent_ownership import (
    null_on_padding,
    solid_groups,
    takes_a_value_on_padding,
)
from .keyspace import lineage_reads
from .models import ConceptAttrs, GroupAttrs, GroupBucket, RootReason
from .projection import reads_a_rollup, reads_rows_only
from .region_reads import (
    aggregates_over_region,
    evaluated_over_region,
    fed_gate,
    inline_arguments_taking_a_value,
    keyless,
    nameable,
    restated_over_region,
)


def _filters_region_domain(
    address: str,
    region: Region,
    keyspace: Keyspace,
    carried: set[str],
    environment: BuildEnvironment,
    outputs: list[BuildConcept],
) -> bool:
    """Whether a WHERE reading `address` still filters a region domain's rows.

    The domain reaches FINAL beside the filtered row stream, not through it, so
    an atom hosted anywhere else is lost on the rows the domain adds back. A
    column the domain carries is filtered on the domain itself. Anything else
    is restated where the domain's rows join back (`restated_over_region`, the
    rule condition placement applies), riding there as a hidden column when
    the statement does not project it: a value carried on the region (a
    scalar over an aggregate by the span, read off the domain), or an absent
    value read from rows alone or off a rowset boundary, NULL on the extension
    row. So is an aggregate the region's rows do not feed (`sum(amount) by
    status`): it pairs on solid keys and is absent on the extension row."""
    if address in carried:
        return True
    if not restated_over_region(
        address, region, carried, keyspace, outputs, environment
    ):
        return False
    if keyspace.carried_on(address, region) or keyless(address, keyspace):
        return True
    concept = environment.concepts.get(address)
    if concept is None:
        return False
    if concept.derivation == Derivation.AGGREGATE:
        return True
    return concept.derivation in (
        Derivation.ROOT,
        Derivation.ROWSET,
    ) or reads_rows_only(concept)


def _splits_for_region(bucket: GroupBucket) -> bool:
    """A plain keyed ROOT bucket, or a cluster the dim peel took out of one,
    or a rowset boundary (a row source whose body padded the region)."""
    if bucket.derivation == Derivation.ROWSET:
        return not bucket.extent_spans
    return bucket.reason in (RootReason.ROW_STREAM, RootReason.ENTITY)


def _members_of(
    buckets: dict[str, GroupBucket],
    label: str,
    derivations: Collection[Derivation],
    exact: bool,
) -> Iterator[str]:
    """Primary members of `derivations` buckets in `label` (`exact`) or in any
    phase of its scope."""
    scope = _scope_and_phase(label)[0]
    for bucket in buckets.values():
        in_scope = (
            bucket.label == label
            if exact
            else _scope_and_phase(bucket.label)[0] == scope
        )
        if in_scope and bucket.derivation in derivations:
            yield from bucket.primary_members


def _needs_solid_rows(
    buckets: dict[str, GroupBucket],
    label: str,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """Whether something in `label` must be evaluated on rows the region is
    absent from: a row-stream derivation that takes a value on the padding
    (`count(grain(s.o, s.sk))`: the hash coalesces NULLs), or an aggregate
    whose inline argument does."""
    return any(
        takes_a_value_on_padding(m, region, keyspace, environment)
        for m in _members_of(buckets, label, ROW_STREAM_DERIVATIONS, exact=True)
    ) or any(
        inline_arguments_taking_a_value(
            environment.concepts.get(m), region, keyspace, environment
        )
        for m in _members_of(buckets, label, (Derivation.AGGREGATE,), exact=True)
    )


def _named_value_on_padding(
    buckets: dict[str, GroupBucket],
    label: str,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A named row-stream derivation of the scope, its WHERE included, takes a
    value on a padded row of `region`. One reading a ROLLUP pass is computed
    on the pass's subtotal rows, where no region is absent."""
    return any(
        takes_a_value_on_padding(m, region, keyspace, environment)
        and not reads_a_rollup(m, environment)
        for m in _members_of(buckets, label, ROW_STREAM_DERIVATIONS, exact=False)
    )


def _inline_values_on_padding(
    buckets: dict[str, GroupBucket],
    label: str,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> list[BuildConceptArgs]:
    """The inline arguments of the scope's aggregates doing the same."""
    return [
        argument
        for m in _members_of(buckets, label, (Derivation.AGGREGATE,), exact=False)
        for argument in inline_arguments_taking_a_value(
            environment.concepts.get(m), region, keyspace, environment
        )
    ]


def _region_is_demanded(
    region: Region,
    keyspace: Keyspace,
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    label: str | None = None,
) -> bool:
    if region.spans & keyspace.output_demanded_spans:
        return True
    # a WHERE filters the statement's rows and never adds one: an aggregate
    # of its own counting the region (`where count(customer_id) by
    # customer_id > 0`) asks for no row of it
    return any(
        label in (None, a.label)
        and a.derivation == Derivation.AGGREGATE
        and not a.existence_only
        and _scope_and_phase(a.label)[1] != "condition"
        and aggregates_over_region((a.address,), region, keyspace, environment)
        for a in concept_attrs.values()
    )


def undemanded_spans(
    keyspace: Keyspace,
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """The spans of live regions with rows of their own that nothing in the
    statement asks for: no output is a function of what they reach and no
    aggregate counts them. Such a region is not a row of the statement, so no
    group may extend its spans (`select order_id, label` is the orders; the
    customer with none is not a row of it, and `label` is not evaluated on a
    padding of her). A region under an authored coalescing relation is that
    relation's, and the union machinery decides."""
    coalescing = environment.domain_graph.coalescing_relation_members()
    # a rowset body's region this plan reads nothing of is padded below the
    # boundary; the boundary plans its body without it (`rowset.owned_spans`)
    out: set[str] = set(keyspace.unread_spans)
    for region in keyspace.live_regions:
        if not region.has_own_rows or region.spans & coalescing:
            continue
        if not _region_is_demanded(region, keyspace, concept_attrs, environment):
            out |= region.spans
    return frozenset(out)


def _mixes_region(bucket: GroupBucket, region: Region, keyspace: Keyspace) -> bool:
    held = [keyspace.carried_on(m, region) for m in bucket.primary_members]
    return any(held) and not all(held)


def _holds_a_materialized_aggregate(
    bucket: GroupBucket,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A summary table rolled up to the statement's grain, held as a ROOT: a
    rollup over the region's rows the keyspace does not model. One keyed by
    the region's span (`total_amount` by customer) is an attribute of the
    region's entity like any other, and the domain carries it."""
    return any(
        (c := environment.concepts.get(m)) is not None
        and c.is_aggregate
        and not keyspace.carried_on(m, region)
        for m in bucket.primary_members
    )


def _pads_region(
    bucket: GroupBucket,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """Something the region carries sourced beside something absent on it: the
    bucket's rows are the solid stream beside the region's domain. A bucket
    reading a MATERIALIZED aggregate is not modelled yet and keeps the padded
    plan."""
    return _mixes_region(
        bucket, region, keyspace
    ) and not _holds_a_materialized_aggregate(bucket, region, keyspace, environment)


class DomainKind(Enum):
    """Where a demanded region's own rows come from."""

    # a ROOT bucket of its own, joined back to the solid row stream
    OWN = "own"
    # the scope sources nothing absent on the region: its row stream is the
    # region's rows
    ROW_STREAM = "row_stream"
    # the rowset boundary whose body padded the region, and nothing the scope
    # derives has to be evaluated on the solid rows
    BOUNDARY = "boundary"
    # an authored coalescing relation (`union join ocust = cid`) unions the
    # region's key, and the union machinery builds it
    RELATION = "relation"
    # not modelled: the padded plan stands in for the domain
    PADDED = "padded"


@dataclass
class RegionDomain:
    """A live extension region the statement asks rows of, what the scope's
    root demand holds that the region carries, and where those rows come
    from. Only an OWN domain has a bucket."""

    region: Region
    kind: DomainKind
    label: str
    carried: frozenset[str]
    bucket: GroupBucket | None = None
    note: str = ""


def _own_bucket(
    region: Region,
    label: str,
    eligible: list[GroupBucket],
    rowset: list[GroupBucket],
    members: dict[str, str],
    null_members: frozenset[str] = frozenset(),
) -> GroupBucket:
    extent = f"extent:{'|'.join(sorted(region.spans))}"
    domain = GroupBucket(
        depth_label=rowset[0].depth_label if rowset else DepthLabel.ROOT,
        derivation=Derivation.ROWSET if rowset else Derivation.ROOT,
        grain_components=frozenset(),
        label=label,
        discriminator=f"{rowset[0].discriminator}:{extent}" if rowset else extent,
        extent_spans=region.spans,
        null_member_spans=null_members,
        reason=RootReason.REGION,
    )
    depths = {a: d for b in eligible for a, d in b.member_depths.items()}
    for address, node_id in members.items():
        domain.add_member(address, node_id, depths.get(address, DepthLabel.ROOT))
    return domain


def _takes_a_value_on_a_null_member(
    buckets: dict[str, GroupBucket],
    label: str,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A row-level derivation of what the region carries that is not NULL on a
    fact row keyed on the span's NULL member, where every entity but the
    span's is present. An aggregate, and what is derived from one, groups that
    member on the fact's rows."""
    if not region.spans & keyspace.value_null_spans:
        return False
    row = keyspace.row_absent(region.spans)
    return any(
        b.derivation == Derivation.BASIC
        and keyspace.carried_on(m, region)
        and _null_propagating(environment.concepts.get(m))
        and takes_a_value_on_padding(m, row, keyspace, environment)
        for b in buckets.values()
        if b.label == label
        for m in b.primary_members
    )


def _null_propagating(concept: BuildConcept | None) -> bool:
    """Every lineage step is a per-row scalar of its arguments, so NULL inputs
    give NULL. Stricter than `reads_rows_only` on purpose: a window or filter
    over the NULL member's rows can still take a value (`row_number()` is 1
    there), while for `reads_rows_only` it is enough that no aggregate is
    crossed, since `where order_seq is null` must keep the orderless customer
    (`test_derived_key_domain`)."""
    if concept is None or concept.lineage is None:
        return concept is not None
    return concept.derivation == Derivation.BASIC and all(
        _null_propagating(arg) for arg in concept.lineage.concept_arguments
    )


def _region_domain(
    region: Region,
    label: str,
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    keyspace: Keyspace,
    condition_arg_addresses: frozenset[str],
    mandatory_list: list[BuildConcept],
    relation_spans: set[str],
    rollup_padded: frozenset[str],
) -> RegionDomain | None:
    eligible = [
        b for b in buckets.values() if b.label == label and _splits_for_region(b)
    ]
    carried = frozenset(
        m for b in eligible for m in b.primary_members if keyspace.carried_on(m, region)
    )
    if not carried or not _region_is_demanded(
        region, keyspace, concept_attrs, environment, label
    ):
        return None
    scope = (buckets, label, region, keyspace, environment)
    named, inline = _named_value_on_padding(*scope), _inline_values_on_padding(*scope)
    if carried & rollup_padded and (
        not (named or inline) or not all(nameable(argument) for argument in inline)
    ):
        # a rollup's subtotal rows NULL the key a domain would join back on,
        # so the region's rows enter below the pass or not at all. The padded
        # plan does that, and is right while nothing below the pass takes a
        # value on a padded row. A derivation that does is computed on the
        # solid rows and the domain pads it under the pass
        # (`evaluated_over_region`): a named one as its own node, an inline
        # function argument once the strategy builder stands a concept in
        # for it. Any other inline argument has no node to compute on first,
        # and keeps the padded plan.
        return RegionDomain(
            region, DomainKind.PADDED, label, carried, note="rollup key"
        )
    if region.spans & relation_spans and not named:
        return RegionDomain(region, DomainKind.RELATION, label, carried)
    if not any(_mixes_region(b, region, keyspace) for b in eligible):
        return RegionDomain(region, DomainKind.ROW_STREAM, label, carried)
    sources = [b for b in eligible if _pads_region(b, region, keyspace, environment)]
    if not sources:
        # a rollup over the region's rows the keyspace does not model
        return RegionDomain(
            region, DomainKind.PADDED, label, carried, note="materialized aggregate"
        )
    rowset = [b for b in sources if b.derivation == Derivation.ROWSET]
    if rowset and not _needs_solid_rows(buckets, label, region, keyspace, environment):
        return RegionDomain(region, DomainKind.BOUNDARY, label, carried)
    # beside a rowset the domain is the boundary again; otherwise it takes
    # every member of the region the scope's row stream holds
    holders = rowset or [
        b
        for b in eligible
        if b.derivation == Derivation.ROOT
        and not _holds_a_materialized_aggregate(b, region, keyspace, environment)
    ]
    members = {
        address: node_id
        for b in (*sources, *holders)
        for address, node_id in zip(b.primary_members, b.primary_node_ids)
        if keyspace.carried_on(address, region)
    }
    undelivered = sorted(
        address
        for address in condition_arg_addresses
        if not _filters_region_domain(
            address, region, keyspace, set(members), environment, mandatory_list
        )
        # a statement-wide gate the region's rows feed (`avg(bal) by *`) is
        # restated over the domain too, but the padded plan is the smaller
        # one and is right while nothing takes a value on a padded row
        or (not (named or inline) and fed_gate(address, region, keyspace, environment))
    )
    if undelivered:
        logger.info(
            f"region {region.describe()} keeps its padded plan: no host"
            f" filters its rows by {undelivered}"
        )
        return RegionDomain(
            region,
            DomainKind.PADDED,
            label,
            carried,
            note=f"no host filters its rows by {undelivered}",
        )
    # a fact binding the span `?` holds a NULL member no dimension row does:
    # `coalesce(name, 'unknown')` is 'unknown' on its rows, which a domain of
    # the dimension's rows alone would pad instead. Such a domain holds the
    # NULL member too (`strategy_builder._with_null_members`), and its rows
    # join back null-safely; where nothing takes a value on it the fact's
    # rows pass through the join back unmatched, which is the same answer.
    null_members = (
        region.spans & keyspace.value_null_spans
        if _takes_a_value_on_a_null_member(
            buckets, label, region, keyspace, environment
        )
        else frozenset()
    )
    return RegionDomain(
        region,
        DomainKind.OWN,
        label,
        frozenset(members),
        _own_bucket(region, label, eligible, rowset, members, null_members),
    )


def decide_region_domains(
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    keyspace: Keyspace,
    condition_arg_addresses: frozenset[str],
    mandatory_list: list[BuildConcept],
    rollup_padded: frozenset[str],
) -> list[RegionDomain]:
    """Say where the rows of each live extension region the statement asks
    rows of come from (`DomainKind`), giving the region a ROOT bucket of its
    own when the statement derives something absent on it.

    A derived concept is NULL where a key's entity is absent, so it must not
    evaluate over padded rows. The domain bucket carries the region's own rows
    and owns their extent (`elect_extent_owners`); everything feeding the
    derivation pairs on solid keys, and the region's rows join back above it
    (`feed_region_domains_to_present_scalars`). Regions are disjoint, so one
    domain per region.

    Only a region with rows of its own that the statement asks rows of (an
    output within its spans' reach, or an aggregate counting it). A ROWSET
    boundary already holds the region's rows and is split only when something
    must be evaluated on the solid rows (`_needs_solid_rows`). Decided on the
    scope's whole root demand, before any entity peel."""
    relation_spans = environment.domain_graph.coalescing_relation_members()
    labels = sorted({b.label for b in buckets.values()})
    domains: list[RegionDomain] = []
    for region in keyspace.live_regions:
        if not region.has_own_rows:
            continue
        assert all(span in environment.concepts for span in region.spans), region
        for label in labels:
            domain = _region_domain(
                region,
                label,
                buckets,
                concept_attrs,
                environment,
                keyspace,
                condition_arg_addresses,
                mandatory_list,
                relation_spans,
                rollup_padded,
            )
            if domain is not None:
                domains.append(domain)
    owned = [d.region for d in domains if d.bucket is not None]
    assert len(owned) == len(set(owned)), "two OWN domains for one region"
    return domains


def carry_region_spans(
    buckets: dict[str, GroupBucket],
    domains: list[GroupBucket],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> None:
    """The region's rows join back on its spans, so every scan they pair with
    carries them.

    A span the statement never names rides the domain and every solid stream
    beside it as a hidden column.

    The condition phase's fact scan (`root_d1`) rides the span too when it
    reads something absent on the region: a WHERE over such a value beside
    dimension-only outputs (`select customer_id, name where status is null`)
    reaches FINAL as a producer of its own, and pairs with the domain only on
    the span. Without it the merge is keyless."""
    for bucket in domains:
        region = keyspace.region_of(bucket.extent_spans)
        assert region is not None, bucket.extent_spans
        scope = _scope_and_phase(bucket.label)[0]
        solid = [
            b
            for b in buckets.values()
            if b.label == bucket.label
            and _splits_for_region(b)
            and _pads_region(b, region, keyspace, environment)
        ]
        for span in sorted(region.spans - set(bucket.primary_members)):
            for side in (bucket, *solid):
                _carry(side, span)
        for scan in buckets.values():
            if (
                scan.reason is not RootReason.CONDITION
                or _scope_and_phase(scan.label)[0] != scope
                or all(keyspace.carried_on(m, region) for m in scan.primary_members)
            ):
                continue
            for span in sorted(region.spans):
                if span not in scan.primary_members:
                    _carry(scan, span)


def _carry(bucket: GroupBucket, span: str) -> None:
    if span not in bucket.carried_spans:
        bucket.carried_spans.append(span)


def split_carried_only_row_streams(
    buckets: dict[str, GroupBucket],
    primary_group: dict[str, str],
    domains: list[RegionDomain],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> None:
    """A row-stream bucket holding a member that reads only what a region
    carries (`item_desc as d`) beside one that reads something absent there
    (`grain(order_number, item_sk)`) is split: the carried-only members get a
    bucket of their own, which `feed_region_domains_to_present_scalars` then
    sources from the domain. Kept together, the whole bucket is computed on
    the solid rows and the rename is NULL for the region's unmatched member,
    while its source `item_desc` rides the domain beside it."""
    for domain, region in [(d.bucket, d.region) for d in domains if d.bucket]:
        for gid in list(buckets):
            bucket = buckets[gid]
            if (
                bucket.derivation != Derivation.BASIC
                or bucket.label != domain.label
                or bucket.extent_spans
                or len(bucket.primary_members) < 2
            ):
                continue
            carried_only = [
                idx
                for idx, member in enumerate(bucket.primary_members)
                if _reads_only_carried(member, region, keyspace, environment)
            ]
            if not carried_only or len(carried_only) == len(bucket.primary_members):
                continue
            moved_concepts = [
                c
                for idx in carried_only
                if (c := environment.concepts.get(bucket.primary_members[idx]))
                is not None
            ]
            grain = frozenset(
                component
                for c in moved_concepts
                if c.grain
                for component in c.grain.components
            )
            split = GroupBucket(
                depth_label=bucket.depth_label,
                derivation=bucket.derivation,
                grain_components=grain or bucket.grain_components,
                label=bucket.label,
                discriminator=(
                    f"{bucket.discriminator}:reads:{'|'.join(sorted(region.spans))}"
                ),
            )
            for idx in carried_only:
                addr = bucket.primary_members[idx]
                node_id = bucket.primary_node_ids[idx]
                split.add_member(
                    addr, node_id, bucket.member_depths.get(addr, bucket.depth_label)
                )
                primary_group[node_id] = split.group_id
            assert split.group_id not in buckets, split.group_id
            buckets[split.group_id] = split
            bucket.drop_members(carried_only)


def _carried_arguments(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> list[bool]:
    return [
        keyspace.carried_on(read, region)
        for read in lineage_reads(address, environment)
    ]


def _reads_carried(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    return any(_carried_arguments(address, region, keyspace, environment))


def _reads_only_carried(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    carried = _carried_arguments(address, region, keyspace, environment)
    return bool(carried) and all(carried)


def _is_rows_of_another_region(
    members: tuple[str, ...] | list[str],
    region: Region,
    domain_regions: list[Region],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A row stream reading only what another region carries is that region's
    rows; regions are disjoint, so it never reads this one's."""
    return any(
        other != region
        and all(_reads_only_carried(m, other, keyspace, environment) for m in members)
        for other in domain_regions
    )


def feed_region_domains_to_present_scalars(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> None:
    """Wire each region domain to what evaluates on the region's rows.

    - A scalar over an aggregate by the span is keyed on the span, present on
      the region (`case when count(order_id) by customer_id > 0 ... else
      'dormant'`), so it and the WHERE's copy of it read the domain beside the
      solid aggregate.
    - An aggregate is evaluated OVER the region's rows when they survive it
      (`evaluated_over_region`): a grouping key the region carries, or an
      argument it counts. An inline argument taking a value on a padded row
      keeps it solid; so does a reader that must stay solid (`solid_groups`).
      A ROLLUP pass takes the region's rows below it or not at all.
    - A WHERE's statement-wide aggregate over the region reads the domain
      alone (`_counts_the_domain`).
    - A row-stream derivation reading something the domain carries reads the
      domain of every region it is NULL on the padding of
      (`null_on_padding`), unless it feeds a stream that must stay solid. One
      reading nothing carried has no use for the domain's rows."""
    domain_regions = [
        region
        for domain in attrs.values()
        if domain.extent_spans
        and (region := keyspace.region_of(domain.extent_spans)) is not None
    ]
    for domain_gid, domain in list(attrs.items()):
        if not domain.extent_spans:
            continue
        region = keyspace.region_of(domain.extent_spans)
        assert region is not None, domain.extent_spans
        scope = _scope_and_phase(domain.label)[0]
        # a row stream that must never see an extension row (`solid_groups`)
        # keeps every aggregate it reads solid too
        solid = solid_groups(group_graph, attrs, region, keyspace, environment)
        for gid, a in attrs.items():
            if (
                a.derivation in ROW_STREAM_DERIVATIONS
                and _scope_and_phase(a.label)[0] == scope
                and a.primary_members
            ):
                if not _reads_region_domain(
                    gid, a, region, solid, domain_regions, keyspace, environment
                ):
                    continue
                if _is_region_rows(gid, a, region, solid, keyspace, environment):
                    _detach_solid_roots(
                        group_graph, group_edges, attrs, gid, domain, environment
                    )
            elif _counts_the_domain(a, domain, region, keyspace, environment):
                _detach_solid_roots(
                    group_graph, group_edges, attrs, gid, domain, environment
                )
            elif not (
                _counts_the_region_in_condition(
                    a, domain, region, keyspace, environment
                )
                or a.derivation == Derivation.AGGREGATE
                and a.label == domain.label
                and evaluated_over_region(
                    a.primary_members,
                    a.grain_components,
                    region,
                    keyspace,
                    environment,
                    one_pass=a.nulls_grouping_keys,
                )
                and not (solid and solid & nx.descendants(group_graph, gid))
            ):
                continue
            add_edge(group_graph, group_edges, domain_gid, gid, EdgeKind.LINEAGE)


def _counts_the_domain(
    a: GroupAttrs,
    domain: GroupAttrs,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A WHERE's own statement-wide aggregate over nothing but what the domain
    holds (`count(customer_id) by *`): the region's rows are its input, read
    off the domain. Its condition scan is shared with the atoms beside it, and
    joined to a fact one of them reads it holds only the members that fact
    references."""
    if (
        a.derivation != Derivation.AGGREGATE
        or a.label == domain.label
        or _scope_and_phase(a.label)[0] != _scope_and_phase(domain.label)[0]
        or not a.grain_components <= {ALL_ROWS_ADDRESS}
        or not aggregates_over_region(a.primary_members, region, keyspace, environment)
    ):
        return False
    reads = frozenset().union(
        *(lineage_reads(m, environment) for m in a.primary_members)
    )
    return reads - {ALL_ROWS_ADDRESS} <= set(domain.primary_members)


def _counts_the_region_in_condition(
    a: GroupAttrs,
    domain: GroupAttrs,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A WHERE's own aggregate of the domain's scope counting what the region
    carries by something absent on it (`where count(customer_id) by status =
    1`): the region's rows are its input beside the solid ones, as they are
    the output spelling's, under the NULL group. One by what the region
    carries is read off the domain (`city`)."""
    return (
        a.derivation == Derivation.AGGREGATE
        and a.label != domain.label
        and _scope_and_phase(a.label)
        == (_scope_and_phase(domain.label)[0], "condition")
        and not any(keyspace.carried_on(m, region) for m in a.primary_members)
        and aggregates_over_region(a.primary_members, region, keyspace, environment)
    )


def _reads_region_domain(
    gid: str,
    a: GroupAttrs,
    region: Region,
    solid: set[str],
    domain_regions: list[Region],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A row stream keyed on what the region carries, or one reading something
    a domain carries that is NULL on this region's padding however it is
    planned."""
    members = a.primary_members
    if all(keyspace.carried_on(m, region) for m in members):
        return True
    return (
        gid not in solid
        and not _is_rows_of_another_region(
            members, region, domain_regions, keyspace, environment
        )
        and any(
            _reads_carried(m, r, keyspace, environment)
            for m in members
            for r in domain_regions
        )
        and all(
            keyspace.carried_on(m, region)
            or (
                (c := environment.concepts.get(m)) is not None
                and null_on_padding(c, region, keyspace, environment)
            )
            for m in members
        )
    )


def _is_region_rows(
    gid: str,
    a: GroupAttrs,
    region: Region,
    solid: set[str],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """A row stream reading nothing but what the region carries (`upper(name)`)
    is the region's rows: the domain replaces the solid stream it was split
    from as its source, or it is evaluated over both, one row per fact row,
    and a consumer joining it back fans out. Every read, not the member's
    keys: a filter is keyed on its content and reads its predicate off the
    solid rows too. Asked only of a stream that reads the domain, so the
    stream it loses is always replaced (an existence set the keyspace does
    not key reads only carried values, but is not the region's rows)."""
    return gid not in solid and all(
        _reads_only_carried(m, region, keyspace, environment) for m in a.primary_members
    )


def _detach_solid_roots(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    domain: GroupAttrs,
    environment: BuildEnvironment,
) -> None:
    """Replace the solid roots `gid` reads with the domain, keeping one that
    supplies a read the domain does not hold: a condition phase's private scan
    holds columns the domain, taken from the row stream, never saw."""
    scope = _scope_and_phase(domain.label)[0]
    reads = frozenset().union(
        *(lineage_reads(m, environment) for m in attrs[gid].primary_members)
    )
    for pred in list(group_graph.predecessors(gid)):
        if (
            attrs[pred].derivation == Derivation.ROOT
            and not attrs[pred].extent_spans
            and _scope_and_phase(attrs[pred].label)[0] == scope
            and edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE
            and reads & set(attrs[pred].members) <= set(domain.primary_members)
        ):
            remove_edge(group_graph, group_edges, pred, gid)


def detach_final_span_domain_producers(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    buckets: dict[str, GroupBucket],
    placements: list[ConditionPlacement],
) -> None:
    """An atom restated at FINAL over a region domain's rows is applied there
    only. The constraint edges from its value's producer (a condition branch)
    into the row-stream hosts below would join that branch into the solid
    stream and apply the atom there, pairing on solid keys the region's rows
    never match; without them the producer reaches FINAL as a contributor of
    its own, read off the domain, and FINAL tests every row.

    A host the domain itself feeds (an aggregate by the span, evaluated over
    the region's rows) unites them below FINAL: when the atom is placed there
    the producer stays its contributor, FINAL collapses into it, and the atom
    is applied there, on every united row before the aggregate (`count(order_id)
    by customer_id where flag = 1 or flag is null`: the padded row has no flag
    and counts 0, a rejected order is not counted). The producer constrains
    only the hosts the atom was placed at: an atom over an aggregate BY the
    span is placed at FINAL, and merged into a sibling aggregate's input it
    would drop whole items there, which the domain then pads back with the
    count read off the filtered stream."""
    for placement in placements:
        if placement.reason is not PlacementReason.FINAL_SPAN_DOMAIN:
            continue
        inputs = {a.address for a in placement.atom.row_arguments}
        hosts = {
            gid for p in placements if p.atom is placement.atom for gid in p.group_ids
        }
        for gid, bucket in buckets.items():
            if not inputs & set(bucket.primary_members):
                continue
            for succ in list(group_graph.successors(gid)):
                if (
                    succ != FINAL_NODE_ID
                    and succ not in hosts
                    and edge_kind(group_edges, gid, succ) == EdgeKind.CONSTRAINT
                ):
                    remove_edge(group_graph, group_edges, gid, succ)
