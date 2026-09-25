"""Region domains: a live `~` extension region's own rows, carried by a ROOT
group of their own so a derivation absent there never reads padded rows
(docs/keyspace_phase_plan.md). Created during grouping, fed to the scalars that
evaluate on the region, and restated at FINAL for the WHERE atoms over it.
"""

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
)
from trilogy.core.models.build_environment import BuildEnvironment

from .concept_graph import _scope_and_phase
from .condition_placement import ConditionPlacement, PlacementReason
from .constants import FINAL_NODE_ID, ROW_STREAM_DERIVATIONS, DepthLabel, EdgeKind
from .edges import EdgeMap, add_edge, edge_kind, remove_edge
from .extent_ownership import _solid_groups as solid_groups
from .extent_ownership import null_on_padding
from .models import ConceptAttrs, GroupAttrs, GroupBucket, Keyspace, Region
from .projection import decided_at_output_grain, reads_rows_only


def _has_absent_inline_argument(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    """An aggregate argument written inline (`sum(coalesce(amount, 0))`) is a
    derivation no concept node stands for; it takes a value on `region`'s rows
    when it reads something absent there and is not NULL for it."""
    concept = environment.concepts.get(address)
    if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
        return False
    return any(
        not null_on_padding(arg, region, keyspace, environment)
        and any(
            not keyspace.defined_on(read.address, region)
            for read in arg.concept_arguments
        )
        for arg in concept.lineage.function.arguments
        if isinstance(arg, BuildConceptArgs) and not isinstance(arg, BuildConcept)
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
    the region's rows hold a value for is restated at FINAL over the extended
    rows (`condition_placement._reads_past_region_domain`), riding there as a
    hidden column when the statement does not project it: a value carried on
    the region (a scalar over an aggregate by the span, read off the domain),
    or an absent value read from rows alone, NULL on the extension row."""
    if address in carried:
        return True
    if keyspace.carried_on(address, region):
        return decided_at_output_grain(address, outputs, environment)
    concept = environment.concepts.get(address)
    return (
        concept is not None
        and not keyspace.defined_on(address, region)
        and (concept.derivation == Derivation.ROOT or reads_rows_only(concept))
    )


def _splits_for_region(bucket: GroupBucket) -> bool:
    """A plain keyed ROOT bucket, or a cluster the dim peel took out of one."""
    return (
        bucket.derivation == Derivation.ROOT
        and bucket.depth_label == DepthLabel.ROOT
        and (not bucket.discriminator or bool(bucket.dim_keys))
    )


def _region_is_demanded(
    label: str,
    region: Region,
    keyspace: Keyspace,
    demanded_spans: frozenset[str],
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
) -> bool:
    if region.spans & demanded_spans:
        return True
    return any(
        a.label == label
        and a.derivation == Derivation.AGGREGATE
        and not a.existence_only
        and _aggregates_over_region((a.address,), region, keyspace, environment)
        for a in concept_attrs.values()
    )


def _mixes_region(bucket: GroupBucket, region: Region, keyspace: Keyspace) -> bool:
    held = [keyspace.carried_on(m, region) for m in bucket.primary_members]
    return any(held) and not all(held)


def _keyed_by_region(bucket: GroupBucket, region: Region) -> bool:
    return bool(bucket.dim_keys) and bucket.dim_keys <= region.spans


def add_region_domain_buckets(
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    keyspace: Keyspace,
    condition_arg_addresses: frozenset[str],
    mandatory_list: list[BuildConcept],
) -> None:
    """Give a live extension region its own ROOT bucket when the statement
    derives something absent on it.

    A derived concept is a function of its keys and NULL where a key's entity
    is absent. Sourced with the region's rows, the derivation's inputs are
    padded and it evaluates over rows that have no such entity (`case ... else
    'in-transit'` for a customer with no order). The domain bucket carries the
    region's own rows instead and owns their extent (`elect_extent_owners`):
    everything feeding the derivation pairs on solid keys, and the region's
    rows join back above it, at FINAL or at a scalar over an aggregate by the
    span (`feed_region_domains_to_present_scalars`). One domain per region:
    regions are disjoint, so two families' rows never pair.

    Only a region the statement asks rows of: an output is a function of what
    its span reaches (`output_demanded_spans`, the election's question), or an
    aggregate counts them. `select order_id, status where name = 'ann'` asks
    for orders; the customer with none is not a row of it."""
    # an authored coalescing relation (`union join ocust = cid`) IS its key's
    # domain, and the union machinery builds it; no region domain beside it
    coalescing = environment.domain_graph.coalescing_relation_members()
    for region in keyspace.live_regions:
        if not region.has_own_rows or not all(
            span in environment.concepts for span in region.spans
        ):
            continue
        if region.spans & coalescing:
            continue
        for label in sorted({b.label for b in buckets.values()}):
            eligible = [
                b
                for b in buckets.values()
                if b.label == label and _splits_for_region(b)
            ]
            # the buckets that pad: something the region carries sourced
            # beside something absent on it. Their rows become the solid stream.
            # A bucket reading a MATERIALIZED aggregate (a summary table rolled
            # up to the statement's grain) is a rollup over the region's rows
            # that the group graph holds as a ROOT: not modelled yet, it keeps
            # the padded plan.
            sources = [
                b
                for b in eligible
                if _mixes_region(b, region, keyspace)
                and not any(
                    (c := environment.concepts.get(m)) is not None and c.is_aggregate
                    for m in b.primary_members
                )
            ]
            # a dim peel keyed by the span is the region's own rows already;
            # the domain takes its members too, or FINAL reads them off a solid
            # sibling that passes them through
            carried = {
                m
                for b in eligible
                if b in sources or _keyed_by_region(b, region)
                for m in b.primary_members
                if keyspace.carried_on(m, region)
            }
            if (
                not sources
                or not carried
                or not _region_is_demanded(
                    label,
                    region,
                    keyspace,
                    keyspace.output_demanded_spans,
                    concept_attrs,
                    environment,
                )
            ):
                continue
            undelivered = sorted(
                address
                for address in condition_arg_addresses
                if not _filters_region_domain(
                    address, region, keyspace, carried, environment, mandatory_list
                )
            )
            if undelivered:
                # no host filters the domain's rows by these, so the region
                # keeps its padded plan
                logger.info(
                    f"region {region.describe()} keeps its padded plan: no host"
                    f" filters its rows by {undelivered}"
                )
                continue
            domain = GroupBucket(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain_components=frozenset(),
                label=label,
                discriminator=f"extent:{'|'.join(sorted(region.spans))}",
                extent_spans=region.spans,
            )
            for bucket in eligible:
                if bucket not in sources and not _keyed_by_region(bucket, region):
                    continue
                for addr, node_id in zip(
                    bucket.primary_members, bucket.primary_node_ids
                ):
                    if addr in carried and addr not in domain.primary_members:
                        domain.primary_members.append(addr)
                        domain.primary_node_ids.append(node_id)
                        domain.member_depths[addr] = bucket.member_depths.get(
                            addr, DepthLabel.ROOT
                        )
            # the region's rows join back on its spans: a span the statement
            # never names rides both sides as a hidden column
            for span in sorted(region.spans - carried):
                for side in (domain, *sources):
                    if span not in side.secondary_members:
                        side.secondary_members.append(span)
                        side.member_depths[span] = DepthLabel.ROOT
            buckets[domain.group_id] = domain


def _aggregates_over_region(
    members: tuple[str, ...],
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """An aggregate is evaluated OVER a region's rows when they hold its
    argument: `count(customer_id) by status` counts the customer with no order,
    under the NULL status of a row that has none."""
    for member in members:
        concept = environment.concepts.get(member)
        if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
            return False
        arguments = concept.lineage.function.concept_arguments
        if not arguments or not all(
            keyspace.carried_on(arg.address, region) for arg in arguments
        ):
            return False
    return bool(members)


def _reads_carried(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    concept = environment.concepts.get(address)
    if concept is None or concept.lineage is None:
        return False
    return any(
        keyspace.carried_on(arg.address, region)
        for arg in concept.lineage.concept_arguments
    )


def feed_region_domains_to_present_scalars(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> None:
    """A scalar over an aggregate by the span is keyed on the span, which IS
    present on the region, so it evaluates there: `case when count(order_id)
    by customer_id > 0 ... else 'dormant'` is 'dormant' for a customer with no
    order. The aggregate below it pairs on solid keys, so the scalar reads the
    region's domain beside it. A WHERE's own copy of such a scalar (the
    condition phase of the same scope) reads it the same way, or the atom
    restated at FINAL never sees the region's rows.

    An aggregate is evaluated OVER the region's rows when they survive it: a
    grouping key the region carries keeps each extension row its own group
    (`count(order_id) by customer_id` is 0 for the customer with no order, and
    `coalesce(sum(amount), 0)` above it is 0), or the argument is what it
    counts (`count(customer_id) by status`). Its named BASIC arguments are
    computed on the solid rows first (`_project_basic_aggregate_inputs`), but
    an INLINE argument has no node to compute on, so one that takes a value on
    a padded row (`sum(case when undelivered then 1 else 0 end)`) keeps the
    aggregate solid. Grouped by nothing the region carries (`sum(qty) by
    order_id`) every extension row would collapse into one NULL group: the
    aggregate pairs on solid keys and the domain pads it at FINAL. Never when
    a row stream that must not see an extension row reads it
    (`_solid_groups`).

    A row-stream derivation that READS something a region domain carries
    (`sale_price - cost` reads the product's `cost`) reads the domain of
    every region it is NULL on the padding of however it is planned
    (`null_on_padding`), unless it feeds a stream that must stay solid:
    evaluated over the padded rows it is NULL exactly where the rule says, it
    holds the same regions as its consumer's other contributors, and the
    solid stream beside the domain re-sources nothing the domain carries
    (thelook: `users LEFT JOIN order_items FULL JOIN products` stays one
    SELECT). One that reads nothing carried (a rename of the fact's own
    column) has no use for the domain's rows; reading them would only pad
    its stream."""
    domain_regions = [
        region
        for domain in attrs.values()
        if domain.extent_spans
        and (region := keyspace.region_of(domain.extent_spans)) is not None
    ]
    for domain_gid, domain in list(attrs.items()):
        region = (
            keyspace.region_of(domain.extent_spans) if domain.extent_spans else None
        )
        if region is None:
            continue
        scope = _scope_and_phase(domain.label)[0]
        # a row stream that must never see an extension row (`_solid_groups`)
        # keeps every aggregate it reads solid too
        solid = solid_groups(group_graph, attrs, region, keyspace, environment)
        for gid, a in attrs.items():
            if (
                a.derivation in ROW_STREAM_DERIVATIONS
                and _scope_and_phase(a.label)[0] == scope
                and a.primary_members
                and (
                    all(keyspace.carried_on(m, region) for m in a.primary_members)
                    or (
                        gid not in solid
                        and any(
                            _reads_carried(m, r, keyspace, environment)
                            for m in a.primary_members
                            for r in domain_regions
                        )
                        and all(
                            keyspace.carried_on(m, region)
                            or (
                                (c := environment.concepts.get(m)) is not None
                                and null_on_padding(c, region, keyspace, environment)
                            )
                            for m in a.primary_members
                        )
                    )
                )
            ) or (
                a.derivation == Derivation.AGGREGATE
                and a.label == domain.label
                and not solid & nx.descendants(group_graph, gid)
                and (
                    _aggregates_over_region(
                        a.primary_members, region, keyspace, environment
                    )
                    or (
                        any(keyspace.carried_on(g, region) for g in a.grain_components)
                        and not any(
                            _has_absent_inline_argument(
                                m, region, keyspace, environment
                            )
                            for m in a.primary_members
                        )
                    )
                )
            ):
                add_edge(group_graph, group_edges, domain_gid, gid, EdgeKind.LINEAGE)


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
    its own, read off the domain, and FINAL tests every row."""
    for placement in placements:
        if placement.reason is not PlacementReason.FINAL_SPAN_DOMAIN:
            continue
        inputs = {a.address for a in placement.atom.row_arguments}
        for gid, bucket in buckets.items():
            if not inputs & set(bucket.primary_members):
                continue
            for succ in list(group_graph.successors(gid)):
                if (
                    succ != FINAL_NODE_ID
                    and edge_kind(group_edges, gid, succ) == EdgeKind.CONSTRAINT
                ):
                    remove_edge(group_graph, group_edges, gid, succ)
