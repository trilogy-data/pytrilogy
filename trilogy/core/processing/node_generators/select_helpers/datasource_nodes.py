from dataclasses import dataclass
from typing import TYPE_CHECKING

from trilogy.constants import logger
from trilogy.core.enums import AggregateGroupingMode, Derivation, Granularity
from trilogy.core.graph_models import (
    ReferenceGraph,
    datasource_has_filter_sensitive_aggregate,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildUnionDatasource,
    BuildWhereClause,
    CanonicalBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import get_additive_rollup_concepts
from trilogy.core.processing.condition_utility import (
    condition_implies,
    filter_union_children,
)
from trilogy.core.processing.node_generators.select_helpers.condition_routing import (
    ConditionExpression,
    datasource_conditions,
    preexisting_conditions,
)
from trilogy.core.processing.nodes import (
    ConstantNode,
    GroupNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.utility import padding

if TYPE_CHECKING:
    from trilogy.core.processing.nodes.union_node import UnionNode

LOGGER_PREFIX = "[GEN_ROOT_MERGE_NODE]"


@dataclass
class SourceNodeCandidate:
    node: StrategyNode
    force_group: bool
    group_source_count: int
    conditions_deferred: bool


def extract_address(node: str) -> str:
    return node.split("~")[1].split("@")[0]


def finalize_select_node(
    candidate: SourceNodeCandidate,
    environment: BuildEnvironment,
    depth: int,
    defer_group: bool = False,
    requested_concepts: list[BuildConcept] | None = None,
) -> StrategyNode:

    node = candidate.node
    # When the concept set was widened with filter-only columns so a pushed
    # WHERE could be sourced, the source can be finer than the requested grain
    # (e.g. count(<key>) off a datasource whose grain is below <key>). The
    # condition is applied on `node`, so its filter columns are already
    # consumed — collapse back to the requested grain here, *after* the
    # condition, instead of letting the request fan out.
    if requested_concepts is not None and not defer_group:
        req_addrs = {c.canonical_address for c in requested_concepts}
        output_addrs = {c.canonical_address for c in node.usable_outputs}
        condition_applied = (
            node.conditions is not None or node.preexisting_conditions is not None
        )
        node_grain = (
            BuildGrain.from_concepts(node.grain.components, environment=environment)
            if node.grain
            else None
        )
        requested_grain = BuildGrain.from_concepts(
            requested_concepts, environment=environment
        )
        if (
            req_addrs
            and req_addrs.issubset(output_addrs)
            and condition_applied
            and node_grain is not None
            and not node_grain.issubset(requested_grain)
        ):
            grouped_output = [
                c for c in node.output_concepts if c.canonical_address in req_addrs
            ]
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} regrouping widened source to "
                f"requested grain {[c.address for c in grouped_output]} after "
                "condition application"
            )
            return GroupNode(
                output_concepts=grouped_output,
                input_concepts=node.output_concepts,
                environment=environment,
                parents=[node],
                depth=depth + 1,
                partial_concepts=[
                    c for c in node.partial_concepts if c.canonical_address in req_addrs
                ],
                nullable_concepts=[
                    c
                    for c in node.nullable_concepts
                    if c.canonical_address in req_addrs
                ],
                preexisting_conditions=node.preexisting_conditions,
                force_group=True,
            )

    if candidate.force_group is True and not defer_group:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} source requires group before consumption."
        )
        return GroupNode(
            output_concepts=candidate.node.output_concepts,
            input_concepts=candidate.node.output_concepts,
            environment=environment,
            parents=[candidate.node],
            depth=depth + 1,
            partial_concepts=candidate.node.partial_concepts,
            nullable_concepts=candidate.node.nullable_concepts,
            preexisting_conditions=candidate.node.preexisting_conditions,
            force_group=candidate.force_group,
        )
    if candidate.force_group is True and defer_group:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} deferring source group until "
            "single grouped source merge can resolve grain."
        )
    return candidate.node


def create_select_node_candidate(
    ds_name: str,
    subgraph: list[str],
    accept_partial: bool,
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> SourceNodeCandidate:
    all_concepts = [
        environment.canonical_concepts[extract_address(c)]
        for c in subgraph
        if c.startswith("c~")
    ]

    if all(c.derivation == Derivation.CONSTANT for c in all_concepts):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} All concepts {[x.address for x in all_concepts]} are constants, returning constant node"
        )
        return SourceNodeCandidate(
            node=ConstantNode(
                output_concepts=all_concepts,
                input_concepts=[],
                environment=environment,
                parents=[],
                depth=depth,
                partial_concepts=[],
                force_group=False,
                preexisting_conditions=conditions.conditional if conditions else None,
            ),
            force_group=False,
            group_source_count=0,
            conditions_deferred=False,
        )

    datasource: BuildDatasource | BuildUnionDatasource = g.datasources[ds_name]

    if isinstance(datasource, BuildDatasource):
        bcandidate, force_group = create_datasource_node(
            datasource,
            all_concepts,
            accept_partial,
            environment,
            depth,
            conditions=conditions,
        )
        return SourceNodeCandidate(
            node=bcandidate,
            force_group=force_group,
            group_source_count=1 if force_group else 0,
            conditions_deferred=conditions is not None
            and (
                bcandidate.preexisting_conditions is None
                or not condition_implies(
                    bcandidate.preexisting_conditions, conditions.conditional
                )
            ),
        )
    elif isinstance(datasource, BuildUnionDatasource):
        bcandidate, force_group, group_source_count = create_union_datasource_candidate(
            datasource,
            all_concepts,
            accept_partial,
            environment,
            depth,
            conditions=conditions,
        )
        return SourceNodeCandidate(
            node=bcandidate,
            force_group=force_group,
            group_source_count=group_source_count,
            conditions_deferred=conditions is not None
            and (
                bcandidate.preexisting_conditions is None
                or not condition_implies(
                    bcandidate.preexisting_conditions, conditions.conditional
                )
            ),
        )
    else:
        raise ValueError(f"Unknown datasource type {datasource}")


def create_select_node(
    ds_name: str,
    subgraph: list[str],
    accept_partial: bool,
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
    defer_group: bool = False,
) -> StrategyNode:
    candidate = create_select_node_candidate(
        ds_name,
        subgraph,
        accept_partial,
        g,
        environment,
        depth,
        conditions,
    )
    return finalize_select_node(candidate, environment, depth, defer_group)


def create_datasource_node(
    datasource: BuildDatasource,
    all_concepts: list[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
    injected_conditions: ConditionExpression | None = None,
) -> tuple[StrategyNode, bool]:
    target_grain = BuildGrain.from_concepts(all_concepts, environment=environment)
    datasource_grain = BuildGrain.from_concepts(
        datasource.grain.components, environment=environment
    )
    force_group = False
    if not datasource_grain.issubset(target_grain):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX}_DS_NODE Select node must be wrapped in group, {datasource_grain} not subset of target grain {target_grain} from {all_concepts}"
        )
        force_group = True
    else:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX}_DS_NODE Select node grain {datasource_grain} is subset of target grain {target_grain}, no group required"
        )
    if not datasource_grain.components:
        force_group = any(
            x.granularity != Granularity.SINGLE_ROW for x in datasource.output_concepts
        )
    if any(
        c.is_aggregate
        and isinstance(c.lineage, BuildAggregateWrapper)
        and c.lineage.grouping != AggregateGroupingMode.STANDARD
        for c in all_concepts
    ):
        force_group = True
    rollup_concepts = (
        get_additive_rollup_concepts(
            datasource=datasource,
            requested_concepts=all_concepts,
            concepts_by_address=environment.concepts,
            datasources=[
                ds
                for ds in environment.datasources.values()
                if isinstance(ds, BuildDatasource)
            ],
            target_grain=target_grain,
            conditions=conditions,
        )
        if force_group
        else []
    )
    rollup_addresses = {c.address for c in rollup_concepts}
    datasource_output_addresses = {c.address for c in datasource.output_concepts}

    output_concepts = [
        c
        for c in all_concepts
        if not c.is_aggregate
        or c.address in rollup_addresses
        or datasource_grain.issubset(c.grain)
        or (c.is_aggregate and c.address in datasource_output_addresses)
    ]
    output_addresses = {c.address for c in output_concepts}
    partial_concepts = [
        c.concept
        for c in datasource.columns
        if not c.is_complete and c.concept.address in output_addresses
    ]

    partial_lcl = CanonicalBuildConceptList(concepts=partial_concepts)
    nullable_concepts = [
        c.concept
        for c in datasource.columns
        if c.is_nullable and c.concept.address in output_addresses
    ]

    nullable_lcl = CanonicalBuildConceptList(concepts=nullable_concepts)
    partial_is_full = bool(
        conditions
        and datasource.non_partial_for
        and condition_implies(
            conditions.conditional, datasource.non_partial_for.conditional
        )
    )

    routed_conditions = datasource_conditions(
        datasource, conditions, injected_conditions, partial_is_full
    )

    all_inputs = [c.concept for c in datasource.columns]
    canonical_all = CanonicalBuildConceptList(concepts=all_inputs)

    for x in all_concepts:
        if x not in all_inputs and x in canonical_all:
            all_inputs.append(x)
    all_inputs = [
        c for c in all_inputs if not c.is_aggregate or c.address in output_addresses
    ]

    satisfies_conditions = not datasource_has_filter_sensitive_aggregate(
        datasource, conditions
    ) and all(
        x.granularity == Granularity.SINGLE_ROW for x in datasource.output_concepts
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} creating select node for datasource {datasource.name} with conditions {routed_conditions}, "
        f"partial_is_full {partial_is_full}, satisfies_conditions {satisfies_conditions}, "
        f"force_group {force_group}"
    )
    rval = SelectNode(
        input_concepts=all_inputs,
        output_concepts=sorted(output_concepts, key=lambda x: x.address),
        environment=environment,
        parents=[],
        depth=depth,
        partial_concepts=(
            [] if partial_is_full else [c for c in output_concepts if c in partial_lcl]
        ),
        rollup_concepts=rollup_concepts,
        nullable_concepts=[c for c in output_concepts if c in nullable_lcl],
        accept_partial=accept_partial,
        datasource=datasource,
        grain=datasource.grain,
        conditions=routed_conditions,
        preexisting_conditions=preexisting_conditions(
            datasource, conditions, partial_is_full, satisfies_conditions
        ),
    )
    return rval, force_group


def create_union_datasource_candidate(
    datasource: BuildUnionDatasource,
    all_concepts: list[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple["UnionNode", bool, int]:
    from trilogy.core.processing.nodes.union_node import UnionNode

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating union node parents with condition {conditions}"
    )

    effective: list[tuple[BuildDatasource, ConditionExpression | None]]
    if conditions:
        qcond = conditions.conditional
        non_partial_map = {
            child.name: child.non_partial_for for child in datasource.children
        }
        kept = filter_union_children(non_partial_map, qcond)
        for child in datasource.children:
            if child.name not in kept:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} dropping {child.name}: "
                    f"non_partial_for {child.non_partial_for!r} mutually exclusive with {qcond!r}"
                )
        effective = [
            (child, kept[child.name])
            for child in datasource.children
            if child.name in kept
        ]
        if len(effective) < len(datasource.children):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} reduced union from {len(datasource.children)} "
                f"to {len(effective)} branch(es)"
            )
    else:
        effective = [(child, None) for child in datasource.children]

    force_group = False
    group_source_count = 0
    parents = []
    for child, injected_cond in effective:
        subnode, fg = create_datasource_node(
            child,
            all_concepts,
            accept_partial,
            environment,
            depth + 1,
            injected_conditions=injected_cond,
        )
        parents.append(subnode)
        force_group = force_group or fg
        if fg:
            group_source_count = max(group_source_count, 1)
    # Intrinsic column-level partials (``~col``, captured at parse time) carry
    # over by default — the column is missing values relative to its universe
    # and the union doesn't necessarily repair that. Two cases DO repair it:
    #
    #   1. The intrinsic concept itself is the union's discriminator. The
    #      siblings' ``complete_where`` clauses partition that concept; the
    #      union covers the universe of the concept directly. (e.g. enum
    #      union over ``~category`` with each branch ``complete where
    #      category = '<value>'``.)
    #
    #   2. The union's discriminator is a property/key whose owner is the
    #      intrinsic concept — i.e., every value of the intrinsic concept has
    #      a determined discriminator value. Covering every discriminator
    #      value therefore covers every intrinsic-concept value. (e.g.
    #      ``~order_id`` with each branch ``complete where order_date <= X``
    #      / ``> X``: every order has a date, dates are partitioned, so all
    #      orders are covered.)
    #
    # Intrinsic partials that fail both checks survive — e.g. ``~order_id``
    # on returns: ``complete where sales_channel = 'X'`` covers all channels
    # but not all orders, so order_id stays partial.
    intrinsic_addrs: set[str] = set()
    discriminator_addrs: set[str] = set()
    for child, _ in effective:
        intrinsic_addrs |= child.column_level_partial_addresses
        if child.non_partial_for is not None:
            discriminator_addrs.update(
                c.address for c in child.non_partial_for.concept_arguments
            )
    if intrinsic_addrs and discriminator_addrs:
        intrinsic_addrs -= discriminator_addrs
        for addr in list(intrinsic_addrs):
            for disc_addr in discriminator_addrs:
                disc_concept = environment.concepts.get(disc_addr)
                if (
                    disc_concept is not None
                    and disc_concept.keys
                    and addr in disc_concept.keys
                ):
                    intrinsic_addrs.discard(addr)
                    break
    union_partials: list[BuildConcept] = (
        [c for c in all_concepts if c.address in intrinsic_addrs]
        if intrinsic_addrs
        else []
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} returning union node with {len(parents)} branch(es)"
    )
    return (
        UnionNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            partial_concepts=union_partials,
            preexisting_conditions=conditions.conditional if conditions else None,
        ),
        force_group,
        group_source_count,
    )


def create_union_datasource(
    datasource: BuildUnionDatasource,
    all_concepts: list[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple["UnionNode", bool]:
    node, force_group, _ = create_union_datasource_candidate(
        datasource,
        all_concepts,
        accept_partial,
        environment,
        depth,
        conditions,
    )
    return node, force_group
