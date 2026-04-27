from typing import TYPE_CHECKING

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.graph_models import (
    ReferenceGraph,
    datasource_has_filter_sensitive_aggregate,
)
from trilogy.core.models.build import (
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


def extract_address(node: str) -> str:
    return node.split("~")[1].split("@")[0]


def create_select_node(
    ds_name: str,
    subgraph: list[str],
    accept_partial: bool,
    g: ReferenceGraph,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode:
    all_concepts = [
        environment.canonical_concepts[extract_address(c)]
        for c in subgraph
        if c.startswith("c~")
    ]

    if all(c.derivation == Derivation.CONSTANT for c in all_concepts):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} All concepts {[x.address for x in all_concepts]} are constants, returning constant node"
        )
        return ConstantNode(
            output_concepts=all_concepts,
            input_concepts=[],
            environment=environment,
            parents=[],
            depth=depth,
            partial_concepts=[],
            force_group=False,
            preexisting_conditions=conditions.conditional if conditions else None,
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
    elif isinstance(datasource, BuildUnionDatasource):
        bcandidate, force_group = create_union_datasource(
            datasource,
            all_concepts,
            accept_partial,
            environment,
            depth,
            conditions=conditions,
        )
    else:
        raise ValueError(f"Unknown datasource type {datasource}")

    if force_group is True:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} source requires group before consumption."
        )
        candidate: StrategyNode = GroupNode(
            output_concepts=all_concepts,
            input_concepts=all_concepts,
            environment=environment,
            parents=[bcandidate],
            depth=depth + 1,
            partial_concepts=bcandidate.partial_concepts,
            nullable_concepts=bcandidate.nullable_concepts,
            preexisting_conditions=bcandidate.preexisting_conditions,
            force_group=force_group,
        )
    else:
        candidate = bcandidate

    return candidate


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


def create_union_datasource(
    datasource: BuildUnionDatasource,
    all_concepts: list[BuildConcept],
    accept_partial: bool,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple["UnionNode", bool]:
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
            partial_concepts=[],
            preexisting_conditions=conditions.conditional if conditions else None,
        ),
        force_group,
    )
