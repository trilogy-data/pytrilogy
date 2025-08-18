from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import (
    BuildConcept,
    BuildFilterItem,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)
from trilogy.core.processing.nodes import (
    FilterNode,
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.utility import is_scalar_condition, padding, unique

LOGGER_PREFIX = "[GEN_FILTER_NODE]"

FILTER_TYPES = (BuildFilterItem,)


def pushdown_filter_to_parent(
    local_optional: List[BuildConcept],
    conditions: BuildWhereClause | None,
    filter_where: BuildWhereClause,
    same_filter_optional: list[BuildConcept],
    depth: int,
) -> bool:
    optimized_pushdown = False
    if not is_scalar_condition(filter_where.conditional):
        optimized_pushdown = False
    elif not local_optional:
        optimized_pushdown = True
    elif conditions and conditions == filter_where:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} query conditions are the same as filter conditions, can optimize across all concepts"
        )
        optimized_pushdown = True
    elif same_filter_optional == local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} all optional concepts are included in the filter, can optimize across all concepts"
        )
        optimized_pushdown = True

    return optimized_pushdown


def build_parent_concepts(
    concept: BuildConcept,
    environment: BuildEnvironment,
    local_optional: List[BuildConcept],
    conditions: BuildWhereClause | None = None,
    depth: int = 0,
) -> tuple[
    list[BuildConcept],
    list[tuple[BuildConcept, ...]],
    list[BuildConcept],
    bool,
    bool,
]:
    parent_row_concepts, parent_existence_concepts = resolve_filter_parent_concepts(
        concept, environment
    )
    if not isinstance(concept.lineage, FILTER_TYPES):
        raise SyntaxError('Filter node must have a filter type lineage"')
    filter_where = concept.lineage.where

    same_filter_optional: list[BuildConcept] = []
    # mypy struggled here? we shouldn't need explicit bools
    global_filter_is_local_filter: bool = (
        True if (conditions and conditions == filter_where) else False
    )

    exact_partial_matches = True
    for x in local_optional:
        if isinstance(x.lineage, FILTER_TYPES):
            if set([x.address for x in x.lineage.where.concept_arguments]) == set(
                [x.address for x in filter_where.concept_arguments]
            ):
                exact_partial_matches = (
                    exact_partial_matches and x.lineage.where == filter_where
                )
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} fetching parents for peer {x.address} (of {concept.address})"
                )

                for arg in x.lineage.content_concept_arguments:
                    if arg.address not in parent_row_concepts:
                        parent_row_concepts.append(arg)
                same_filter_optional.append(x)
                continue
        elif global_filter_is_local_filter:
            same_filter_optional.append(x)

    # sometimes, it's okay to include other local optional above the filter
    # in case it is, prep our list
    extra_row_level_optional: list[BuildConcept] = []

    for x in local_optional:
        if x.address in same_filter_optional:
            continue
        extra_row_level_optional.append(x)
    is_optimized_pushdown = exact_partial_matches and pushdown_filter_to_parent(
        local_optional, conditions, filter_where, same_filter_optional, depth
    )
    if not is_optimized_pushdown:
        parent_row_concepts += extra_row_level_optional
    return (
        parent_row_concepts,
        parent_existence_concepts,
        same_filter_optional,
        is_optimized_pushdown,
        global_filter_is_local_filter,
    )


def add_existence_sources(
    core_parent_nodes: list[StrategyNode],
    parent_existence_concepts: list[tuple[BuildConcept, ...]],
    source_concepts,
    environment,
    g,
    depth,
    history,
):
    for existence_tuple in parent_existence_concepts:
        if not existence_tuple:
            continue
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching filter node existence parents {[x.address for x in existence_tuple]}"
        )
        parent_existence = source_concepts(
            mandatory_list=list(existence_tuple),
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not parent_existence:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} filter existence node parents could not be found"
            )
            return None
        core_parent_nodes.append(parent_existence)


def gen_filter_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    if not isinstance(concept.lineage, FILTER_TYPES):
        raise SyntaxError('Filter node must have a filter type lineage"')
    where = concept.lineage.where

    (
        parent_row_concepts,
        parent_existence_concepts,
        same_filter_optional,
        optimized_pushdown,
        global_filter_is_local_filter,
    ) = build_parent_concepts(
        concept,
        environment=environment,
        local_optional=local_optional,
        conditions=conditions,
        depth=depth,
    )

    row_parent: StrategyNode = source_concepts(
        mandatory_list=parent_row_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )

    core_parent_nodes: list[StrategyNode] = []
    flattened_existence = [x for y in parent_existence_concepts for x in y]
    if parent_existence_concepts:
        add_existence_sources(
            core_parent_nodes,
            parent_existence_concepts,
            source_concepts,
            environment,
            g,
            depth,
            history,
        )

    if not row_parent:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node row parents {[x.address for x in parent_row_concepts]} could not be found"
        )
        return None
    if global_filter_is_local_filter:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node conditions match global conditions adding row parent {row_parent.output_concepts} with condition {where.conditional}"
        )
        row_parent.add_parents(core_parent_nodes)
        row_parent.set_output_concepts([concept] + local_optional)
        return row_parent
    if optimized_pushdown:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} returning optimized filter node with pushdown to parent with condition {where.conditional} across {[concept] + same_filter_optional + row_parent.output_concepts} "
        )
        if isinstance(row_parent, SelectNode):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} nesting select node in strategy node"
            )
            parent = StrategyNode(
                input_concepts=row_parent.output_concepts,
                output_concepts=[concept]
                + same_filter_optional
                + row_parent.output_concepts,
                environment=row_parent.environment,
                parents=[row_parent],
                depth=row_parent.depth,
                partial_concepts=row_parent.partial_concepts,
                force_group=False,
            )
        else:
            parent = row_parent
            parent.add_output_concepts([concept] + same_filter_optional)
        parent.add_parents(core_parent_nodes)
        if not parent.preexisting_conditions == where.conditional:
            parent.add_condition(where.conditional)
        parent.add_existence_concepts(flattened_existence, False)
        parent.grain = BuildGrain.from_concepts(
            parent.output_concepts,
            environment=environment,
        )
        parent.rebuild_cache()
        filter_node = parent
    else:
        core_parent_nodes.append(row_parent)
        filters = [concept] + same_filter_optional
        parents_for_grain = [
            x.lineage.content
            for x in filters
            if isinstance(x.lineage, BuildFilterItem)
            and isinstance(x.lineage.content, BuildConcept)
        ]
        filter_node = FilterNode(
            input_concepts=unique(
                parent_row_concepts + flattened_existence,
                "address",
            ),
            output_concepts=[concept] + same_filter_optional + parent_row_concepts,
            environment=environment,
            parents=core_parent_nodes,
            grain=BuildGrain.from_concepts(
                parents_for_grain + parent_row_concepts, environment=environment
            ),
            preexisting_conditions=conditions.conditional if conditions else None,
        )

    if not local_optional or all(
        [x.address in filter_node.output_concepts for x in local_optional]
    ):
        optional_outputs = [
            x for x in filter_node.output_concepts if x.address in local_optional
        ]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for filter node, has all of {[x.address for x in local_optional]}"
        )
        filter_node.set_output_concepts(
            [
                concept,
            ]
            + optional_outputs
        )
        return filter_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} need to enrich filter node with additional concepts {[x.address for x in local_optional if x.address not in filter_node.output_concepts]}"
    )
    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=parent_row_concepts
        + [x for x in local_optional if x.address not in filter_node.output_concepts],
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich_node:
        logger.error(
            f"{padding(depth)}{LOGGER_PREFIX} filter node enrichment node could not be found"
        )
        return filter_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} returning filter node and enrich node with {enrich_node.output_concepts} and {enrich_node.input_concepts}"
    )
    return MergeNode(
        input_concepts=filter_node.output_concepts + enrich_node.output_concepts,
        output_concepts=[
            concept,
        ]
        + local_optional,
        environment=environment,
        parents=[
            filter_node,
            enrich_node,
        ],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
