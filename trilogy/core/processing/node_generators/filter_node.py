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
    immediate_parent, parent_row_concepts, parent_existence_concepts = (
        resolve_filter_parent_concepts(concept, environment)
    )
    if not isinstance(concept.lineage, FILTER_TYPES):
        raise SyntaxError('Filter node must have a filter type lineage"')
    where = concept.lineage.where

    optional_included: list[BuildConcept] = []

    for x in local_optional:
        if isinstance(x.lineage, FILTER_TYPES):
            if concept.lineage.where == where:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} fetching {x.lineage.content.address} as optional parent with same filter conditions "
                )
                parent_row_concepts.append(x.lineage.content)
                optional_included.append(x)
                continue
        if conditions and conditions == where:
            optional_included.append(x)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} filter `{concept}` condition `{concept.lineage.where}` derived from {immediate_parent.address} row parents {[x.address for x in parent_row_concepts]} and {[[y.address] for x  in parent_existence_concepts for y  in x]} existence parents"
    )
    # we'll populate this with the row parent
    # and the existence parent(s)
    core_parents = []

    row_parent: StrategyNode = source_concepts(
        mandatory_list=parent_row_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )

    flattened_existence = [x for y in parent_existence_concepts for x in y]
    if parent_existence_concepts:
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
            core_parents.append(parent_existence)
    if not row_parent:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node row parents {[x.address for x in parent_row_concepts]} could not be found"
        )
        return None

    optimized_pushdown = False
    if not is_scalar_condition(where.conditional):
        optimized_pushdown = False
    elif not local_optional:
        optimized_pushdown = True
    elif conditions and conditions == where:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} query conditions are the same as filter conditions, can optimize across all concepts"
        )
        optimized_pushdown = True
    elif optional_included == local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} all optional concepts are included in the filter, can optimize across all concepts"
        )
        optimized_pushdown = True
    if optimized_pushdown:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} returning optimized filter node with pushdown to parent with condition {where.conditional}"
        )
        if isinstance(row_parent, SelectNode):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} nesting select node in strategy node"
            )
            parent = StrategyNode(
                input_concepts=row_parent.output_concepts,
                output_concepts=[concept] + row_parent.output_concepts,
                environment=row_parent.environment,
                parents=[row_parent],
                depth=row_parent.depth,
                partial_concepts=row_parent.partial_concepts,
                force_group=False,
            )
        else:
            parent = row_parent

        expected_output = [concept] + [
            x
            for x in local_optional
            if x.address in [y for y in parent.output_concepts]
            or x.address in [y for y in optional_included]
        ]
        parent.add_parents(core_parents)
        parent.add_condition(where.conditional)
        parent.add_existence_concepts(flattened_existence, False).set_output_concepts(
            expected_output, False
        )
        parent.grain = BuildGrain.from_concepts(
            (
                [environment.concepts[k] for k in immediate_parent.keys]
                if immediate_parent.keys
                else [immediate_parent]
            )
            + [
                x
                for x in local_optional
                if x.address in [y.address for y in parent.output_concepts]
            ],
            environment=environment,
        )
        parent.rebuild_cache()
        filter_node = parent
    else:
        core_parents.append(row_parent)

        filter_node = FilterNode(
            input_concepts=unique(
                [immediate_parent] + parent_row_concepts + flattened_existence,
                "address",
            ),
            output_concepts=[concept, immediate_parent] + parent_row_concepts,
            environment=environment,
            parents=core_parents,
            grain=BuildGrain.from_concepts(
                [immediate_parent] + parent_row_concepts,
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

    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=[immediate_parent] + parent_row_concepts + local_optional,
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
        input_concepts=[concept, immediate_parent] + local_optional,
        output_concepts=[
            concept,
        ]
        + local_optional,
        environment=environment,
        parents=[
            # this node fetches only what we need to filter
            filter_node,
            enrich_node,
        ],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
