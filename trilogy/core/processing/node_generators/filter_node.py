from typing import List


from trilogy.core.enums import JoinType
from trilogy.core.models import Concept, Environment, FilterItem
from trilogy.core.processing.nodes import (
    FilterNode,
    MergeNode,
    NodeJoin,
    History,
    StrategyNode,
)
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)
from trilogy.constants import logger
from trilogy.core.processing.utility import padding, unique
from trilogy.core.processing.node_generators.common import concept_to_relevant_joins

LOGGER_PREFIX = "[GEN_FILTER_NODE]"


def gen_filter_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> StrategyNode | None:
    immediate_parent, parent_row_concepts, parent_existence_concepts = (
        resolve_filter_parent_concepts(concept)
    )
    if not isinstance(concept.lineage, FilterItem):
        raise SyntaxError('Filter node must have a lineage of type "FilterItem"')
    where = concept.lineage.where

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} fetching filter node row parents {[x.address for x in parent_row_concepts]}"
    )
    core_parents = []
    parent: StrategyNode = source_concepts(
        mandatory_list=parent_row_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )

    if not parent:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node row parents {[x.address for x in parent_row_concepts]} could not be found"
        )
        return None

    if not local_optional and not parent_existence_concepts:
        optimized_pushdown = True
    else:
        optimized_pushdown = False

    if optimized_pushdown:
        if parent.conditions:
            parent.conditions = parent.conditions + where.conditional
        else:
            parent.conditions = where.conditional
        parent.output_concepts = [concept]
        parent.rebuild_cache()
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} returning optimized filter node with pushdown to parent with condition {where.conditional}"
        )
        return parent

    core_parents.append(parent)
    if parent_existence_concepts:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching filter node existence parents {[x.address for x in parent_existence_concepts]}"
        )
        parent_existence = source_concepts(
            mandatory_list=parent_existence_concepts,
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

    filter_node = FilterNode(
        input_concepts=unique(
            [immediate_parent] + parent_row_concepts + parent_existence_concepts,
            "address",
        ),
        output_concepts=[concept, immediate_parent] + parent_row_concepts,
        environment=environment,
        g=g,
        parents=core_parents,
    )
    if not local_optional or all(
        [x.address in [y.address for y in parent_row_concepts] for x in local_optional]
    ):
        return filter_node
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=[immediate_parent] + parent_row_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    if not enrich_node:
        return filter_node
    x = MergeNode(
        input_concepts=[concept, immediate_parent] + local_optional,
        output_concepts=[
            concept,
        ]
        + local_optional,
        environment=environment,
        g=g,
        parents=[
            # this node fetches only what we need to filter
            filter_node,
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=enrich_node,
                right_node=filter_node,
                concepts=concept_to_relevant_joins(
                    [immediate_parent] + parent_row_concepts
                ),
                join_type=JoinType.LEFT_OUTER,
                filter_to_mutual=False,
            )
        ],
    )
    return x
