from typing import List


from trilogy.core.enums import JoinType
from trilogy.core.models import Concept, Environment, FilterItem, Grain
from trilogy.core.processing.nodes import (
    FilterNode,
    MergeNode,
    NodeJoin,
    History,
    StrategyNode,
)
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
    resolve_condition_parent_concepts
)
from trilogy.constants import logger
from trilogy.core.processing.utility import padding, unique
from trilogy.core.processing.node_generators.common import concept_to_relevant_joins

LOGGER_PREFIX = "[GEN_FILTER_NODE]"


def gen_filter_node(
    concepts: List[Concept],
    condition: FilterItem,
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> StrategyNode | None:
    parent_row_concepts, parent_existence_concepts = (
        resolve_condition_parent_concepts(condition)
    )

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} fetching filter node row parents {[x.address for x in parent_row_concepts]}"
    )
    core_parents = []
    parent: StrategyNode = source_concepts(
        mandatory_list=concepts+ parent_row_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
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
    if not parent:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node row parents {[x.address for x in parent_row_concepts]} could not be found"
        )
        return None
    
    non_parent_optional = [x for x in local_optional if x.address not in [y.address for y in parent_row_concepts]]
    if not non_parent_optional:
        optimized_pushdown = True
    else:
        optimized_pushdown = False
    if optimized_pushdown:
        if parent.conditions:
            parent.conditions = parent.conditions + where.conditional
        else:
            parent.conditions = where.conditional
        # add our existence concepts to the parent
        parent.parents +=core_parents
        parent.output_concepts = [concept] + local_optional
        parent.input_concepts = parent.input_concepts +flattened_existence
        parent.existence_concepts = (parent.existence_concepts or []) + flattened_existence
        parent.grain = Grain(components=list(immediate_parent.keys) if immediate_parent.keys else [immediate_parent])
        parent.rebuild_cache()

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} returning optimized filter node with pushdown to parent with condition {where.conditional}"
        )
        return parent
    # append this now
    core_parents.append(parent)

    filter_node = FilterNode(
        input_concepts=unique(
            [immediate_parent] + parent_row_concepts + flattened_existence,
            "address",
        ),
        output_concepts=[concept, immediate_parent] + parent_row_concepts,
        environment=environment,
        g=g,
        parents=core_parents,
        grain=Grain(
            components=[immediate_parent] + parent_row_concepts,
        ),
    )

    assert filter_node.resolve().grain == Grain(
        components=[immediate_parent] + parent_row_concepts,
    )
    if not local_optional or all(
        [x.address in [y.address for y in parent_row_concepts] for x in local_optional]
    ):
        outputs = [
            x
            for x in filter_node.output_concepts
            if x.address in [y.address for y in local_optional]
        ]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for filter node"
        )
        filter_node.output_concepts = [
            concept,
        ] + outputs
        filter_node.rebuild_cache()
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
