from typing import List


from preql.core.enums import JoinType
from preql.core.models import (
    Concept,
    Environment,
    FilterItem,
)
from preql.core.processing.nodes import FilterNode, MergeNode, NodeJoin, History
from preql.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)
from preql.constants import logger
from preql.core.processing.utility import padding
from preql.core.processing.node_generators.common import concept_to_relevant_joins

LOGGER_PREFIX = "[GEN_FILTER_NODE]"


def gen_filter_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> MergeNode | FilterNode | None:
    immediate_parent, parent_concepts = resolve_filter_parent_concepts(concept)

    logger.info(f"{padding(depth)}{LOGGER_PREFIX} fetching filter node parents")
    parent = source_concepts(
        mandatory_list=parent_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    if not parent:
        return None
    filter_node = FilterNode(
        input_concepts=[immediate_parent] + parent_concepts,
        output_concepts=[concept, immediate_parent] + parent_concepts,
        environment=environment,
        g=g,
        parents=[parent],
        conditions=(
            concept.lineage.where.conditional
            if isinstance(concept.lineage, FilterItem)
            else None
        ),
        partial_concepts=[immediate_parent],
    )
    if not local_optional:
        return filter_node
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=[immediate_parent] + parent_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    x = MergeNode(
        input_concepts=[concept, immediate_parent] + local_optional,
        output_concepts=[concept, immediate_parent] + local_optional,
        environment=environment,
        g=g,
        parents=[
            # this node fetches only what we need to filter
            filter_node,
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=filter_node,
                right_node=enrich_node,
                concepts=concept_to_relevant_joins(
                    [immediate_parent] + parent_concepts
                ),
                join_type=JoinType.INNER,
                filter_to_mutual=False,
            )
        ],
        # all of these concepts only count as partial
        partial_concepts=[immediate_parent] + local_optional,
        # we should not need to implicitly group here
        force_group=False,
    )
    return x
