from preql.core.models import Concept, Environment, Function
from preql.core.processing.nodes import GroupNode, StrategyNode, MergeNode, NodeJoin
from typing import List
from preql.core.enums import JoinType

from preql.constants import logger
from preql.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_GROUP_TO_NODE]"


def gen_group_to_node(
    concept: Concept,
    local_optional,
    environment: Environment,
    g,
    depth: int,
    source_concepts,
) -> GroupNode | MergeNode:
    # aggregates MUST always group to the proper grain
    if not isinstance(concept.lineage, Function):
        raise SyntaxError("Group to should have function lineage")
    parent_concepts: List[Concept] = concept.lineage.concept_arguments
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} group by node has required parents {[x.address for x in parent_concepts]}"
    )
    parents: List[StrategyNode] = [
        source_concepts(
            mandatory_list=parent_concepts,
            environment=environment,
            g=g,
            depth=depth + 1,
        )
    ]

    group_node = GroupNode(
        output_concepts=parent_concepts + [concept],
        input_concepts=parent_concepts,
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
    )

    # early exit if no optional
    if not local_optional:
        return group_node

    # the keys we group by
    # are what we can use for enrichment
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=parent_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
    )

    return MergeNode(
        input_concepts=[concept]
        + local_optional
        + [x for x in parent_concepts if x.address != concept.address],
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=[
            # this node gets the group
            group_node,
            # this node gets enrichment
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=group_node,
                right_node=enrich_node,
                concepts=parent_concepts,
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
        whole_grain=True,
        depth=depth,
        partial_concepts=group_node.partial_concepts,
    )
