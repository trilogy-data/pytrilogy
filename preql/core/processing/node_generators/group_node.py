from preql.core.models import Concept, Environment
from preql.utility import unique
from preql.core.processing.nodes import GroupNode, StrategyNode, MergeNode, NodeJoin
from typing import List
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)

from preql.core.enums import JoinType
from preql.constants import logger
from preql.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_GROUP_NODE]"


def gen_group_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
):
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts: List[Concept] = unique(
        resolve_function_parent_concepts(concept), "address"
    )

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    output_concepts = [concept]

    if concept.grain and len(concept.grain.components_copy) > 0:
        grain_components = (
            concept.grain.components_copy if not concept.grain.abstract else []
        )
        parent_concepts += grain_components
        output_concepts += grain_components

    if parent_concepts:
        parent_concepts = unique(parent_concepts, "address")
        parents: List[StrategyNode] = [
            source_concepts(
                mandatory_list=parent_concepts,
                environment=environment,
                g=g,
                depth=depth + 1,
            )
        ]
    else:
        parents = []

    # the keys we group by
    # are what we can use for enrichment
    group_key_parents = concept.grain.components_copy

    group_node = GroupNode(
        output_concepts=output_concepts,
        input_concepts=parent_concepts,
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
    )

    # early exit if no optional
    if not local_optional:
        return group_node

    # exit early if enrichment is irrelevant.
    if set([x.address for x in local_optional]).issubset(
        set([y.address for y in parent_concepts])
    ):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} group by node has required parents {[x.address for x in parent_concepts]}"
        )
        return group_node
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=group_key_parents + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
    )
    return MergeNode(
        input_concepts=group_key_parents + local_optional + output_concepts,
        output_concepts=output_concepts + local_optional,
        environment=environment,
        g=g,
        parents=[enrich_node, group_node],
        node_joins=[
            NodeJoin(
                left_node=enrich_node,
                right_node=group_node,
                concepts=group_key_parents,
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
    )
