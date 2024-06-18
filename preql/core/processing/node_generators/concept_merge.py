from preql.core.models import (
    Concept,
    Environment,
    MergeStatement,
)
from preql.core.processing.nodes import MergeNode, NodeJoin, History
from preql.core.processing.nodes.base_node import concept_list_to_grain, StrategyNode
from typing import List

from preql.core.enums import JoinType
from preql.constants import logger
from preql.core.processing.utility import padding
from preql.core.processing.node_generators.common import concept_to_relevant_joins
from itertools import combinations
from preql.core.processing.node_generators.common import resolve_join_order

LOGGER_PREFIX = "[GEN_CONCEPT_MERGE_NODE]"


def merge_joins(base: MergeStatement, parents: List[StrategyNode]) -> List[NodeJoin]:
    output = []
    for left, right in combinations(parents, 2):
        output.append(
            NodeJoin(
                left_node=left,
                right_node=right,
                concepts=[
                    base.merge_concept,
                ],
                join_type=JoinType.FULL,
            )
        )
    return resolve_join_order(output)


def gen_concept_merge_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> MergeNode | None:
    if not isinstance(concept.lineage, MergeStatement):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate merge node for {concept}"
        )
        return None
    lineage: MergeStatement = concept.lineage

    base_parents: List[StrategyNode] = []
    for select in lineage.concepts:
        # if it's a merge concept, filter it out of the optional
        sub_optional = [
            x
            for x in local_optional
            if x.address not in lineage.concepts_lcl and x.namespace == select.namespace
        ]
        snode: StrategyNode = source_concepts(
            mandatory_list=[select] + sub_optional,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Cannot generate merge node for {concept}"
            )
            return None
        snode.add_output_concept(lineage.merge_concept)
        base_parents.append(snode)

    node_joins = merge_joins(lineage, base_parents)

    enrichment = set([x.address for x in local_optional])
    outputs = [x for y in base_parents for x in y.output_concepts]

    additional_relevant = [x for x in outputs if x.address in enrichment]
    node = MergeNode(
        input_concepts=[x for y in base_parents for x in y.output_concepts],
        output_concepts=outputs + additional_relevant + [concept],
        environment=environment,
        g=g,
        depth=depth,
        parents=base_parents,
        node_joins=node_joins,
    )

    qds = node.rebuild_cache()

    # assume grain to be outoput of select
    # but don't include anything aggregate at this point
    qds.grain = concept_list_to_grain(
        node.output_concepts, parent_sources=qds.datasources
    )
    possible_joins = concept_to_relevant_joins(additional_relevant)
    if not local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enriched required for merge concept node; exiting early"
        )
        return node
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for merge concept node; exiting early"
        )
        return node
    if all(
        [x.address in [y.address for y in node.output_concepts] for x in local_optional]
    ):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} all enriched concepts returned from base merge concept node; exiting early"
        )
        return node
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=additional_relevant + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate merge concept enrichment node for {concept} with optional {local_optional}, returning just merge concept"
        )
        return node

    return MergeNode(
        input_concepts=enrich_node.output_concepts + node.output_concepts,
        output_concepts=node.output_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth,
        parents=[
            # this node gets the window
            node,
            # this node gets enrichment
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=enrich_node,
                right_node=node,
                concepts=possible_joins,
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
        partial_concepts=node.partial_concepts,
    )
