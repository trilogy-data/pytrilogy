from preql.core.models import (
    Concept,
    Environment,
    SelectStatement,
    RowsetDerivationStatement,
    RowsetItem,
    MultiSelectStatement,
)
from preql.core.processing.nodes import MergeNode, NodeJoin, History, StrategyNode
from preql.core.processing.nodes.base_node import concept_list_to_grain
from typing import List

from preql.core.enums import JoinType
from preql.constants import logger
from preql.core.processing.utility import padding
from preql.core.processing.node_generators.common import concept_to_relevant_joins


LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def gen_rowset_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> StrategyNode | None:
    if not isinstance(concept.lineage, RowsetItem):
        raise SyntaxError(
            f"Invalid lineage passed into rowset fetch, got {type(concept.lineage)}, expected {RowsetItem}"
        )
    lineage: RowsetItem = concept.lineage
    rowset: RowsetDerivationStatement = lineage.rowset
    select: SelectStatement | MultiSelectStatement = lineage.rowset.select
    node: StrategyNode = source_concepts(
        mandatory_list=select.output_components,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    node.conditions = select.where_clause.conditional if select.where_clause else None
    # rebuild any cached info with the new condition clause
    node.rebuild_cache()
    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset node for {concept}"
        )
        return None
    enrichment = set([x.address for x in local_optional])
    rowset_relevant = [
        x
        for x in rowset.derived_concepts
        if x.address == concept.address or x.address in enrichment
    ]
    additional_relevant = [
        x for x in select.output_components if x.address in enrichment
    ]
    # add in other other concepts
    for item in rowset_relevant:
        node.output_concepts.append(item)
    for item in additional_relevant:
        node.output_concepts.append(item)
    if select.where_clause:
        for item in additional_relevant:
            node.partial_concepts.append(item)

    # assume grain to be outoput of select
    # but don't include anything aggregate at this point
    assert node.resolution_cache
    node.resolution_cache.grain = concept_list_to_grain(
        node.output_concepts, parent_sources=node.resolution_cache.datasources
    )
    possible_joins = concept_to_relevant_joins(additional_relevant)
    if not local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enriched required for rowset node; exiting early"
        )
        return node
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for rowset node; exiting early"
        )
        return node
    if all(
        [x.address in [y.address for y in node.output_concepts] for x in local_optional]
    ):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} all enriched concepts returned from base rowset node; exiting early"
        )
        return node
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=additional_relevant + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset enrichment node for {concept} with optional {local_optional}, returning just rowset node"
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
                concepts=concept_to_relevant_joins(additional_relevant),
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
        partial_concepts=node.partial_concepts,
    )
