from preql.core.models import Concept, Environment, Select, RowsetDerivation, RowsetItem
from preql.core.processing.nodes import MergeNode, NodeJoin
from typing import List

from preql.core.enums import JoinType
from preql.constants import logger
from preql.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def gen_rowset_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
) -> MergeNode | None:
    if not isinstance(concept.lineage, RowsetItem):
        raise SyntaxError(
            f"Invalid lineage passed into rowset fetch, got {type(concept.lineage)}, expected {RowsetItem}"
        )
    lineage: RowsetItem = concept.lineage
    rowset: RowsetDerivation = lineage.rowset
    select: Select = lineage.rowset.select
    node: MergeNode = source_concepts(
        mandatory_list=select.output_components,
        environment=environment,
        g=g,
        depth=depth + 1,
    )
    if select.where_clause:
        node.conditions = select.where_clause.conditional
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
    # we need a better API for refreshing a nodes QDS
    node.resolution_cache = node._resolve()
    if not local_optional:
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
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset enrichment node for {concept} with optional {local_optional}"
        )
        return None
    return MergeNode(
        input_concepts=enrich_node.output_concepts + node.output_concepts,
        output_concepts=node.output_concepts + local_optional,
        environment=environment,
        g=g,
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
                concepts=additional_relevant,
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
        partial_concepts=node.partial_concepts,
    )
