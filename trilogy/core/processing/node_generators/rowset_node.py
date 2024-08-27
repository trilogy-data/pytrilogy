from trilogy.core.models import (
    Concept,
    Environment,
    SelectStatement,
    RowsetDerivationStatement,
    RowsetItem,
    MultiSelectStatement,
)
from trilogy.core.processing.nodes import MergeNode, NodeJoin, History, StrategyNode
from trilogy.core.processing.nodes.base_node import concept_list_to_grain
from typing import List

from trilogy.core.enums import JoinType, PurposeLineage
from trilogy.constants import logger
from trilogy.core.processing.utility import padding, unique
from trilogy.core.processing.node_generators.common import concept_to_relevant_joins


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
    existence_parents: List[StrategyNode] = []
    if where := select.where_clause:
        targets = select.output_components + where.conditional.row_arguments
        for sub_select in where.conditional.existence_arguments:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} generating parent existence node with {[x.address for x in sub_select]}"
            )
            parent_check = source_concepts(
                mandatory_list=sub_select,
                environment=environment,
                g=g,
                depth=depth + 1,
                history=history,
            )
            if not parent_check:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent existence node for rowset node for {concept}"
                )
                return None
            existence_parents.append(parent_check)
    else:
        targets = select.output_components
    node: StrategyNode = source_concepts(
        mandatory_list=unique(targets, "address"),
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )

    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset node for {concept}"
        )
        return None
    # add our existence concepts in
    if existence_parents:
        node.parents += existence_parents
        # we don't need to join to any existence parents
        if isinstance(node, MergeNode):
            node.node_joins = []
        for parent in existence_parents:
            for x in parent.output_concepts:
                if x.address not in node.output_lcl:
                    node.existence_concepts.append(x)

    node.conditions = select.where_clause.conditional if select.where_clause else None
    enrichment = set([x.address for x in local_optional])
    rowset_relevant = [x for x in rowset.derived_concepts]
    select_hidden = set([x.address for x in select.hidden_components])
    rowset_hidden = [
        x
        for x in rowset.derived_concepts
        if isinstance(x.lineage, RowsetItem)
        and x.lineage.content.address in select_hidden
    ]
    additional_relevant = [
        x for x in select.output_components if x.address in enrichment
    ]
    # add in other other concepts

    node.add_output_concepts(rowset_relevant + additional_relevant)
    if select.where_clause:
        for item in additional_relevant:
            node.partial_concepts.append(item)

    final_hidden = rowset_hidden + [
        x
        for x in node.output_concepts
        if x.address not in [y.address for y in local_optional + [concept]]
        and x.derivation != PurposeLineage.ROWSET
    ]
    node.hide_output_concepts(final_hidden)
    # assume grain to be output of select
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
