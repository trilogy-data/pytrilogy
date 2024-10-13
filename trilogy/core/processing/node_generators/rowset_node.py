from trilogy.core.models import (
    Concept,
    Environment,
    SelectStatement,
    RowsetDerivationStatement,
    RowsetItem,
    MultiSelectStatement,
    WhereClause,
)
from trilogy.core.processing.nodes import MergeNode, History, StrategyNode
from typing import List

from trilogy.core.enums import PurposeLineage
from trilogy.constants import logger
from trilogy.core.processing.utility import padding, concept_to_relevant_joins
from trilogy.core.processing.nodes.base_node import concept_list_to_grain

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def gen_rowset_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: WhereClause | None = None,
) -> StrategyNode | None:
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, RowsetItem):
        raise SyntaxError(
            f"Invalid lineage passed into rowset fetch, got {type(concept.lineage)}, expected {RowsetItem}"
        )
    lineage: RowsetItem = concept.lineage
    rowset: RowsetDerivationStatement = lineage.rowset
    select: SelectStatement | MultiSelectStatement = lineage.rowset.select
    node = get_query_node(environment, select, graph=g, history=history)

    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent rowset node for {concept}"
        )
        return None
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
        if x.address not in local_optional + [concept]
        and x.derivation != PurposeLineage.ROWSET
    ]
    node.hide_output_concepts(final_hidden)
    assert node.resolution_cache
    # assume grain to be output of select
    # but don't include anything hidden(the non-rowset concepts)
    node.grain = concept_list_to_grain(
        [
            x
            for x in node.output_concepts
            if x.address
            not in [
                y for y in node.hidden_concepts if y.derivation != PurposeLineage.ROWSET
            ]
        ],
        parent_sources=node.resolution_cache.datasources,
    )

    node.rebuild_cache()

    if not local_optional or all(
        x.address in node.output_concepts for x in local_optional
    ):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enrichment required for rowset node as all optional found or no optional; exiting early."
        )
        # node.set_preexisting_conditions(conditions.conditional if conditions else None)
        return node

    possible_joins = concept_to_relevant_joins(node.output_concepts)
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for rowset node to get {[x.address for x in local_optional]}; have {[x.address for x in node.output_concepts]}"
        )
        return node
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=possible_joins + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=conditions,
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
            node,
            enrich_node,
        ],
        partial_concepts=node.partial_concepts,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
