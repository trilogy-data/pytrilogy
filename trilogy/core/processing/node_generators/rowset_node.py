from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildWhereClause,
    Factory,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, MergeNode, StrategyNode
from trilogy.core.processing.utility import concept_to_relevant_joins, padding

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def gen_rowset_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, BuildRowsetItem):
        raise SyntaxError(
            f"Invalid lineage passed into rowset fetch, got {type(concept.lineage)}, expected {BuildRowsetItem}"
        )
    lineage: BuildRowsetItem = concept.lineage
    rowset: BuildRowsetLineage = lineage.rowset
    select: SelectLineage | MultiSelectLineage = lineage.rowset.select

    node = get_query_node(history.base_environment, select)

    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent rowset node for {concept}"
        )
        raise UnresolvableQueryException(
            f"Cannot generate parent select for concept {concept} in rowset {rowset.name}; ensure the rowset is a valid statement."
        )
    enrichment = set([x.address for x in local_optional])

    factory = Factory(environment=history.base_environment, grain=select.grain)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} rowset derived concepts are {lineage.rowset.derived_concepts}"
    )
    concept_pool = list(environment.concepts.values()) + list(
        environment.alias_origin_lookup.values()
    )
    rowset_outputs = [
        x.address for x in concept_pool if x.address in lineage.rowset.derived_concepts
    ]
    rowset_relevant: list[BuildConcept] = [
        v for v in concept_pool if v.address in rowset_outputs
    ]

    select_hidden = node.hidden_concepts
    rowset_hidden = [
        x
        for x in rowset_relevant
        if x.address in lineage.rowset.derived_concepts
        and isinstance(x.lineage, BuildRowsetItem)
        and x.lineage.content.address in select_hidden
    ]
    additional_relevant = [
        factory.build(x) for x in select.output_components if x.address in enrichment
    ]
    # add in other other concepts
    node.set_output_concepts(rowset_relevant + additional_relevant)
    if select.where_clause:
        for item in additional_relevant:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} adding {item} to partial concepts"
            )
            node.partial_concepts.append(item)

    final_hidden = rowset_hidden + [
        x
        for x in node.output_concepts
        if x.address not in local_optional + [concept]
        and x.derivation != Derivation.ROWSET
        and not any(z in lineage.rowset.derived_concepts for z in x.pseudonyms)
    ]
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} hiding {final_hidden}")
    node.hide_output_concepts(final_hidden)
    assert node.resolution_cache
    # assume grain to be output of select
    # but don't include anything hidden(the non-rowset concepts)
    node.grain = BuildGrain.from_concepts(
        [
            x
            for x in node.output_concepts
            if x.address
            not in [
                y
                for y in node.hidden_concepts
                if y in environment.concepts
                and environment.concepts[y].derivation != Derivation.ROWSET
            ]
        ],
    )

    node.rebuild_cache()
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} final output is {[x.address for x in node.output_concepts]}"
    )
    if not local_optional or all(
        x.address in node.output_concepts and x.address not in node.partial_concepts
        for x in local_optional
    ):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enrichment required for rowset node as all optional {[x.address for x in local_optional]} found or no optional; exiting early."
        )
        return node
    remaining = [
        x
        for x in local_optional
        if x not in node.output_concepts or x in node.partial_concepts
    ]
    possible_joins = concept_to_relevant_joins(
        [x for x in node.output_concepts if x.derivation != Derivation.ROWSET]
    )
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for rowset node to get {[x.address for x in local_optional]}; have {[x.address for x in node.output_concepts]}"
        )
        return node
    if any(x.derivation == Derivation.ROWSET for x in possible_joins):

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot enrich rowset node with rowset concepts; exiting early"
        )
        return node
    logger.info([x.address for x in possible_joins + local_optional])
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=possible_joins + remaining,
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=conditions,
        history=history,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset enrichment node for {concept} with optional {local_optional}, returning just rowset node"
        )
        return node

    non_hidden = [
        x for x in node.output_concepts if x.address not in node.hidden_concepts
    ]
    for x in possible_joins:
        if x.address in node.hidden_concepts:
            node.unhide_output_concepts([x])
    non_hidden_enrich = [
        x
        for x in enrich_node.output_concepts
        if x.address not in enrich_node.hidden_concepts
    ]
    return MergeNode(
        input_concepts=non_hidden + non_hidden_enrich,
        output_concepts=non_hidden + local_optional,
        environment=environment,
        depth=depth,
        parents=[
            node,
            enrich_node,
        ],
        partial_concepts=node.partial_concepts + enrich_node.partial_concepts,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
