from trilogy.core.models import (
    Concept,
    Environment,
    MergeStatement,
)
from trilogy.core.processing.nodes import MergeNode, NodeJoin, History
from trilogy.core.processing.nodes.base_node import concept_list_to_grain, StrategyNode
from typing import List

from trilogy.core.enums import JoinType, PurposeLineage
from trilogy.constants import logger
from trilogy.core.processing.utility import padding
from trilogy.core.processing.node_generators.common import concept_to_relevant_joins
from itertools import combinations
from trilogy.core.processing.node_generators.common import resolve_join_order
from trilogy.utility import unique


LOGGER_PREFIX = "[GEN_CONCEPT_MERGE_NODE]"


def merge_joins(
    parents: List[StrategyNode], merge_concepts: List[Concept]
) -> List[NodeJoin]:
    output = []
    for left, right in combinations(parents, 2):
        output.append(
            NodeJoin(
                left_node=left,
                right_node=right,
                concepts=merge_concepts,
                join_type=JoinType.FULL,
                filter_to_mutual=True,
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

    # get additional concepts that should be merged across the environments
    additional_merge: List[Concept] = [*lineage.concepts]
    target_namespaces = set(x.namespace for x in [concept] + local_optional)
    for x in local_optional:
        if x.address in environment.merged_concepts:
            ms = environment.merged_concepts[x.address].lineage
            assert isinstance(ms, MergeStatement)
            additional_merge += ms.concepts

    for select in lineage.concepts:
        # if it's a merge concept, filter it out of the optional
        if select.namespace not in target_namespaces:
            continue
        sub_optional = [
            x
            for x in local_optional
            if x.address not in environment.merged_concepts
            and x.namespace == select.namespace
        ]

        sub_additional_merge = [
            x for x in additional_merge if x.namespace == select.namespace
        ]
        sub_optional += sub_additional_merge
        final: List[Concept] = unique([select] + sub_optional, "address")
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} generating concept merge parent node with {[x.address for x in final]}"
        )
        snode: StrategyNode = source_concepts(
            mandatory_list=final,
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
        for x in sub_additional_merge:
            snode.add_output_concept(environment.merged_concepts[x.address])
        base_parents.append(snode)

    node_joins = merge_joins(
        base_parents,
        unique(
            [environment.merged_concepts[x.address] for x in additional_merge],
            "address",
        ),
    )

    enrichment = set([x.address for x in local_optional])
    outputs = [
        x
        for y in base_parents
        for x in y.output_concepts
        if x.derivation != PurposeLineage.MERGE
    ]

    additional_relevant = [x for x in outputs if x.address in enrichment]
    final_outputs = outputs + additional_relevant + [concept]
    virtual_outputs = [x for x in final_outputs if x.derivation == PurposeLineage.MERGE]
    node = MergeNode(
        input_concepts=[x for y in base_parents for x in y.output_concepts],
        output_concepts=[
            x for x in final_outputs if x.derivation != PurposeLineage.MERGE
        ],
        environment=environment,
        g=g,
        depth=depth,
        parents=base_parents,
        node_joins=node_joins,
        virtual_output_concepts=virtual_outputs,
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
    missing = [
        x
        for x in local_optional
        if x.address not in [y.address for y in node.output_concepts]
    ]
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating merge concept enrichment node for missing {[x.address for x in missing]}"
    )
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=additional_relevant + missing,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate merge concept enrichment node for {concept.address} with optional {[x.address for x in local_optional]}, returning just merge concept"
        )
        return node

    # we still need the hidden concepts to be returned to the search
    # since they must be on the final node
    # to avoid further recursion
    # TODO: let the downstream search know they were found
    return MergeNode(
        input_concepts=enrich_node.output_concepts + node.output_concepts,
        # also filter out the
        output_concepts=[
            x
            for x in node.output_concepts + local_optional
            if x.derivation != PurposeLineage.MERGE
        ],
        hidden_concepts=[],
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
        virtual_output_concepts=virtual_outputs,
    )
