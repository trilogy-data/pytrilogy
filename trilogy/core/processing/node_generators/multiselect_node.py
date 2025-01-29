from collections import defaultdict
from itertools import combinations
from typing import List

from trilogy.constants import logger
from trilogy.core.enums import JoinType, Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import resolve_join_order
from trilogy.core.processing.nodes import History, MergeNode, NodeJoin
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.utility import concept_to_relevant_joins, padding

LOGGER_PREFIX = "[GEN_MULTISELECT_NODE]"


def extra_align_joins(
    base: BuildMultiSelectLineage,
    environment: BuildEnvironment,
    parents: List[StrategyNode],
) -> List[NodeJoin]:
    node_merge_concept_map = defaultdict(list)
    output = []
    for align in base.align.items:
        jc = environment.concepts[align.aligned_concept]
        if jc.purpose == Purpose.CONSTANT:
            continue
        for node in parents:
            for item in align.concepts:
                if item in node.output_lcl:
                    node_merge_concept_map[node].append(jc)

    for left, right in combinations(node_merge_concept_map.keys(), 2):
        matched_concepts = [
            x
            for x in node_merge_concept_map[left]
            if x in node_merge_concept_map[right]
        ]
        output.append(
            NodeJoin(
                left_node=left,
                right_node=right,
                concepts=matched_concepts,
                join_type=JoinType.FULL,
            )
        )
    return resolve_join_order(output)


def gen_multiselect_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> MergeNode | None:
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, BuildMultiSelectLineage):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate multiselect node for {concept}"
        )
        return None
    lineage: BuildMultiSelectLineage = concept.lineage

    base_parents: List[StrategyNode] = []
    partial = []
    for select in lineage.selects:

        snode: StrategyNode = get_query_node(history.base_environment, select)

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Fetched parent node with outputs {select.output_components}"
        )
        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Cannot generate multiselect node for {concept}"
            )
            return None
        merge_concepts = []
        for x in [*snode.output_concepts]:
            merge_name = lineage.get_merge_concept(x)
            if merge_name:
                merge = environment.concepts[merge_name]
                snode.output_concepts.append(merge)
                merge_concepts.append(merge)
        # clear cache so QPS
        snode.rebuild_cache()
        for mc in merge_concepts:
            assert mc in snode.resolve().output_concepts
        base_parents.append(snode)
        if select.where_clause:
            for item in select.output_components:
                partial.append(item)
        logger.info(snode.hidden_concepts)

    node_joins = extra_align_joins(lineage, environment, base_parents)
    logger.info(
        f"Non-hidden {[x for y in base_parents for x in y.output_concepts if x.address not in y.hidden_concepts]}"
    )
    node = MergeNode(
        input_concepts=[
            x
            for y in base_parents
            for x in y.output_concepts
            if x.address not in y.hidden_concepts
        ],
        output_concepts=[
            x
            for y in base_parents
            for x in y.output_concepts
            if x.address not in y.hidden_concepts
        ],
        environment=environment,
        depth=depth,
        parents=base_parents,
        node_joins=node_joins,
        grain=BuildGrain.from_concepts(
            [
                x
                for y in base_parents
                for x in y.output_concepts
                if x.address not in y.hidden_concepts
            ],
            environment=environment,
        ),
    )

    enrichment = set([x.address for x in local_optional])

    multiselect_relevant = [
        environment.concepts[x]
        for x in lineage.derived_concepts
        if x == concept.address or x in enrichment
    ]
    additional_relevant = [x for x in node.output_concepts if x.address in enrichment]
    # add in other other concepts

    node.set_output_concepts(multiselect_relevant + additional_relevant)

    # node.add_partial_concepts(partial)
    # if select.where_clause:
    #     for item in additional_relevant:
    #         node.partial_concepts.append(item)
    node.grain = BuildGrain.from_concepts(node.output_concepts, environment=environment)
    node.rebuild_cache()
    # we need a better API for refreshing a nodes QDS
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
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} Missing required concepts {[x for x in local_optional if x.address not in [y.address for y in node.output_concepts]]}"
    )
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=additional_relevant + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
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
        depth=depth,
        parents=[
            # this node gets the multiselect
            node,
            # this node gets enrichment
            enrich_node,
        ],
        partial_concepts=node.partial_concepts,
    )
