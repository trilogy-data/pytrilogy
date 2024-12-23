from collections import defaultdict
from itertools import combinations
from typing import List

from trilogy.constants import logger
from trilogy.core.enums import BooleanOperator, JoinType, Purpose
from trilogy.core.models import (
    Concept,
    Conditional,
    Environment,
    Grain,
    MultiSelectStatement,
    WhereClause,
)
from trilogy.core.processing.node_generators.common import resolve_join_order
from trilogy.core.processing.nodes import History, MergeNode, NodeJoin
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.utility import concept_to_relevant_joins, padding

LOGGER_PREFIX = "[GEN_MULTISELECT_NODE]"


def extra_align_joins(
    base: MultiSelectStatement, parents: List[StrategyNode]
) -> List[NodeJoin]:
    node_merge_concept_map = defaultdict(list)
    output = []
    for align in base.align.items:
        jc = align.gen_concept(base)
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
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: WhereClause | None = None,
) -> MergeNode | None:
    if not isinstance(concept.lineage, MultiSelectStatement):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate multiselect node for {concept}"
        )
        return None
    lineage: MultiSelectStatement = concept.lineage

    base_parents: List[StrategyNode] = []
    partial = []
    for select in lineage.selects:
        snode: StrategyNode = source_concepts(
            mandatory_list=select.output_components,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=select.where_clause,
        )
        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Cannot generate multiselect node for {concept}"
            )
            return None
        if select.having_clause:
            if snode.conditions:
                snode.conditions = Conditional(
                    left=snode.conditions,
                    right=select.having_clause.conditional,
                    operator=BooleanOperator.AND,
                )
            else:
                snode.conditions = select.having_clause.conditional
        merge_concepts = []
        for x in [*snode.output_concepts]:
            merge = lineage.get_merge_concept(x)
            if merge:
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

    node_joins = extra_align_joins(lineage, base_parents)
    node = MergeNode(
        input_concepts=[x for y in base_parents for x in y.output_concepts],
        output_concepts=[x for y in base_parents for x in y.output_concepts],
        environment=environment,
        depth=depth,
        parents=base_parents,
        node_joins=node_joins,
        hidden_concepts=set([x for y in base_parents for x in y.hidden_concepts]),
    )

    enrichment = set([x.address for x in local_optional])

    multiselect_relevant = [
        x
        for x in lineage.derived_concepts
        if x.address == concept.address or x.address in enrichment
    ]
    additional_relevant = [x for x in node.output_concepts if x.address in enrichment]
    # add in other other concepts

    node.set_output_concepts(multiselect_relevant + additional_relevant)

    # node.add_partial_concepts(partial)
    # if select.where_clause:
    #     for item in additional_relevant:
    #         node.partial_concepts.append(item)
    node.grain = Grain.from_concepts(node.output_concepts, environment=environment)
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
