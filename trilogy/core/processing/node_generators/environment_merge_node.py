from trilogy.core.models import (
    Concept,
    Environment,
    MergeStatement,
    MergeDatasource,
    MergeUnit,
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
    parents: List[StrategyNode], merge_statement: MergeStatement
) -> List[NodeJoin]:
    output = []
    for left, right in combinations(parents, 2):
        tuples = []
        for item in merge_statement.merges:
            left_c = [x for x in item.concepts if x in left.output_concepts]
            right_c = [x for x in item.concepts if x in right.output_concepts]
            tuples.append((left_c, right_c))
        output.append(
            NodeJoin(
                left_node=left,
                right_node=right,
                concepts=[],
                join_type=JoinType.FULL,
                concept_pairs=tuples,
            )
        )
    return resolve_join_order(output)


def gen_environment_merge_node(
    all_concepts: List[Concept],
    datasource:MergeDatasource,
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> MergeNode | None:
    lineage: MergeStatement = datasource.lineage

    base_parents: List[StrategyNode] = []
    
    target_namespaces = set(x.namespace for x in lineage.concepts)

    for namespace in target_namespaces:

        sub_optional = [
            x
            for x in lineage.concepts if x.namespace == namespace
        ]

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} generating concept merge parent node with {[x.address for x in sub_optional]}"
        )
        snode: StrategyNode = source_concepts(
            mandatory_list=sub_optional,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )

        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Cannot generate merge node for merge datasource"
            )
            return None
        base_parents.append(snode)

    node_joins = merge_joins(
        base_parents,
        datasource.lineage
    )

    return  MergeNode(
        input_concepts=datasource.concepts,
        output_concepts=all_concepts,
        environment=environment,
        g=g,
        depth=depth,
        parents=base_parents,
        node_joins=node_joins,
    )