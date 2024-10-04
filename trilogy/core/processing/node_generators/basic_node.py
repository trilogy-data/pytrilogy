# directly select out a basic derivation
from typing import List

from trilogy.core.models import Concept, WhereClause
from trilogy.core.processing.nodes import StrategyNode, History
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.constants import logger
from trilogy.core.enums import SourceType

LOGGER_PREFIX = "[GEN_BASIC_NODE]"


def gen_basic_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: WhereClause | None = None,
):
    depth_prefix = "\t" * depth
    parent_concepts = resolve_function_parent_concepts(concept)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} has parents {[x.address for x in parent_concepts]}"
    )

    equivalent_optional = [
        x
        for x in local_optional
        if x.lineage == concept.lineage and x.address != concept.address
    ]
    non_equivalent_optional = [
        x for x in local_optional if x not in equivalent_optional
    ]
    parent_node: StrategyNode = source_concepts(
        mandatory_list=parent_concepts + non_equivalent_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )

    if not parent_node:
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} No basic node could be generated for {concept}"
        )
        return None

    parent_node.source_type = SourceType.BASIC
    parent_node.add_output_concept(concept)
    for x in equivalent_optional:
        parent_node.add_output_concept(x)

    parent_node.remove_output_concepts(
        [
            x
            for x in parent_node.output_concepts
            if x.address not in [concept] + local_optional
        ]
    )

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept}: output {[x.address for x in parent_node.output_concepts]}"
    )
    return parent_node
