# directly select out a basic derivation
from typing import List

from preql.core.models import (
    Concept,
)
from preql.core.processing.nodes import StrategyNode, History, MergeNode
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from preql.constants import logger

LOGGER_PREFIX = "[GEN_BASIC_NODE]"


def gen_basic_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
):
    depth_prefix = "\t" * depth
    parent_concepts = resolve_function_parent_concepts(concept)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} has parents {[x.address for x in parent_concepts]}"
    )

    output_concepts = [concept] + local_optional
    partials = []

    attempts = [(parent_concepts, [concept])]
    if local_optional:
        attempts.append((parent_concepts + local_optional, local_optional + [concept]))

    for attempt, output in reversed(attempts):
        parent_node = source_concepts(
            mandatory_list=attempt,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not parent_node:
            continue
        parents: List[StrategyNode] = [parent_node]
        for x in output_concepts:
            sources = [p for p in parents if x in p.output_concepts]
            if not sources:
                continue
            if all(x in source.partial_concepts for source in sources):
                partials.append(x)
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept} with attempted extra {[x.address for x in attempt]}"
        )
        return MergeNode(
            input_concepts=attempt,
            output_concepts=output,
            environment=environment,
            g=g,
            parents=parents,
            depth=depth,
            partial_concepts=partials,
        )
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} No basic node could be generated for {concept}"
    )
    return None
