# directly select out a basic derivation
from typing import List

from trilogy.core.models import (
    Concept,
)
from trilogy.core.processing.nodes import StrategyNode, History
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.utility import unique
from trilogy.constants import logger

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

    local_optional_redundant = [x for x in local_optional if x in parent_concepts]
    attempts = [(parent_concepts, [concept] + local_optional_redundant)]
    from itertools import combinations

    if local_optional:
        for combo in range(1, len(local_optional) + 1):
            combos = combinations(local_optional, combo)
            for optional_set in combos:
                attempts.append(
                    (
                        unique(parent_concepts + list(optional_set), "address"),
                        list(optional_set) + [concept],
                    )
                )
    # check for the concept by itself
    for attempt, basic_output in reversed(attempts):
        partials = []
        attempt = unique(attempt, "address")
        parent_node: StrategyNode = source_concepts(
            mandatory_list=attempt,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not parent_node:
            continue
        parents: List[StrategyNode] = [parent_node]
        for x in basic_output:
            sources = [p for p in parents if x in p.output_concepts]
            if not sources:
                continue
            if all(x in source.partial_concepts for source in sources):
                partials.append(x)
        outputs = parent_node.output_concepts + [concept]
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept} with attempted extra {[x.address for x in attempt]}, output {[x.address for x in outputs]}"
        )
        # parents.resolve()

        parent_node.add_output_concept(concept)

        parent_node.remove_output_concepts(
            [
                x
                for x in parent_node.output_concepts
                if x.address not in [y.address for y in basic_output]
            ]
        )
        return parent_node
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} No basic node could be generated for {concept}"
    )
    return None
