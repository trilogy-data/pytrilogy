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
    parent_concepts = resolve_function_parent_concepts(concept)
    depth_prefix = "\t" * depth
    if not parent_concepts:
        raise ValueError(
            f"concept {concept} has basic lineage {concept.derivation} {type(concept.lineage)} {str(concept.lineage)}  but no parents!"
        )
    output_concepts = [concept] + local_optional
    partials = []
    enriched = parent_concepts + local_optional
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Generating basic enrichment node with {[x.address for x in enriched]}"
    )
    enriched_node = source_concepts(
        mandatory_list=parent_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    if not enriched_node:
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} Could not find enrichment node for {concept} with local optional {[c.address for c in local_optional]}"
        )
        base: StrategyNode = source_concepts(
            mandatory_list=parent_concepts,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        return MergeNode(
            input_concepts=parent_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            parents=[base],
            depth=depth,
            partial_concepts=base.partial_concepts,
        )
    parents: List[StrategyNode] = [enriched_node]
    for x in output_concepts:
        sources = [p for p in parents if x in p.output_concepts]
        if not sources:
            continue
        if all(x in source.partial_concepts for source in sources):
            partials.append(x)
    logger.info(f"{depth_prefix}{LOGGER_PREFIX} Returning basic select node")
    return MergeNode(
        input_concepts=parent_concepts + local_optional,
        output_concepts=output_concepts,
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
        partial_concepts=partials,
    )
