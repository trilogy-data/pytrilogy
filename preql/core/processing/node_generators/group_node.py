from preql.core.models import Concept, Environment, LooseConceptList
from preql.utility import unique
from preql.core.processing.nodes import GroupNode, StrategyNode, History
from typing import List
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from preql.constants import logger
from preql.core.processing.utility import padding, create_log_lambda
from preql.core.processing.node_generators.common import (
    gen_enrichment_node,
)

LOGGER_PREFIX = "[GEN_GROUP_NODE]"


def gen_group_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
):
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts: List[Concept] = unique(
        resolve_function_parent_concepts(concept), "address"
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} parent_concepts are {[x.address for x in parent_concepts]} from group grain {concept.grain}"
    )

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    output_concepts = [concept]

    if concept.grain and len(concept.grain.components_copy) > 0:
        grain_components = (
            concept.grain.components_copy if not concept.grain.abstract else []
        )
        parent_concepts += grain_components
        output_concepts += grain_components

    if parent_concepts:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching group node parents {LooseConceptList(concepts=parent_concepts)}"
        )
        parent_concepts = unique(parent_concepts, "address")
        parent = source_concepts(
            mandatory_list=parent_concepts,
            environment=environment,
            g=g,
            depth=depth,
            history=history,
        )
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} group by node parents unresolvable"
            )
            return None
        parents: List[StrategyNode] = [parent]
    else:
        parents = []

    # the keys we group by
    # are what we can use for enrichment
    group_key_parents = concept.grain.components_copy

    group_node = GroupNode(
        output_concepts=output_concepts,
        input_concepts=parent_concepts,
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
    )

    # early exit if no optional
    if not local_optional:
        return group_node
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} group node requires enrichment")
    return gen_enrichment_node(
        group_node,
        join_keys=group_key_parents,
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
    )
