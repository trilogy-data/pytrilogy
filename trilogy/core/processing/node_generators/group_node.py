from trilogy.core.models import (
    Concept,
    Environment,
    LooseConceptList,
    WhereClause,
    Function,
    AggregateWrapper,
    Grain,
)
from trilogy.utility import unique
from trilogy.core.processing.nodes import GroupNode, StrategyNode, History
from typing import List
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.constants import logger
from trilogy.core.processing.utility import padding, create_log_lambda
from trilogy.core.processing.node_generators.common import (
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
    conditions: WhereClause | None = None,
) -> StrategyNode | None:
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts: List[Concept] = unique(
        resolve_function_parent_concepts(concept), "address"
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} parent concepts are {[x.address for x in parent_concepts]} from group grain {concept.grain}"
    )

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    output_concepts = [concept]

    if concept.grain and len(concept.grain.components_copy) > 0:
        grain_components = (
            concept.grain.components_copy if not concept.grain.abstract else []
        )
        parent_concepts += grain_components
        output_concepts += grain_components
        for possible_agg in local_optional:
            if not isinstance(possible_agg.lineage, (AggregateWrapper, Function)):
                continue
            if possible_agg.grain and possible_agg.grain == concept.grain:
                agg_parents: List[Concept] = resolve_function_parent_concepts(
                    possible_agg
                )
                if set([x.address for x in agg_parents]).issubset(
                    set([x.address for x in parent_concepts])
                ):
                    output_concepts.append(possible_agg)
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                    )
                elif Grain(components=agg_parents) == Grain(components=parent_concepts):
                    extra = [x for x in agg_parents if x.address not in parent_concepts]
                    parent_concepts += extra
                    output_concepts.append(possible_agg)
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                    )
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
            conditions=conditions,
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
        preexisting_conditions=conditions.conditional if conditions else None,
    )

    # early exit if no optional

    if not local_optional:
        return group_node
    missing_optional = [
        x.address for x in local_optional if x.address not in group_node.output_concepts
    ]
    if not missing_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for group node, has all of {[x.address for x in local_optional]}"
        )
        return group_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} group node requires enrichment, missing {missing_optional}"
    )
    return gen_enrichment_node(
        group_node,
        join_keys=group_key_parents,
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
        conditions=conditions,
    )
