from typing import List

from trilogy.constants import logger
from trilogy.core.internal import ALL_ROWS_CONCEPT
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFunction,
    BuildGrain,
    BuildWhereClause,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    gen_enrichment_node,
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import GroupNode, History, StrategyNode
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_GROUP_NODE]"


def get_aggregate_grain(
    concept: BuildConcept, environment: BuildEnvironment
) -> BuildGrain:
    parent_concepts: List[BuildConcept] = unique(
        resolve_function_parent_concepts(concept, environment=environment), "address"
    )

    if (
        concept.grain
        and len(concept.grain.components) > 0
        and not concept.grain.abstract
    ):
        grain_components = [environment.concepts[c] for c in concept.grain.components]
        parent_concepts += grain_components
        return BuildGrain.from_concepts(parent_concepts)
    else:
        return BuildGrain.from_concepts(parent_concepts)


def gen_group_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts: List[BuildConcept] = unique(
        resolve_function_parent_concepts(concept, environment=environment), "address"
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} parent concepts for {concept} {concept.lineage} are {[x.address for x in parent_concepts]} from group grain {concept.grain}"
    )

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    output_concepts = [concept]
    grain_components = [environment.concepts[c] for c in concept.grain.components]
    if (
        concept.grain
        and len(concept.grain.components) > 0
        and not concept.grain.abstract
    ):

        parent_concepts += grain_components
        build_grain_parents = get_aggregate_grain(concept, environment)
        output_concepts += grain_components
        for possible_agg in local_optional:

            if not isinstance(
                possible_agg.lineage,
                (BuildAggregateWrapper, BuildFunction),
            ):
                continue
            if possible_agg.grain and possible_agg.grain != concept.grain:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} mismatched equivalent group by with grain {possible_agg.grain} for {concept.address}"
                )

            if possible_agg.grain and possible_agg.grain == concept.grain:
                agg_parents: List[BuildConcept] = resolve_function_parent_concepts(
                    possible_agg,
                    environment=environment,
                )
                comp_grain = get_aggregate_grain(possible_agg, environment)
                if set([x.address for x in agg_parents]).issubset(
                    set([x.address for x in parent_concepts])
                ):
                    output_concepts.append(possible_agg)
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                    )
                elif comp_grain == build_grain_parents:
                    extra = [x for x in agg_parents if x.address not in parent_concepts]
                    parent_concepts += extra
                    output_concepts.append(possible_agg)
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                    )
                else:
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} cannot include optional agg {possible_agg.address}; it has mismatched parent grain {comp_grain } vs local parent {build_grain_parents}"
                    )
    elif concept.grain.abstract:
        for possible_agg in local_optional:
            if not isinstance(
                possible_agg.lineage,
                (BuildAggregateWrapper, BuildFunction),
            ):

                continue
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} considering optional agg {possible_agg.address} for {concept.address}"
            )
            agg_parents = resolve_function_parent_concepts(
                possible_agg,
                environment=environment,
            )
            comp_grain = get_aggregate_grain(possible_agg, environment)
            if not possible_agg.grain.abstract:
                continue
            if set([x.address for x in agg_parents]).issubset(
                set([x.address for x in parent_concepts])
            ):
                output_concepts.append(possible_agg)
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                )
            elif comp_grain == get_aggregate_grain(concept, environment):
                extra = [x for x in agg_parents if x.address not in parent_concepts]
                parent_concepts += extra
                output_concepts.append(possible_agg)
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                )
            else:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} cannot include optional agg {possible_agg.address}; it has mismatched parent grain {comp_grain } vs local parent {get_aggregate_grain(concept, environment)}"
                )
    if parent_concepts:
        target_grain = BuildGrain.from_concepts(parent_concepts)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching group node parents {LooseBuildConceptList(concepts=parent_concepts)} with expected grain {target_grain}"
        )
        parent_concepts = unique(
            [x for x in parent_concepts if not x.name == ALL_ROWS_CONCEPT], "address"
        )
        parent: StrategyNode | None = source_concepts(
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

    group_node = GroupNode(
        output_concepts=output_concepts,
        input_concepts=parent_concepts,
        environment=environment,
        parents=parents,
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
        required_outputs=parent_concepts,
    )

    # early exit if no optional

    if not local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning group node"
        )
        return group_node
    missing_optional = [
        x.address for x in local_optional if x.address not in group_node.usable_outputs
    ]
    if not missing_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for group node, has all of {[x.address for x in local_optional]}"
        )
        return group_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} group node for {concept.address} requires enrichment, missing {missing_optional}"
    )
    return gen_enrichment_node(
        group_node,
        join_keys=grain_components,
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(
            LOGGER_PREFIX + f" for {concept.address}", depth, logger
        ),
        history=history,
        conditions=conditions,
    )
