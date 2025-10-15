from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import (
    BuildConcept,
    BuildFunction,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    StrategyNode,
    UnnestNode,
    WhereSafetyNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_UNNEST_NODE]"


def get_pseudonym_parents(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    source_concepts,
    environment: BuildEnvironment,
    g,
    depth,
    history,
    conditions,
) -> List[StrategyNode]:
    for x in concept.pseudonyms:
        attempt = source_concepts(
            mandatory_list=[environment.alias_origin_lookup[x]] + local_optional,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
            accept_partial=True,
        )
        if not attempt:
            continue
        return [attempt]
    return []


def gen_unnest_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    arguments = []
    join_nodes: list[StrategyNode] = []
    depth_prefix = "\t" * depth
    if isinstance(concept.lineage, BuildFunction):
        arguments = concept.lineage.concept_arguments
    search_optional = local_optional
    if (not arguments) and (local_optional and concept.pseudonyms):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} unnest node for {concept} has no parents; creating solo unnest node"
        )
        join_nodes += get_pseudonym_parents(
            concept,
            local_optional,
            source_concepts,
            environment,
            g,
            depth,
            history,
            conditions,
        )
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} unnest node for {concept} got join nodes {join_nodes}"
        )
        search_optional = []

    equivalent_optional = [x for x in search_optional if x.lineage == concept.lineage]

    non_equivalent_optional = [
        x for x in search_optional if x not in equivalent_optional
    ]
    all_parents = arguments + non_equivalent_optional
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} unnest node for {concept} with lineage {concept.lineage} has parents + optional {all_parents} and equivalent optional {equivalent_optional}"
    )
    local_conditions = False
    expected_outputs = [concept] + local_optional
    parent: StrategyNode | None = None
    if arguments or search_optional:
        parent = source_concepts(
            mandatory_list=all_parents,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} could not find unnest node parents"
            )
            return None
    elif conditions:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} unnest node has no parents but conditions inputs {conditions.row_arguments} vs expected output {expected_outputs}"
        )
        if all([x.address in expected_outputs for x in conditions.row_arguments]):
            local_conditions = True
        else:
            parent = source_concepts(
                mandatory_list=conditions.conditional.row_arguments,
                environment=environment,
                g=g,
                depth=depth + 1,
                history=history,
                conditions=conditions,
            )
            if not parent:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} could not find unnest node condition inputs with no parents"
                )
                return None
    else:
        parent = None
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} unnest node for {concept} got parent {parent}"
    )
    base = UnnestNode(
        unnest_concepts=[concept] + equivalent_optional,
        input_concepts=arguments + non_equivalent_optional,
        output_concepts=[concept] + search_optional,
        environment=environment,
        parents=([parent] if parent else []),
    )

    conditional = conditions.conditional if conditions else None
    if join_nodes:
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} unnest node for {concept} needs to merge with join nodes {join_nodes}"
        )
        for x in join_nodes:
            logger.info(
                f"{depth_prefix}{LOGGER_PREFIX} join node {x} with partial {x.partial_concepts}"
            )
            pseudonyms = [
                environment.alias_origin_lookup[p] for p in concept.pseudonyms
            ]
            x.add_partial_concepts(pseudonyms)
        return MergeNode(
            input_concepts=base.output_concepts
            + [j for n in join_nodes for j in n.output_concepts],
            output_concepts=[concept] + local_optional,
            environment=environment,
            parents=[base] + join_nodes,
            conditions=conditional if local_conditions is True else None,
            preexisting_conditions=(
                conditional if conditional and local_conditions is False else None
            ),
        )
    # we need to sometimes nest an unnest node,
    # as unnest operations are not valid in all situations
    new = WhereSafetyNode(
        input_concepts=base.output_concepts,
        output_concepts=base.output_concepts,
        environment=environment,
        parents=[base],
        conditions=conditional if local_conditions is True else None,
        preexisting_conditions=(
            conditional if conditional and local_conditions is False else None
        ),
    )
    # qds = new.resolve()
    # assert qds.source_map[concept.address] == {base.resolve()}
    # for x in equivalent_optional:
    #     assert qds.source_map[x.address] == {base.resolve()}
    return new
