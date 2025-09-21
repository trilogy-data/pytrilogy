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
    StrategyNode,
    UnnestNode,
    WhereSafetyNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_UNNEST_NODE]"


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
    depth_prefix = "\t" * depth
    if isinstance(concept.lineage, BuildFunction):
        arguments = concept.lineage.concept_arguments

    equivalent_optional = [x for x in local_optional if x.lineage == concept.lineage]

    non_equivalent_optional = [
        x for x in local_optional if x not in equivalent_optional
    ]
    all_parents = arguments + non_equivalent_optional
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} unnest node for {concept} with lineage {concept.lineage} has parents + optional {all_parents} and equivalent optional {equivalent_optional}"
    )
    local_conditions = False
    expected_outputs = [concept] + local_optional
    if arguments or local_optional:
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
        output_concepts=[concept] + local_optional,
        environment=environment,
        parents=([parent] if parent else []),
    )
    # we need to sometimes nest an unnest node,
    # as unnest operations are not valid in all situations
    # TODO: inline this node when we can detect it's safe
    conditional = conditions.conditional if conditions else None
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
