from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildWhereClause,
    BuildWindowItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    concepts_to_grain_concepts,
    gen_enrichment_node,
)
from trilogy.core.processing.nodes import (
    History,
    StrategyNode,
    WhereSafetyNode,
    WindowNode,
)
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_WINDOW_NODE]"


WINDOW_TYPES = (BuildWindowItem,)


def resolve_window_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment, depth: int
) -> List[BuildConcept]:
    if not isinstance(concept.lineage, WINDOW_TYPES):
        raise ValueError
    base = concept.lineage.concept_arguments
    if concept.grain:
        for gitem in concept.grain.components:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} appending grain item {gitem} to base"
            )
            base.append(environment.concepts[gitem])
    if concept.keys:
        for item in concept.keys:
            logger.info(f"{padding(depth)}{LOGGER_PREFIX} appending key {item} to base")
            base.append(environment.concepts[item])
    return unique(base, "address")


def gen_window_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    parent_concepts = resolve_window_parent_concepts(concept, environment, depth)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating window node for {concept} with parents {[x.address for x in parent_concepts]} and optional {local_optional}"
    )

    additional_outputs = []
    for x in local_optional:
        if not isinstance(x.lineage, WINDOW_TYPES):
            continue
        assert isinstance(x.lineage, WINDOW_TYPES)
        parents = resolve_window_parent_concepts(x, environment, depth)

        matched = set([p.address for p in parents]) == set(
            [p.address for p in parent_concepts]
        )
        if matched:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found equivalent optional {x} with parents {parents}"
            )
            additional_outputs.append(x)

    output_targets = parent_concepts + additional_outputs + [concept]
    # finally, the ones we'll need to enrich
    non_equivalent_optional = [
        x for x in local_optional if x.address not in output_targets
    ]

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} resolving final parents {parent_concepts + output_targets}"
    )

    parent_node: StrategyNode = source_concepts(
        mandatory_list=parent_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not parent_node:
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} window node parents unresolvable")
        return None
    parent_node.resolve()
    if not all(
        [
            x.address in [y.address for y in parent_node.output_concepts]
            for x in parent_concepts
        ]
    ):
        missing = [
            x
            for x in parent_concepts
            if x.address not in [y.address for y in parent_node.output_concepts]
        ]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} window node parents unresolvable, missing {missing}"
        )
        raise SyntaxError
    _window_node = WindowNode(
        input_concepts=parent_concepts,
        output_concepts=output_targets,
        environment=environment,
        parents=[
            parent_node,
        ],
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    _window_node.rebuild_cache()
    _window_node.resolve()

    window_node = WhereSafetyNode(
        input_concepts=output_targets,
        output_concepts=output_targets,
        environment=environment,
        parents=[_window_node],
        preexisting_conditions=conditions.conditional if conditions else None,
        grain=BuildGrain.from_concepts(
            concepts=parent_concepts + output_targets,
            environment=environment,
        ),
    )
    if not non_equivalent_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning window node"
        )
        # prune outputs if we don't need join keys
        window_node.set_output_concepts(output_targets)
        return window_node

    missing_optional = [
        x.address
        for x in local_optional
        if x.address not in window_node.output_concepts
    ]

    if not missing_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for window node, has all of {[x.address for x in local_optional]}"
        )
        return window_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} window node for {concept.address} requires enrichment, missing {missing_optional}, has {[x.address for x in window_node.output_concepts]}"
    )

    return gen_enrichment_node(
        window_node,
        join_keys=concepts_to_grain_concepts(output_targets, environment),
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
        conditions=conditions,
    )
