from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildSubselectItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import gen_enrichment_node
from trilogy.core.processing.nodes import (
    History,
    StrategyNode,
    SubselectNode,
)
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_SUBSELECT_NODE]"

SUBSELECT_TYPES = (BuildSubselectItem,)


def resolve_subselect_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment, depth: int
) -> List[BuildConcept]:
    if not isinstance(concept.lineage, SUBSELECT_TYPES):
        raise ValueError(
            f"Expected subselect lineage for {concept.address}, got {type(concept.lineage)}"
        )
    lineage: BuildSubselectItem = concept.lineage
    base: list[BuildConcept] = list(lineage.concept_arguments)
    if concept.grain:
        base_addrs = {x.address for x in base}
        for gitem in concept.grain.components:
            if gitem not in base_addrs:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} appending grain item {gitem}"
                )
                base.append(environment.concepts[gitem])
    return unique(base, "address")


def gen_subselect_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    parent_concepts = resolve_subselect_parent_concepts(concept, environment, depth)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating subselect node for {concept} with parents {[x.address for x in parent_concepts]}"
    )

    targets: list[BuildConcept] = []
    if concept.keys:
        for item in concept.keys:
            if item in [x.address for x in parent_concepts]:
                continue
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} appending search for key {item}"
            )
            targets.append(environment.concepts[item])

    lineage: BuildSubselectItem = concept.lineage  # type: ignore
    # Cross-datasource: inner concepts resolved separately from outer
    if lineage.outer_arguments:
        inner_concepts = unique(list(lineage.inner_concept_arguments), "address")
        outer_concepts = parent_concepts
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cross-datasource: inner={[x.address for x in inner_concepts]}, outer={[x.address for x in outer_concepts]}"
        )
        inner_node: StrategyNode = source_concepts(
            mandatory_list=inner_concepts,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        if not inner_node:
            logger.info(f"{padding(depth)}{LOGGER_PREFIX} inner concepts unresolvable")
            return None
        inner_node.resolve()

        outer_node: StrategyNode = source_concepts(
            mandatory_list=outer_concepts + targets,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        if not outer_node:
            logger.info(f"{padding(depth)}{LOGGER_PREFIX} outer concepts unresolvable")
            return None
        outer_node.resolve()
        all_parent_concepts = unique(outer_concepts + inner_concepts, "address")
        parents = [outer_node, inner_node]
    else:
        parent_node: StrategyNode = source_concepts(
            mandatory_list=parent_concepts + targets,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        if not parent_node:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} subselect node parents unresolvable"
            )
            return None
        parent_node.resolve()
        all_parent_concepts = parent_concepts
        parents = [parent_node]

    _subselect_node = SubselectNode(
        input_concepts=all_parent_concepts + targets,
        output_concepts=[concept] + all_parent_concepts + targets,
        environment=environment,
        parents=parents,
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    _subselect_node.rebuild_cache()
    _subselect_node.resolve()

    non_equivalent_optional = [
        x
        for x in local_optional
        if x.address not in [y.address for y in _subselect_node.output_concepts]
    ]

    if not non_equivalent_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning subselect node"
        )
        _subselect_node.set_output_concepts([concept] + all_parent_concepts + targets)
        return _subselect_node

    missing_optional = [
        x.address
        for x in local_optional
        if x.address not in _subselect_node.output_concepts
    ]

    if not missing_optional:
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed")
        return _subselect_node

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} subselect node for {concept.address} requires enrichment, missing {missing_optional}"
    )

    return gen_enrichment_node(
        _subselect_node,
        join_keys=[
            environment.concepts[c]
            for c in BuildGrain.from_concepts(
                concepts=all_parent_concepts + targets, environment=environment
            ).components
        ],
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
        conditions=conditions,
    )
