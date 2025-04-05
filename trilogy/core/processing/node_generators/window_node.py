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
    gen_enrichment_node,
)
from trilogy.core.processing.nodes import History, StrategyNode, WindowNode
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_WINDOW_NODE]"


WINDOW_TYPES = (BuildWindowItem,)


def resolve_window_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment
) -> tuple[BuildConcept, List[BuildConcept]]:
    if not isinstance(concept.lineage, WINDOW_TYPES):
        raise ValueError
    base = []
    if concept.lineage.over:
        base += concept.lineage.over
    if concept.lineage.order_by:
        for item in concept.lineage.order_by:
            base += item.concept_arguments

    return concept.lineage.content, unique(base, "address")


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
    base, parent_concepts = resolve_window_parent_concepts(concept, environment)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating window node for {concept} with parents {parent_concepts} and optional {local_optional}"
    )
    equivalent_optional = [
        x
        for x in local_optional
        if isinstance(x.lineage, WINDOW_TYPES)
        and resolve_window_parent_concepts(x, environment)[1] == parent_concepts
    ]

    targets = [base]
    # append in keys to get the right grain
    if concept.keys:
        for item in concept.keys:
            targets.append(environment.concepts[item])
    additional_outputs = []
    if equivalent_optional:
        for x in equivalent_optional:
            assert isinstance(x.lineage, WINDOW_TYPES)
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found equivalent optional {x} with parents {resolve_window_parent_concepts(x, environment)[1]}"
            )
            additional_outputs.append(x)

    grain_equivalents = [
        x for x in local_optional if x.keys and all([key in targets for key in x.keys])
    ]

    for x in grain_equivalents:
        targets.append(x)

    # finally, the ones we'll need to enrich
    non_equivalent_optional = [x for x in local_optional if x.address not in targets]

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} resolving final parents {parent_concepts + targets}"
    )
    parent_node: StrategyNode = source_concepts(
        mandatory_list=parent_concepts + targets,
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
        input_concepts=parent_concepts + targets,
        output_concepts=[concept] + additional_outputs + parent_concepts + targets,
        environment=environment,
        parents=[
            parent_node,
        ],
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    _window_node.rebuild_cache()
    _window_node.resolve()
    window_node = StrategyNode(
        input_concepts=[concept] + additional_outputs + parent_concepts + targets,
        output_concepts=[concept] + additional_outputs + parent_concepts + targets,
        environment=environment,
        parents=[_window_node],
        preexisting_conditions=conditions.conditional if conditions else None,
        # hidden_concepts=[
        #     x.address for x in parent_concepts if x.address not in local_optional
        # ],
    )
    if not non_equivalent_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning window node"
        )
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
        join_keys=[
            environment.concepts[c]
            for c in BuildGrain.from_concepts(
                concepts=targets, environment=environment
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
