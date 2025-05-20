from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode, RecursiveNode
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_RECURSIVE_NODE]"


def gen_recursive_node(
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
    if isinstance(concept.lineage, BuildFunction):
        arguments = concept.lineage.concept_arguments
    # source parents first
    parent = source_concepts(
        mandatory_list=arguments,
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

    base = RecursiveNode(
        input_concepts=arguments,
        output_concepts=[concept]+arguments,
        environment=environment,
        parents=([parent] if (arguments or local_optional) else []),
    )
    # TODO:
    # get local optional
    return base
