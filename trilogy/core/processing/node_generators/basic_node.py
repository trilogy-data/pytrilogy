from typing import List

from trilogy.constants import logger
from trilogy.core.enums import FunctionClass, SourceType
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_BASIC_NODE]"


def is_equivalent_basic_function_lineage(
    x: BuildConcept,
    y: BuildConcept,
):
    if not isinstance(x.lineage, BuildFunction) or not isinstance(
        y.lineage, BuildFunction
    ):
        return False
    if x.lineage.operator == y.lineage.operator:
        return True
    if (
        y.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        or y.lineage.operator in FunctionClass.ONE_TO_MANY.value
    ):
        return False
    return True


def gen_basic_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
):
    depth_prefix = "\t" * depth
    parent_concepts = resolve_function_parent_concepts(concept, environment=environment)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} with lineage {concept.lineage} has parents {[x for x in parent_concepts]}"
    )

    equivalent_optional = [
        x
        for x in local_optional
        if is_equivalent_basic_function_lineage(concept, x)
        and x.address != concept.address
    ]

    if equivalent_optional:
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} has equivalent optional {[x.address for x in equivalent_optional]}"
        )
    for eo in equivalent_optional:
        parent_concepts += resolve_function_parent_concepts(eo, environment=environment)
    non_equivalent_optional = [
        x
        for x in local_optional
        if x not in equivalent_optional
        and not any(x.address in y.pseudonyms for y in equivalent_optional)
    ]
    all_parents: list[BuildConcept] = unique(
        parent_concepts + non_equivalent_optional, "address"
    )
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Fetching parents {[x.address for x in all_parents]}"
    )
    parent_node: StrategyNode = source_concepts(
        mandatory_list=all_parents,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )

    if not parent_node:
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} No basic node could be generated for {concept}"
        )
        return None

    parent_node.source_type = SourceType.BASIC
    parent_node.add_output_concept(concept)
    for x in equivalent_optional:
        parent_node.add_output_concept(x)

    parent_node.remove_output_concepts(
        [
            x
            for x in parent_node.output_concepts
            if x.address not in [concept] + local_optional
        ]
    )

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept}: output {[x.address for x in parent_node.output_concepts]}"
    )
    return parent_node
