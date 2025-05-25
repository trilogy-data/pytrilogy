from typing import List

from trilogy.constants import DEFAULT_NAMESPACE, RECURSIVE_GATING_CONCEPT, logger
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildFunction,
    BuildGrain,
    BuildWhereClause,
    ComparisonOperator,
    DataType,
    Derivation,
    Purpose,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, RecursiveNode, StrategyNode
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_RECURSIVE_NODE]"

GATING_CONCEPT = BuildConcept(
    name=RECURSIVE_GATING_CONCEPT,
    namespace=DEFAULT_NAMESPACE,
    grain=BuildGrain(),
    build_is_aggregate=False,
    datatype=DataType.BOOL,
    purpose=Purpose.KEY,
    derivation=Derivation.BASIC,
)


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
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} Fetching recursive node for {concept.name} with arguments {arguments} and conditions {conditions}"
    )
    parent = source_concepts(
        mandatory_list=arguments,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        # conditions=conditions,
    )
    if not parent:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} could not find recursive node parents"
        )
        return None
    outputs = (
        [concept]
        + arguments
        + [
            GATING_CONCEPT,
        ]
    )
    base = RecursiveNode(
        input_concepts=arguments,
        output_concepts=outputs,
        environment=environment,
        parents=([parent] if (arguments or local_optional) else []),
        # preexisting_conditions=conditions
    )
    # TODO:
    # recursion will result in a union; group up to our final targets
    wrapped_base = StrategyNode(
        input_concepts=outputs,
        output_concepts=outputs,
        environment=environment,
        parents=[base],
        depth=depth,
        conditions=BuildComparison(
            left=GATING_CONCEPT, right=True, operator=ComparisonOperator.IS
        ),
    )
    return wrapped_base
