from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode, UnnestNode
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
    if isinstance(concept.lineage, BuildFunction):
        arguments = concept.lineage.concept_arguments

    equivalent_optional = [x for x in local_optional if x.lineage == concept.lineage]
    non_equivalent_optional = [
        x for x in local_optional if x not in equivalent_optional
    ]
    if arguments or local_optional:
        parent = source_concepts(
            mandatory_list=arguments + non_equivalent_optional,
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

    base = UnnestNode(
        unnest_concepts=[concept] + equivalent_optional,
        input_concepts=arguments + non_equivalent_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        parents=([parent] if (arguments or local_optional) else []),
    )
    # we need to sometimes nest an unnest node,
    # as unnest operations are not valid in all situations
    # TODO: inline this node when we can detect it's safe
    new = StrategyNode(
        input_concepts=base.output_concepts,
        output_concepts=base.output_concepts,
        environment=environment,
        parents=[base],
        # conditions=conditions.conditional if conditions else None,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    qds = new.resolve()
    assert qds.source_map[concept.address] == {base.resolve()}
    for x in equivalent_optional:
        assert qds.source_map[x.address] == {base.resolve()}
    return new
