from typing import List


from trilogy.core.models import Concept, Function
from trilogy.core.processing.nodes import SelectNode, UnnestNode, History, StrategyNode
from trilogy.core.processing.utility import padding
from trilogy.constants import logger

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def gen_unnest_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> StrategyNode | None:
    arguments = []
    if isinstance(concept.lineage, Function):
        arguments = concept.lineage.concept_arguments
    if arguments or local_optional:
        parent = source_concepts(
            mandatory_list=arguments + local_optional,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} could not find unnest node parents"
            )
            return None

    base = UnnestNode(
        unnest_concept=concept,
        input_concepts=arguments + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=([parent] if (arguments or local_optional) else []),
    )
    # we need to sometimes nest an unnest node,
    # as unnest operations are not valid in all situations
    # TODO: inline this node when we can detect it's safe
    new = SelectNode(
        input_concepts=[concept] + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=[base],
    )
    qds = new.resolve()
    assert qds.source_map[concept.address] == {base.resolve()}
    return new
