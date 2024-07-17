from typing import List


from trilogy.core.models import Concept, Function
from trilogy.core.processing.nodes import SelectNode, UnnestNode, History


def gen_unnest_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> UnnestNode | None:
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
            return None
    # we need to always nest an unnest node,
    # as unnest operations are not valid in all situations

    return SelectNode(
        input_concepts=[concept] + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=[
            UnnestNode(
                unnest_concept=concept,
                input_concepts=arguments + local_optional,
                output_concepts=[concept] + local_optional,
                environment=environment,
                g=g,
                parents=([parent] if (arguments or local_optional) else []),
            )
        ],
    )
