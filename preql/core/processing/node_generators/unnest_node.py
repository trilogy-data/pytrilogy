from typing import List


from preql.core.models import Concept, Function
from preql.core.processing.nodes import UnnestNode, History


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
    return UnnestNode(
        unnest_concept=concept,
        input_concepts=arguments + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=([parent] if (arguments or local_optional) else []),
    )
