from typing import List


from preql.core.models import Concept, Function
from preql.core.processing.nodes import (
    UnnestNode,
)


def gen_unnest_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
) -> UnnestNode:
    arguments = []
    if isinstance(concept.lineage, Function):
        arguments = concept.lineage.concept_arguments
    return UnnestNode(
        unnest_concept=concept,
        input_concepts=arguments + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=[
            source_concepts(  # this fetches the parent + join keys
                # to then connect to the rest of the query
                arguments,
                local_optional,
                environment,
                g,
                depth=depth + 1,
            )
        ]
        if (arguments or local_optional)
        else [],
    )
