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
        concept,
        local_optional,
        environment,
        g,
        parents=[
            source_concepts(  # this fetches the parent + join keys
                # to then connect to the rest of the query
                arguments,
                local_optional,
                environment,
                g,
                depth=depth + 1,
            )
        ],
    )
