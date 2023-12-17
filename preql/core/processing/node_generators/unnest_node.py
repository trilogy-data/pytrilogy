from typing import List, Tuple


from preql.core.enums import JoinType
from preql.core.models import (
    Concept,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    UnnestNode,
    MergeNode,
)




def gen_unnest_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
) -> UnnestNode:
    return UnnestNode(
        concept,
        local_optional,
        environment,
        g,
        parents= [source_concepts(  # this fetches the parent + join keys
                # to then connect to the rest of the query
                concept.lineage.concept_arguments,
                local_optional,
                environment,
                g,
                depth=depth + 1,
            )]
    )
