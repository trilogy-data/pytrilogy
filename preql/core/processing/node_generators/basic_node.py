# directly select out a basic derivation


from preql.core.models import (
    Concept,
)
from preql.core.processing.nodes import (
    SelectNode,
)
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)


def gen_basic_node(
    concept: Concept, local_optional, environment, g, depth, source_concepts
):
    parent_concepts = resolve_function_parent_concepts(concept)
    if not parent_concepts:
        raise ValueError(
            f"concept {concept} has basic lineage {concept.derivation} {type(concept.lineage)} but no parents!"
        )
    return SelectNode(
        [concept],
        local_optional,
        environment,
        g,
        parents=[
            source_concepts(
                parent_concepts,
                local_optional,
                environment,
                g,
                depth=depth + 1,
            )
        ],
        depth=depth + 1,
    )
