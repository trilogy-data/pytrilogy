from typing import List


from preql.core.models import (
    Concept,
    WindowItem,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    WindowNode,
)


def resolve_window_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, WindowItem):
        raise ValueError
    base = [concept.lineage.content]
    if concept.lineage.over:
        base += concept.lineage.over
    if concept.lineage.order_by:
        for item in concept.lineage.order_by:
            base += [item.expr.output]
    return unique(base, "address")


def gen_window_node(
    concept: Concept, local_optional, environment, g, depth, source_concepts
) -> WindowNode:
    parent_concepts = resolve_window_parent_concepts(concept)
    return WindowNode(
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
    )
