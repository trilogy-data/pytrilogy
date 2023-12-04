from typing import List, Tuple


from preql.core.enums import JoinType
from preql.core.models import (
    Concept,
    FilterItem,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    FilterNode,
    MergeNode,
)


def resolve_filter_parent_concepts(concept: Concept) -> Tuple[Concept, List[Concept]]:
    if not isinstance(concept.lineage, FilterItem):
        raise ValueError
    base = [concept.lineage.content]
    base += concept.lineage.where.concept_arguments
    return concept.lineage.content, unique(base, "address")


def gen_filter_node(
    concept: Concept,
    local_optional: List[Concept],
    environment,
    g,
    depth: int,
    source_concepts,
) -> MergeNode:
    immediate_parent, parent_concepts = resolve_filter_parent_concepts(concept)
    return MergeNode(
        [concept],
        local_optional,
        environment,
        g,
        parents=[
            # this node fetches only what we need to filter
            FilterNode(
                [concept, immediate_parent],
                [],
                environment,
                g,
                parents=[
                    source_concepts(
                        parent_concepts,
                        [],
                        environment,
                        g,
                        depth=depth + 1,
                    )
                ],
            ),
            source_concepts(  # this fetches the parent + join keys
                # to then connect to the rest of the query
                [immediate_parent],
                local_optional,
                environment,
                g,
                depth=depth + 1,
            ),
        ],
        join_concepts=[immediate_parent],
        force_join_type=JoinType.INNER,
        # all of these concepts only count as partial
        partial_concepts=[immediate_parent] + local_optional,
    )
