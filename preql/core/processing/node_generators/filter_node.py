from typing import List


from preql.core.enums import JoinType
from preql.core.models import (
    Concept,
    Environment,
    FilterItem,
)
from preql.core.processing.nodes import (
    FilterNode,
    MergeNode,
    NodeJoin,
)
from preql.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
)


def gen_filter_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
) -> MergeNode:
    immediate_parent, parent_concepts = resolve_filter_parent_concepts(concept)

    filter_node = FilterNode(
        input_concepts=[immediate_parent] + parent_concepts,
        output_concepts=[concept, immediate_parent],
        environment=environment,
        g=g,
        parents=[
            source_concepts(
                parent_concepts,
                [],
                environment,
                g,
                depth=depth + 1,
            )
        ],
        conditions=concept.lineage.where.conditional
        if isinstance(concept.lineage, FilterItem)
        else None,
    )
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        [immediate_parent],
        local_optional,
        environment,
        g,
        depth=depth + 1,
    )
    return MergeNode(
        input_concepts=[concept, immediate_parent] + local_optional,
        output_concepts=[concept, immediate_parent] + local_optional,
        environment=environment,
        g=g,
        parents=[
            # this node fetches only what we need to filter
            filter_node,
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=filter_node,
                right_node=enrich_node,
                concepts=[immediate_parent],
                join_type=JoinType.INNER,
                filter_to_mutual=False,
            )
        ],
        # all of these concepts only count as partial
        partial_concepts=[immediate_parent] + local_optional,
    )
