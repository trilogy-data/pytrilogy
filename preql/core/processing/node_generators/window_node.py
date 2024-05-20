from typing import List


from preql.core.models import (
    Concept,
    WindowItem,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    WindowNode,
)
from preql.core.processing.nodes import MergeNode

from preql.core.processing.nodes import (
    NodeJoin,
)
from preql.core.enums import JoinType
from preql.constants import logger
from preql.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_WINDOW_NODE]"


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
) -> WindowNode | MergeNode | None:
    parent_concepts = resolve_window_parent_concepts(concept)
    window_node = WindowNode(
        input_concepts=parent_concepts,
        output_concepts=[concept] + parent_concepts,
        environment=environment,
        g=g,
        parents=[
            source_concepts(
                mandatory_list=parent_concepts,
                environment=environment,
                g=g,
                depth=depth + 1,
            )
        ],
    )

    if not local_optional:
        return window_node

    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=parent_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate window enrichment node for {concept} with optional {local_optional}"
        )
        return None
    return MergeNode(
        input_concepts=[concept] + parent_concepts + local_optional,
        output_concepts=[concept] + parent_concepts + local_optional,
        environment=environment,
        g=g,
        parents=[
            # this node gets the window
            window_node,
            # this node gets enrichment
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=enrich_node,
                right_node=window_node,
                concepts=parent_concepts,
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
    )
