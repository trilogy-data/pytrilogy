from typing import List


from preql.core.models import Concept, WindowItem, Environment
from preql.utility import unique
from preql.core.processing.nodes import (
    WindowNode,
)
from preql.core.processing.nodes import MergeNode, History

from preql.constants import logger
from preql.core.processing.utility import padding, create_log_lambda
from preql.core.processing.node_generators.common import (
    gen_enrichment_node,
    concept_to_relevant_joins,
)

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
    concept: Concept,
    local_optional: list[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
) -> WindowNode | MergeNode | None:
    parent_concepts = resolve_window_parent_concepts(concept)

    parent_node = source_concepts(
        mandatory_list=parent_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
    )
    if not parent_node:
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} window node parents unresolvable")
        return None
    _window_node = WindowNode(
        input_concepts=parent_concepts,
        output_concepts=[concept] + parent_concepts,
        environment=environment,
        g=g,
        parents=[
            parent_node,
        ],
    )
    window_node = MergeNode(
        parents=[_window_node],
        environment=environment,
        g=g,
        input_concepts=_window_node.input_concepts,
        output_concepts=_window_node.output_concepts,
        grain=_window_node.grain,
        force_group=False,
    )
    if not local_optional:
        return window_node
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} group node requires enrichment")
    return gen_enrichment_node(
        window_node,
        join_keys=concept_to_relevant_joins(parent_concepts),
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
    )
