from typing import List


from trilogy.core.models import Concept, WindowItem, Environment, WhereClause
from trilogy.utility import unique
from trilogy.core.processing.nodes import WindowNode, StrategyNode, History
from trilogy.constants import logger
from trilogy.core.processing.utility import padding

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
    conditions: WhereClause | None = None,
) -> StrategyNode | None:
    parent_concepts = resolve_window_parent_concepts(concept)
    parent_node = source_concepts(
        mandatory_list=parent_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not parent_node:
        logger.info(f"{padding(depth)}{LOGGER_PREFIX} window node parents unresolvable")
        return None
    parent_node.resolve()
    if not all(
        [
            x.address in [y.address for y in parent_node.output_concepts]
            for x in parent_concepts
        ]
    ):
        missing = [
            x
            for x in parent_concepts
            if x.address not in [y.address for y in parent_node.output_concepts]
        ]
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} window node parents unresolvable, missing {missing}"
        )
        raise SyntaxError
    _window_node = WindowNode(
        input_concepts=parent_concepts + local_optional,
        output_concepts=[concept] + parent_concepts + local_optional,
        environment=environment,
        g=g,
        parents=[
            parent_node,
        ],
        depth=depth,
    )
    _window_node.rebuild_cache()
    _window_node.resolve()
    window_node = StrategyNode(
        input_concepts=[concept] + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        g=g,
        parents=[_window_node],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    return window_node
