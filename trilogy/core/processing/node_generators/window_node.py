from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import BuildConcept, BuildWhereClause, BuildWindowItem
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode, WindowNode
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_WINDOW_NODE]"


WINDOW_TYPES = (BuildWindowItem,)


def resolve_window_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment
) -> tuple[BuildConcept, List[BuildConcept]]:
    if not isinstance(concept.lineage, WINDOW_TYPES):
        raise ValueError
    base = []
    if concept.lineage.over:
        base += concept.lineage.over
    if concept.lineage.order_by:
        for item in concept.lineage.order_by:
            base += item.concept_arguments
    return concept.lineage.content, unique(base, "address")


def gen_window_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    base, parent_concepts = resolve_window_parent_concepts(concept, environment)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} generating window node for {concept} with parents {parent_concepts}"
    )
    equivalent_optional = [
        x
        for x in local_optional
        if isinstance(x.lineage, WINDOW_TYPES)
        and resolve_window_parent_concepts(x, environment)[1] == parent_concepts
    ]

    non_equivalent_optional = [
        x for x in local_optional if x.address not in equivalent_optional
    ]
    targets = [base]
    if equivalent_optional:
        for x in equivalent_optional:
            assert isinstance(x.lineage, WINDOW_TYPES)
            targets.append(x.lineage.content)

    parent_node: StrategyNode = source_concepts(
        mandatory_list=parent_concepts + targets + non_equivalent_optional,
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
        input_concepts=parent_concepts + targets + non_equivalent_optional,
        output_concepts=[concept] + parent_concepts + local_optional,
        environment=environment,
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
        parents=[_window_node],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
    return window_node
