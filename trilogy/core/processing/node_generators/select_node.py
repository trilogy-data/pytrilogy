from trilogy.core.enums import PurposeLineage
from trilogy.core.models import (
    Concept,
    Environment,
    LooseConceptList,
    WhereClause,
)
from trilogy.core.processing.nodes import (
    StrategyNode,
)
from trilogy.core.exceptions import NoDatasourceException
from trilogy.constants import logger
from trilogy.core.processing.utility import padding
from trilogy.core.processing.node_generators.select_merge_node import (
    gen_select_merge_node,
)

LOGGER_PREFIX = "[GEN_SELECT_NODE]"


def gen_select_node(
    concept: Concept,
    local_optional: list[Concept],
    environment: Environment,
    g,
    depth: int,
    accept_partial: bool = False,
    fail_if_not_found: bool = True,
    conditions: WhereClause | None = None,
) -> StrategyNode | None:
    all_concepts = [concept] + local_optional
    all_lcl = LooseConceptList(concepts=all_concepts)
    materialized_lcl = LooseConceptList(
        concepts=[
            x
            for x in all_concepts
            if x.address in [z for z in environment.materialized_concepts]
            or x.derivation == PurposeLineage.CONSTANT
        ]
    )
    if materialized_lcl != all_lcl:
        missing = all_lcl.difference(materialized_lcl)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Skipping select node generation for {concept.address}"
            f" as it + optional includes non-materialized concepts (looking for all {all_lcl}, missing {missing}) "
        )
        if fail_if_not_found:
            raise NoDatasourceException(f"No datasource exists for {concept}")
        return None

    return gen_select_merge_node(
        [concept] + local_optional,
        g=g,
        environment=environment,
        depth=depth,
        accept_partial=accept_partial,
        conditions=conditions,
    )
