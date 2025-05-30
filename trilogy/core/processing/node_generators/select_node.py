from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import NoDatasourceException
from trilogy.core.models.build import (
    BuildConcept,
    BuildWhereClause,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.select_merge_node import (
    gen_select_merge_node,
)
from trilogy.core.processing.nodes import (
    StrategyNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_SELECT_NODE]"


def gen_select_node(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    accept_partial: bool = False,
    fail_if_not_found: bool = True,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    all_lcl = LooseBuildConceptList(concepts=concepts)
    materialized_lcl = LooseBuildConceptList(
        concepts=[
            x
            for x in concepts
            if x.address in environment.materialized_concepts
            or x.derivation == Derivation.CONSTANT
        ]
    )
    if materialized_lcl != all_lcl:
        missing = all_lcl.difference(materialized_lcl)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Skipping select node generation for {concepts}"
            f" as it + optional includes non-materialized concepts (looking for all {all_lcl}, missing {missing}) "
        )
        if fail_if_not_found:
            raise NoDatasourceException(f"No datasource exists for {concepts}")
        return None

    return gen_select_merge_node(
        concepts,
        g=g,
        environment=environment,
        depth=depth,
        accept_partial=accept_partial,
        conditions=conditions,
    )
