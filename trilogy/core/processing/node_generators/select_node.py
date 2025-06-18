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


def validate_query_is_resolvable(
    missing: list[str],
    environment: BuildEnvironment,
    materialized_lcl: LooseBuildConceptList,
) -> None:
    # if a query cannot ever be resolved, exit early with an error
    for x in missing:
        if x not in environment.concepts:
            # if it's locally derived, we can assume it can be resolved
            continue
        validation_concept = environment.concepts[x]
        # if the concept we look up isn't what we searched for,
        # we're in a pseudonym anyway, don't worry about validating
        if validation_concept.address != x:
            continue
        if validation_concept.derivation == Derivation.ROOT:
            has_source = False
            for x in validation_concept.pseudonyms:
                if x in environment.alias_origin_lookup:
                    pseudonym_concept = environment.alias_origin_lookup[x]
                else:
                    pseudonym_concept = environment.concepts[x]
                # if it's not a root concept pseudonym,
                # assume we can derivve it
                if pseudonym_concept.derivation != Derivation.ROOT:
                    has_source = True
                    break
                if pseudonym_concept.address in materialized_lcl:
                    has_source = True
                    break
            if not has_source:
                raise NoDatasourceException(
                    f"No datasource exists for root concept {validation_concept}, and no resolvable pseudonyms found from {validation_concept.pseudonyms}. This query is unresolvable from your environment. Check your datasource configuration?"
                )
    return None


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
        validate_query_is_resolvable(missing, environment, materialized_lcl)
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
