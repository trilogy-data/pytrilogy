from collections.abc import Iterable

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import NoDatasourceException
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildWhereClause,
    CanonicalBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import get_additive_rollup_concepts
from trilogy.core.processing.node_generators.select_merge_node import (
    gen_select_merge_node,
)
from trilogy.core.processing.nodes import (
    StrategyNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_SELECT_NODE]"


def validate_query_is_resolvable(
    addresses: Iterable[str],
    environment: BuildEnvironment,
) -> None:
    """A requested ROOT concept no datasource in the environment binds, under
    any spelling, is a model defect no planner can repair: no retry with other
    conditions or a wider output set will conjure a column. Say so."""
    for address in addresses:
        concept = environment.concepts.get(address)
        # Locally derived, or a pseudonym spelling: not this concept's own claim.
        if concept is None or concept.address != address:
            continue
        if concept.derivation != Derivation.ROOT:
            continue
        if concept.canonical_address in environment.materialized_canonical_concepts:
            continue
        if any(_pseudonym_is_sourced(p, environment) for p in concept.pseudonyms):
            continue
        raise NoDatasourceException(
            f"No datasource exists for root concept {concept}, and no resolvable "
            f"pseudonyms found from {concept.pseudonyms}. This query is "
            "unresolvable from your environment. Check your datasources and "
            "imports to make sure this concept is bound."
        )


def _pseudonym_is_sourced(address: str, environment: BuildEnvironment) -> bool:
    concept = environment.alias_origin_lookup.get(address) or environment.concepts.get(
        address
    )
    if concept is None:
        return False
    # A non-ROOT pseudonym is derivable; a ROOT one needs its own column.
    return (
        concept.derivation != Derivation.ROOT
        or concept.canonical_address in environment.materialized_canonical_concepts
    )


def gen_select_node(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    accept_partial: bool = False,
    fail_if_not_found: bool = True,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    build_datasources = [
        ds for ds in environment.datasources.values() if isinstance(ds, BuildDatasource)
    ]
    target_grain = BuildGrain.from_concepts(concepts)
    rollup_materialized = {
        concept.canonical_address
        for datasource in build_datasources
        for concept in get_additive_rollup_concepts(
            datasource=datasource,
            requested_concepts=concepts,
            concepts_by_address=environment.concepts,
            datasources=build_datasources,
            conditions=conditions,
            target_grain=target_grain,
        )
    }
    all_lcl = CanonicalBuildConceptList(concepts=concepts)
    # search all concepts here, including partial
    materialized_lcl = CanonicalBuildConceptList(
        concepts=[
            x
            for x in concepts
            if x.canonical_address in environment.materialized_canonical_concepts
            or x.canonical_address in rollup_materialized
            or x.derivation == Derivation.CONSTANT
        ]
    )
    if materialized_lcl != all_lcl:
        missing = all_lcl.difference(materialized_lcl)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Skipping select node generation for {concepts}"
            f" as it + optional includes non-materialized concepts (looking for all {all_lcl}, missing {missing})."
        )
        validate_query_is_resolvable(missing, environment)
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
