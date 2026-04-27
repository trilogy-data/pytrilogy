from collections.abc import Iterable, Mapping

from trilogy.core.enums import FunctionType, Granularity, Purpose
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildWhereClause,
)

ADDITIVE_ROLLUP_FUNCTIONS = {FunctionType.COUNT, FunctionType.SUM}


def _concept_lookup(
    address: str, concepts_by_address: Mapping[str, BuildConcept]
) -> BuildConcept | None:
    return concepts_by_address.get(address)


def _aggregate_inputs(concept: BuildConcept) -> list[BuildConcept]:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return []
    return list(concept.lineage.function.concept_arguments)


def _is_additive_aggregate(concept: BuildConcept) -> bool:
    return (
        isinstance(concept.lineage, BuildAggregateWrapper)
        and concept.lineage.function.operator in ADDITIVE_ROLLUP_FUNCTIONS
    )


def _aggregate_signature(
    concept: BuildConcept,
) -> tuple[FunctionType, tuple[str, ...]] | None:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return None
    return (
        concept.lineage.function.operator,
        tuple(
            sorted(
                arg.canonical_address
                for arg in concept.lineage.function.concept_arguments
            )
        ),
    )


def _datasource_has_matching_additive_aggregate(
    datasource: BuildDatasource, concept: BuildConcept
) -> bool:
    signature = _aggregate_signature(concept)
    if signature is None:
        return False
    for output in datasource.output_concepts:
        if not _is_additive_aggregate(output):
            continue
        if _aggregate_signature(output) == signature:
            return True
    return False


def _base_keys(inputs: Iterable[BuildConcept]) -> set[str]:
    keys: set[str] = set()
    for concept in inputs:
        if concept.purpose == Purpose.KEY:
            keys.add(concept.address)
        if concept.keys:
            keys.update(concept.keys)
        keys.update(concept.grain.components)
    return keys


def _datasource_proves_functional_dependency(
    dropped: BuildConcept,
    base_keys: set[str],
    datasources: Iterable[BuildDatasource],
) -> bool:
    if not base_keys:
        return False
    for datasource in datasources:
        output_addresses = {c.address for c in datasource.output_concepts}
        if dropped.address not in output_addresses:
            continue
        if not base_keys.issubset(output_addresses):
            continue
        if datasource.grain.components.issubset(base_keys):
            return True
    return False


def _safe_dropped_grain(
    dropped: BuildConcept,
    base_keys: set[str],
    datasources: Iterable[BuildDatasource],
) -> bool:
    if dropped.granularity == Granularity.SINGLE_ROW:
        return True
    if dropped.address in base_keys:
        return True
    if dropped.keys and dropped.keys.issubset(base_keys):
        return True
    return _datasource_proves_functional_dependency(dropped, base_keys, datasources)


def _conditions_supported(
    datasource: BuildDatasource, conditions: BuildWhereClause | None
) -> bool:
    if not conditions:
        return True
    condition_addresses = {
        c.canonical_address
        for c in conditions.row_arguments
        if c.granularity != Granularity.SINGLE_ROW
    }
    datasource_addresses = {c.canonical_address for c in datasource.output_concepts}
    return condition_addresses.issubset(datasource_addresses)


def get_additive_rollup_concepts(
    datasource: BuildDatasource,
    requested_concepts: list[BuildConcept],
    concepts_by_address: Mapping[str, BuildConcept],
    datasources: Iterable[BuildDatasource],
    conditions: BuildWhereClause | None = None,
) -> list[BuildConcept]:
    if not _conditions_supported(datasource, conditions):
        return []

    target_grain = BuildGrain.from_concepts(requested_concepts)
    if not target_grain.components:
        return []
    target_canonicals = [
        concepts_by_address[component].canonical_address
        for component in target_grain.components
        if component in concepts_by_address
    ]
    if len(target_canonicals) != len(set(target_canonicals)):
        return []
    datasource_grain = datasource.grain
    if datasource_grain.issubset(target_grain):
        return []

    dropped = datasource_grain - target_grain
    dropped_concepts = [
        _concept_lookup(address, concepts_by_address) for address in dropped.components
    ]
    if any(concept is None for concept in dropped_concepts):
        return []

    rollups: list[BuildConcept] = []
    datasource_list = list(datasources)
    for concept in requested_concepts:
        if not concept.is_aggregate:
            continue
        if not _is_additive_aggregate(concept):
            continue
        if not _datasource_has_matching_additive_aggregate(datasource, concept):
            continue
        base_keys = _base_keys(_aggregate_inputs(concept))
        if all(
            _safe_dropped_grain(dropped_concept, base_keys, datasource_list)
            for dropped_concept in dropped_concepts
            if dropped_concept is not None
        ):
            rollups.append(concept)
    return rollups
