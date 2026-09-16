from collections.abc import Iterable, Mapping

from trilogy.core.enums import Granularity, Purpose
from trilogy.core.models.build import (
    ADDITIVE_ROLLUP_FUNCTIONS,
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildWhereClause,
)


def _aggregate_inputs(concept: BuildConcept) -> list[BuildConcept]:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return []
    return list(concept.lineage.function.concept_arguments)


def _is_additive_aggregate(concept: BuildConcept) -> bool:
    return (
        isinstance(concept.lineage, BuildAggregateWrapper)
        and concept.lineage.function.operator in ADDITIVE_ROLLUP_FUNCTIONS
    )


def _datasource_has_matching_additive_aggregate(
    datasource: BuildDatasource, concept: BuildConcept
) -> bool:
    signature = concept.additive_aggregate_signature
    if signature is None:
        return False
    return any(
        output.additive_aggregate_signature == signature
        for output in datasource.output_concepts
    )


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


def _addresses_reachable(
    datasource: BuildDatasource,
    addresses: set[str],
    concepts_by_address: Mapping[str, BuildConcept] | None,
) -> bool:
    """Every canonical address is bound by the datasource, or is a property whose
    keys sit inside the datasource's grain (the planner joins the owning dim)."""
    datasource_addresses = {c.canonical_address for c in datasource.output_concepts}
    missing = addresses - datasource_addresses
    if not missing:
        return True
    if concepts_by_address is None:
        return False
    ds_grain_components = set(datasource.grain.components)
    for address in missing:
        concept = concepts_by_address.get(address)
        if concept is None or concept.purpose != Purpose.PROPERTY:
            return False
        if not concept.keys or not set(concept.keys).issubset(ds_grain_components):
            return False
    return True


def _conditions_supported(
    datasource: BuildDatasource,
    conditions: BuildWhereClause | None,
    concepts_by_address: Mapping[str, BuildConcept] | None = None,
) -> bool:
    if not conditions:
        return True
    return _addresses_reachable(
        datasource,
        {
            c.canonical_address
            for c in conditions.row_arguments
            if c.granularity != Granularity.SINGLE_ROW
        },
        concepts_by_address,
    )


def filter_finer_row_args(
    conditions: BuildWhereClause | None,
    target_grain: BuildGrain,
    concepts_by_address: Mapping[str, BuildConcept],
) -> list[BuildConcept]:
    """Row-arg filter concepts finer than the target grain: not single-row, not a
    grain component, and not a property whose keys sit inside the grain. Such a
    filter splits groups, so it must be applied before aggregation on a finer
    summary rather than after rolling up a coarser one."""
    if conditions is None:
        return []
    target_components = set(target_grain.components)
    target_canonicals = {
        concepts_by_address[c].canonical_address
        for c in target_components
        if c in concepts_by_address
    }
    finer: list[BuildConcept] = []
    for concept in conditions.row_arguments:
        if concept.granularity == Granularity.SINGLE_ROW:
            continue
        if (
            concept.address in target_components
            or concept.canonical_address in target_canonicals
        ):
            continue
        if (
            concept.purpose == Purpose.PROPERTY
            and concept.keys
            and set(concept.keys).issubset(target_components)
        ):
            continue
        finer.append(concept)
    return finer


def get_additive_rollup_concepts(
    datasource: BuildDatasource,
    requested_concepts: list[BuildConcept],
    concepts_by_address: Mapping[str, BuildConcept],
    datasources: Iterable[BuildDatasource],
    target_grain: BuildGrain,
    conditions: BuildWhereClause | None = None,
) -> list[BuildConcept]:
    if not _conditions_supported(datasource, conditions, concepts_by_address):
        return []

    datasource_grain = datasource.grain
    # Grand total target: any finer-grain additive aggregate rolls up to one row.
    # A grand-total datasource is an exact match, handled outside the rollup branch.
    if not target_grain.components:
        if not datasource_grain.components:
            return []
        return [
            concept
            for concept in requested_concepts
            if concept.is_aggregate
            and _is_additive_aggregate(concept)
            and _datasource_has_matching_additive_aggregate(datasource, concept)
        ]
    target_canonicals = [
        concepts_by_address[component].canonical_address
        for component in target_grain.components
        if component in concepts_by_address
    ]
    if len(target_canonicals) != len(set(target_canonicals)):
        return []
    if datasource_grain.issubset(target_grain):
        return []
    # The table must be able to GROUP BY the target grain: every component bound
    # here or reachable as a property of this grain.
    if not _addresses_reachable(
        datasource, set(target_canonicals), concepts_by_address
    ):
        return []

    dropped = datasource_grain - target_grain
    dropped_concepts = [
        concepts_by_address.get(address) for address in dropped.components
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


def merge_rollup_concepts(
    parent_grains: Iterable[BuildGrain | None],
    all_concepts: list[BuildConcept],
    concepts_by_address: Mapping[str, BuildConcept],
) -> list[BuildConcept]:
    """The additive aggregates a merge of `parent_grains` must SUM-roll up to
    reach `all_concepts`' grain: the joined rows sit at the parents' finer
    grain and every target component the join does not reach is a property
    keyed within it (a per-customer count joined to a customer's region rolls
    to region). Empty when no rollup applies, including when any requested
    aggregate is non-additive: a merge cannot regroup a distinct count."""
    additive = [c for c in all_concepts if c.is_aggregate and _is_additive_aggregate(c)]
    if not additive or len(additive) != sum(1 for c in all_concepts if c.is_aggregate):
        return []
    merge_components: set[str] = set()
    for grain in parent_grains:
        if grain and grain.components:
            merge_components.update(grain.components)
    target_components = {c.address for c in all_concepts if not c.is_aggregate}
    # A grain component must actually be summed AWAY. When every one of them
    # survives into the output the merge already sits at the target's row
    # identity, and the outputs beyond it are attributes the join carried in at
    # that same grain: grouping there re-sums one row per group, buying an
    # identical answer and a GROUP BY (thelook q17's pair rollup read beside
    # both its keys' dimension attributes).
    if not merge_components - target_components:
        return []
    unreached = target_components - merge_components
    if not (merge_components and unreached):
        return []
    for address in unreached:
        concept = concepts_by_address.get(address)
        if (
            concept is None
            or concept.purpose != Purpose.PROPERTY
            or not concept.keys
            or not concept.keys.issubset(merge_components)
        ):
            return []
    return additive
