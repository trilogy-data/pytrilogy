"""Targets a summary key relates to through a list (`Cover.keyed_relation_count`).

`ecoregion_id` beside a `(city, species)` summary is no function of the key:
a species ranges over several ecoregions. But the ranges are a list keyed on
`species`, so each summary row belongs to the groups its list names, and the
count by ecoregion is the summary joined to the DISTINCT `(species,
ecoregion_id)` pairs and re-summed (`Cover.distinct_pairs_resum_exact`). The
pairs must be distinct: a list repeating an element would count a summary row
once per repeat (`Cover.repeated_element_counts_once`)."""

from collections.abc import Iterable

from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import (
    _concepts_reachable,
    _datasource_has_matching_rollup_aggregate,
    _is_rollable_aggregate,
    get_rollup_concepts,
)


def _unnest_origins(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    spellings = (concept.address, *sorted(concept.pseudonyms))
    found = [concept] + [
        environment.alias_origin_lookup[s]
        for s in spellings
        if s in environment.alias_origin_lookup
    ]
    return [c for c in found if c.derivation == Derivation.UNNEST and c.lineage]


def relation_keys(
    concept: BuildConcept, grain: set[str], environment: BuildEnvironment
) -> frozenset[str] | None:
    """The grain keys whose list `concept` is an element of (or a property of
    such an element), when that list is a property keyed inside `grain`."""
    for origin in _unnest_origins(concept, environment):
        args = origin.lineage.concept_arguments[:1] if origin.lineage else []
        for arg in args:
            listed = environment.concepts.get(arg.address)
            if (
                listed is not None
                and listed.lineage is None
                and listed.purpose == Purpose.PROPERTY
                and listed.keys
                and set(listed.keys) <= grain
            ):
                return frozenset(listed.keys)
    if concept.lineage is None and concept.purpose == Purpose.PROPERTY and concept.keys:
        keys: set[str] = set()
        for key in concept.keys:
            owner = environment.concepts.get(key)
            found = (
                relation_keys(owner, grain, environment) if owner is not None else None
            )
            if found is None:
                return None
            keys |= found
        return frozenset(keys)
    return None


def relation_targets(
    datasource: BuildDatasource,
    target_grain: BuildGrain,
    environment: BuildEnvironment,
) -> dict[str, frozenset[str]]:
    """Each target `datasource` does not bind that its key relates to, mapped
    to the grain keys it is joined on."""
    grain = set(datasource.grain.components)
    bound = {c.canonical_address for c in datasource.output_concepts}
    out: dict[str, frozenset[str]] = {}
    for address in sorted(target_grain.components):
        concept = environment.concepts.get(address)
        if concept is None or concept.canonical_address in bound:
            continue
        keys = relation_keys(concept, grain, environment)
        if keys is not None:
            out[address] = keys
    return out


def proxy_grain(
    target_grain: BuildGrain, relations: dict[str, frozenset[str]]
) -> BuildGrain:
    """The grain the summary rolls to before the relation join: each related
    target replaced by the keys it is joined on."""
    components = set(target_grain.components) - set(relations)
    for keys in relations.values():
        components |= keys
    return BuildGrain(components=components)


def relation_rollup_concepts(
    datasource: BuildDatasource,
    requested: list[BuildConcept],
    environment: BuildEnvironment,
    datasources: Iterable[BuildDatasource],
    target_grain: BuildGrain,
) -> list[BuildConcept]:
    """The requested aggregates `datasource` answers at `target_grain` through
    a relation join: those it rolls up to the proxy grain, or stores there."""
    relations = relation_targets(datasource, target_grain, environment)
    if not relations:
        return []
    proxy = proxy_grain(target_grain, relations)
    if not datasource.grain.issubset(proxy):
        return get_rollup_concepts(
            datasource,
            requested,
            environment.concepts,
            datasources,
            proxy,
        )
    others = [
        environment.concepts[a] for a in proxy.components if a in environment.concepts
    ]
    if not _concepts_reachable(datasource, others, environment.concepts):
        return []
    return [
        c
        for c in requested
        if c.is_aggregate
        and _is_rollable_aggregate(c)
        and _datasource_has_matching_rollup_aggregate(datasource, c)
    ]
