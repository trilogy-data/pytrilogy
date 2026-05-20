from __future__ import annotations

from trilogy.core.enums import Derivation, JoinType, Purpose
from trilogy.core.models.build import (
    BuildBetween,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFilterItem,
    BuildGrain,
    BuildParenthetical,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import BaseJoin, QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import (
    NULL_PROPAGATING_OPS,
    concepts_implied_non_null,
    decompose_condition,
)

GrainSource = QueryDatasource | BuildDatasource


def non_null_proofs(
    condition: BuildComparison | BuildConditional | BuildParenthetical | BuildBetween,
) -> set[str]:
    """Concept addresses that this condition forces non-null in surviving rows.

    Logical-stage analysis: only descends through null-propagating operators,
    not ``IS NOT NULL``. The merge-stage caller can't see how merged join keys
    will materialize as ``COALESCE(left.k, right.k)`` at SQL time, so it must
    avoid claiming a shared key non-null on either side individually — which
    would happen if ``IS NOT NULL`` were honored here. The post-CTE
    ``DowngradeFullJoinOnGuards`` pass operates on materialized SQL and can
    safely honor the fuller form.
    """
    proofs: set[str] = set()
    for atom in decompose_condition(condition):
        if isinstance(atom, BuildParenthetical):
            if isinstance(
                atom.content,
                (BuildComparison, BuildConditional, BuildParenthetical, BuildBetween),
            ):
                proofs |= non_null_proofs(atom.content)
            continue
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator in NULL_PROPAGATING_OPS:
            proofs |= concepts_implied_non_null(atom.left)
            proofs |= concepts_implied_non_null(atom.right)
    return proofs


def _source_concept_for_address(
    source: GrainSource,
    address: str,
) -> BuildConcept | None:
    return next(
        (
            concept
            for concept in source.output_concepts
            if address in concept.equivalent_addresses
        ),
        None,
    )


def _concept_covers_grain(concept: BuildConcept, grain: BuildGrain) -> bool:
    if grain.components & concept.equivalent_addresses:
        return True
    if (
        concept.derivation == Derivation.MULTISELECT
        and concept.keys
        and grain.components.issubset(concept.keys)
    ):
        return True
    return False


def _concept_coverage_addresses(concept: BuildConcept) -> set[str]:
    addresses = set(concept.equivalent_addresses)
    if concept.is_aggregate and concept.grain and not concept.grain.abstract:
        addresses.update(concept.grain.components)
    if concept.derivation == Derivation.MULTISELECT and concept.keys:
        addresses.update(concept.keys)
    return addresses


def concept_source_address(concept: BuildConcept) -> str:
    if concept.derivation == Derivation.ROWSET and isinstance(
        concept.lineage, BuildRowsetItem
    ):
        return concept.lineage.content.address
    if concept.derivation == Derivation.FILTER and isinstance(
        concept.lineage, BuildFilterItem
    ):
        content = concept.lineage.content
        if isinstance(content, BuildConcept):
            return content.address
    return concept.address


def rowset_source_grain(
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> BuildGrain:
    concepts = [
        concept_source_address(environment.concepts[address])
        for address in grain.components
    ]
    return BuildGrain.from_concepts(concepts, environment=environment)


def _grain_coverage_addresses(
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> set[str]:
    addresses: set[str] = set()
    for candidate in (grain, rowset_source_grain(grain, environment)):
        for address in candidate.components:
            concept = environment.concepts[address]
            addresses.update(_concept_coverage_addresses(concept))
    return addresses


def _concept_covered_by_grain(
    concept: BuildConcept,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return bool(
        _concept_coverage_addresses(concept)
        & _grain_coverage_addresses(grain, environment)
    )


def _join_right_preserves_cardinality(join: BaseJoin | UnnestJoin) -> bool:
    if not isinstance(join, BaseJoin):
        return False
    if join.join_type in (JoinType.FULL, JoinType.RIGHT_OUTER, JoinType.CROSS):
        return False
    if not join.concept_pairs and not join.concepts:
        return False

    right_grain = join.right_datasource.effective_grain
    if not right_grain.components:
        return True
    right_keys = (
        [pair.right for pair in join.concept_pairs]
        if join.concept_pairs
        else join.concepts or []
    )
    materialized_keys = [
        _source_concept_for_address(join.right_datasource, key.address) or key
        for key in right_keys
    ]
    coverage: set[str] = set()
    for key in materialized_keys:
        coverage.update(_concept_coverage_addresses(key))
    return right_grain.components.issubset(coverage) or any(
        _concept_covers_grain(key, right_grain) for key in materialized_keys
    )


def _join_left_keys_covered_by_grain(
    join: BaseJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    if not isinstance(join, BaseJoin):
        return False
    if join.concept_pairs:
        left_keys = [
            _source_concept_for_address(pair.existing_datasource, pair.left.address)
            or pair.left
            for pair in join.concept_pairs
        ]
    elif join.concepts:
        left_keys = [
            (
                _source_concept_for_address(join.left_datasource, concept.address)
                if join.left_datasource
                else None
            )
            or concept
            for concept in join.concepts
        ]
    else:
        return False
    return all(
        _concept_covered_by_grain(concept, grain, environment) for concept in left_keys
    )


def _join_right_grain_can_be_omitted(
    join: BaseJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return _join_right_preserves_cardinality(join) and _join_left_keys_covered_by_grain(
        join, grain, environment
    )


def _datasource_addresses(source: GrainSource) -> set[str]:
    return {concept.address for concept in source.output_concepts}


def _left_join_addresses(
    join: BaseJoin,
    final_datasets: list[GrainSource],
) -> set[str]:
    if join.left_datasource is not None:
        return _datasource_addresses(join.left_datasource)
    if not join.concept_pairs:
        return {
            address
            for source in final_datasets
            if source.identifier != join.right_datasource.identifier
            for address in _datasource_addresses(source)
        }
    return {
        address
        for pair in join.concept_pairs or []
        for address in _datasource_addresses(pair.existing_datasource)
    }


def downgrade_join_for_condition(
    join: BaseJoin | UnnestJoin,
    condition: BuildComparison | BuildConditional | BuildParenthetical | BuildBetween | None,
    final_datasets: list[GrainSource],
) -> None:
    if not isinstance(join, BaseJoin):
        return
    if join.join_type != JoinType.FULL or condition is None:
        return
    proofs = non_null_proofs(condition)
    if not proofs:
        return
    if join.concept_pairs:
        left_keys = {pair.left.address for pair in join.concept_pairs}
        right_keys = {pair.right.address for pair in join.concept_pairs}
    else:
        left_keys = {concept.address for concept in join.concepts or []}
        right_keys = set(left_keys)
    left_all = _left_join_addresses(join, final_datasets)
    right_all = _datasource_addresses(join.right_datasource)
    left_forced = bool(proofs & (left_all - right_all)) or (
        bool(left_keys) and left_keys.issubset(proofs)
    )
    right_forced = bool(proofs & (right_all - left_all)) or (
        bool(right_keys) and right_keys.issubset(proofs)
    )
    if left_forced and right_forced:
        join.join_type = JoinType.INNER
    elif left_forced:
        join.join_type = JoinType.LEFT_OUTER
    elif right_forced:
        join.join_type = JoinType.RIGHT_OUTER


def calculate_joined_pregrain(
    final_datasets: list[GrainSource],
    joins: list[BaseJoin | UnnestJoin],
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> BuildGrain:
    cardinality_preserved = {
        join.right_datasource.identifier
        for join in joins
        if isinstance(join, BaseJoin)
        and _join_right_grain_can_be_omitted(join, grain, environment)
    }
    output = BuildGrain()
    for source in final_datasets:
        if source.identifier in cardinality_preserved:
            continue
        output += source.effective_grain
    return output


def grain_satisfied_by_pregrain(
    pregrain: BuildGrain,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    if pregrain.issubset(grain):
        return True
    if pregrain.issubset(rowset_source_grain(grain, environment)):
        return True
    # Expand grain via _concept_coverage_addresses so a MULTISELECT align
    # identity covers its source keys (the JOIN keys it was derived from).
    # Without this, a pregrain carrying the source keys looks like extra
    # grain to a merge node whose grain only references the align alias.
    coverage = _grain_coverage_addresses(grain, environment)
    return pregrain.components.issubset(coverage)


def condition_key_grain(
    condition: BuildComparison | BuildConditional | BuildParenthetical | BuildBetween | None,
    environment: BuildEnvironment,
) -> BuildGrain:
    if condition is None:
        return BuildGrain()
    return BuildGrain(
        components={
            address
            for address in non_null_proofs(condition)
            if environment.concepts[address].purpose == Purpose.KEY
        }
    )


def has_condition_key_outside_grain(
    condition: BuildComparison | BuildConditional | BuildParenthetical | BuildBetween | None,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    condition_grain = condition_key_grain(condition, environment)
    if not condition_grain.components:
        return False
    return not condition_grain.issubset(rowset_source_grain(grain, environment))
