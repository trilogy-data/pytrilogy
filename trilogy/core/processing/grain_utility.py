from __future__ import annotations

from trilogy.core.enums import ComparisonOperator, Derivation, JoinType, Purpose
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildParenthetical,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import BaseJoin, QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import decompose_condition

GrainSource = QueryDatasource | BuildDatasource

NULL_PROPAGATING_OPS: tuple[ComparisonOperator, ...] = (
    ComparisonOperator.EQ,
    ComparisonOperator.NE,
    ComparisonOperator.LT,
    ComparisonOperator.GT,
    ComparisonOperator.LTE,
    ComparisonOperator.GTE,
    ComparisonOperator.LIKE,
    ComparisonOperator.ILIKE,
    ComparisonOperator.IN,
    ComparisonOperator.NOT_IN,
    ComparisonOperator.CONTAINS,
)


def source_concept_for_address(
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


def concept_covers_grain(concept: BuildConcept, grain: BuildGrain) -> bool:
    if grain.components & concept.equivalent_addresses:
        return True
    if (
        concept.derivation == Derivation.MULTISELECT
        and concept.keys
        and grain.components.issubset(concept.keys)
    ):
        return True
    return False


def concept_coverage_addresses(concept: BuildConcept) -> set[str]:
    addresses = set(concept.equivalent_addresses)
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


def grain_coverage_addresses(
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> set[str]:
    addresses: set[str] = set()
    for candidate in (grain, rowset_source_grain(grain, environment)):
        for address in candidate.components:
            concept = environment.concepts[address]
            addresses.update(concept_coverage_addresses(concept))
    return addresses


def concept_covered_by_grain(
    concept: BuildConcept,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return bool(
        concept_coverage_addresses(concept)
        & grain_coverage_addresses(grain, environment)
    )


def join_right_preserves_cardinality(join: BaseJoin | UnnestJoin) -> bool:
    if not isinstance(join, BaseJoin):
        return False
    if join.join_type in (JoinType.FULL, JoinType.RIGHT_OUTER, JoinType.CROSS):
        return False
    if not join.concept_pairs and not join.concepts:
        return False

    right_grain = join.right_datasource.effective_grain
    if not right_grain.components:
        return True
    coverage: set[str] = set()
    right_keys = (
        [pair.right for pair in join.concept_pairs]
        if join.concept_pairs
        else join.concepts or []
    )
    for join_key in right_keys:
        materialized = (
            source_concept_for_address(join.right_datasource, join_key.address)
            or join_key
        )
        coverage.update(materialized.equivalent_addresses)
        if materialized.derivation == Derivation.MULTISELECT:
            coverage.update(materialized.keys or set())
    return right_grain.components.issubset(coverage) or any(
        concept_covers_grain(
            source_concept_for_address(join.right_datasource, join_key.address)
            or join_key,
            right_grain,
        )
        for join_key in right_keys
    )


def join_left_keys_covered_by_grain(
    join: BaseJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    if not isinstance(join, BaseJoin):
        return False
    if join.concept_pairs:
        left_keys = [
            source_concept_for_address(pair.existing_datasource, pair.left.address)
            or pair.left
            for pair in join.concept_pairs
        ]
    elif join.concepts:
        left_keys = [
            (
                source_concept_for_address(join.left_datasource, concept.address)
                if join.left_datasource
                else None
            )
            or concept
            for concept in join.concepts
        ]
    else:
        return False
    return all(
        concept_covered_by_grain(concept, grain, environment) for concept in left_keys
    )


def join_right_grain_can_be_omitted(
    join: BaseJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return join_right_preserves_cardinality(join) and join_left_keys_covered_by_grain(
        join, grain, environment
    )


def _concepts_in_expression(value: object) -> set[str]:
    if isinstance(value, BuildConcept):
        return {value.address}
    if isinstance(value, BuildParenthetical):
        return _concepts_in_expression(value.content)
    if isinstance(value, BuildAggregateWrapper):
        return _concepts_in_expression(value.function)
    if isinstance(value, BuildFunction):
        addresses: set[str] = set()
        for arg in value.arguments:
            addresses |= _concepts_in_expression(arg)
        return addresses
    return set()


def non_null_proofs(
    condition: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    proofs: set[str] = set()
    for atom in decompose_condition(condition):
        if isinstance(atom, BuildParenthetical):
            if isinstance(
                atom.content,
                (BuildComparison, BuildConditional, BuildParenthetical),
            ):
                proofs |= non_null_proofs(atom.content)
            continue
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator in NULL_PROPAGATING_OPS:
            proofs |= _concepts_in_expression(atom.left)
            proofs |= _concepts_in_expression(atom.right)
    return proofs


def datasource_addresses(source: GrainSource) -> set[str]:
    return {concept.address for concept in source.output_concepts}


def left_join_addresses(
    join: BaseJoin,
    final_datasets: list[GrainSource],
) -> set[str]:
    if join.left_datasource is not None:
        return datasource_addresses(join.left_datasource)
    if not join.concept_pairs:
        return {
            address
            for source in final_datasets
            if source.identifier != join.right_datasource.identifier
            for address in datasource_addresses(source)
        }
    return {
        address
        for pair in join.concept_pairs or []
        for address in datasource_addresses(pair.existing_datasource)
    }


def downgrade_join_for_condition(
    join: BaseJoin | UnnestJoin,
    condition: BuildComparison | BuildConditional | BuildParenthetical | None,
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
    left_all = left_join_addresses(join, final_datasets)
    right_all = datasource_addresses(join.right_datasource)
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
        and join_right_grain_can_be_omitted(join, grain, environment)
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
    return pregrain.issubset(rowset_source_grain(grain, environment))


def condition_key_grain(
    condition: BuildComparison | BuildConditional | BuildParenthetical | None,
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
    condition: BuildComparison | BuildConditional | BuildParenthetical | None,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    condition_grain = condition_key_grain(condition, environment)
    if not condition_grain.components:
        return False
    return not condition_grain.issubset(rowset_source_grain(grain, environment))
