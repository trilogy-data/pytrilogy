from typing import List, Optional, Tuple

from trilogy.constants import logger
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    JoinType,
    Purpose,
    SourceType,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildOrderBy,
    BuildParenthetical,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import BaseJoin, QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.join_resolution import get_node_joins
from trilogy.core.processing.nodes.base_node import (
    NodeJoin,
    StrategyNode,
    resolve_concept_map,
)
from trilogy.core.processing.utility import find_nullable_concepts
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"

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


def deduplicate_nodes(
    merged: dict[str, QueryDatasource | BuildDatasource],
    logging_prefix: str,
    environment: BuildEnvironment,
) -> tuple[bool, dict[str, QueryDatasource | BuildDatasource], set[str]]:
    duplicates = False
    removed: set[str] = set()
    set_map: dict[str, set[str]] = {}
    for k, v in merged.items():
        unique_outputs = [
            # the concept may be a in a different environment for a rowset.
            (environment.concepts.get(x.address) or x).address
            for x in v.output_concepts
            if x not in v.partial_concepts
        ]
        set_map[k] = set(unique_outputs)
    for k1, v1 in set_map.items():
        found = False
        for k2, v2 in set_map.items():
            if k1 == k2:
                continue
            if (
                v1.issubset(v2)
                and merged[k1].grain.issubset(merged[k2].grain)
                and not merged[k2].partial_concepts
                and not merged[k1].partial_concepts
                and not merged[k2].condition
                and not merged[k1].condition
            ):
                og = merged[k1]
                subset_to = merged[k2]
                logger.info(
                    f"{logging_prefix}{LOGGER_PREFIX} extraneous parent node that is subset of another parent node {og.grain.issubset(subset_to.grain)} {og.grain.components} {subset_to.grain.components}"
                )
                merged = {k: v for k, v in merged.items() if k != k1}
                removed.add(k1)
                duplicates = True
                found = True
                break
        if found:
            break

    return duplicates, merged, removed


def deduplicate_nodes_and_joins(
    joins: List[NodeJoin] | None,
    merged: dict[str, QueryDatasource | BuildDatasource],
    logging_prefix: str,
    environment: BuildEnvironment,
) -> Tuple[List[NodeJoin] | None, dict[str, QueryDatasource | BuildDatasource]]:
    # it's possible that we have more sources than we need
    duplicates = True
    while duplicates:
        duplicates = False
        duplicates, merged, removed = deduplicate_nodes(
            merged, logging_prefix, environment=environment
        )
        # filter out any removed joins
        if joins is not None:
            joins = [
                j
                for j in joins
                if j.left_node.resolve().identifier not in removed
                and j.right_node.resolve().identifier not in removed
            ]
    return joins, merged


def source_concept_for_address(
    source: QueryDatasource | BuildDatasource, address: str
) -> BuildConcept | None:
    return next(
        (concept for concept in source.output_concepts if concept.address == address),
        None,
    )


def concept_covers_grain(concept: BuildConcept, grain: BuildGrain) -> bool:
    if concept.address in grain.components:
        return True
    if (
        concept.derivation == Derivation.MULTISELECT
        and concept.keys
        and grain.components.issubset(concept.keys)
    ):
        return True
    return False


def datasource_effective_grain(
    source: QueryDatasource | BuildDatasource,
) -> BuildGrain:
    key_outputs = {
        concept.address
        for concept in source.output_concepts
        if concept.purpose == Purpose.KEY
    }
    return source.grain + BuildGrain(components=key_outputs)


def concept_coverage_addresses(concept: BuildConcept) -> set[str]:
    addresses = {concept.address, *concept.pseudonyms}
    if concept.derivation == Derivation.MULTISELECT and concept.keys:
        addresses.update(concept.keys)
    if (
        concept.derivation == Derivation.BASIC
        and isinstance(concept.lineage, BuildFunction)
        and concept.lineage.concept_arguments
    ):
        addresses.update(arg.address for arg in concept.lineage.concept_arguments)
    return addresses


def grain_coverage_addresses(
    grain: BuildGrain, environment: BuildEnvironment
) -> set[str]:
    addresses: set[str] = set()
    for candidate in (grain, rowset_source_grain(grain, environment)):
        for address in candidate.components:
            concept = environment.concepts[address]
            addresses.update(concept_coverage_addresses(concept))
    return addresses


def concept_covered_by_grain(
    concept: BuildConcept, grain: BuildGrain, environment: BuildEnvironment
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

    right_grain = datasource_effective_grain(join.right_datasource)
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
        coverage.add(materialized.address)
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
    join: BaseJoin | UnnestJoin, grain: BuildGrain, environment: BuildEnvironment
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
    join: BaseJoin | UnnestJoin, grain: BuildGrain, environment: BuildEnvironment
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


def _non_null_proofs(
    condition: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    proofs: set[str] = set()
    for atom in decompose_condition(condition):
        if isinstance(atom, BuildParenthetical):
            if isinstance(
                atom.content,
                (BuildComparison, BuildConditional, BuildParenthetical),
            ):
                proofs |= _non_null_proofs(atom.content)
            continue
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator in NULL_PROPAGATING_OPS:
            proofs |= _concepts_in_expression(atom.left)
            proofs |= _concepts_in_expression(atom.right)
    return proofs


def _datasource_addresses(source: QueryDatasource | BuildDatasource) -> set[str]:
    return {concept.address for concept in source.output_concepts}


def _left_join_addresses(
    join: BaseJoin, final_datasets: list[QueryDatasource | BuildDatasource]
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


def _downgrade_join_for_condition(
    join: BaseJoin | UnnestJoin,
    condition: BuildComparison | BuildConditional | BuildParenthetical | None,
    final_datasets: list[QueryDatasource | BuildDatasource],
) -> None:
    if not isinstance(join, BaseJoin):
        return
    if join.join_type != JoinType.FULL or condition is None:
        return
    proofs = _non_null_proofs(condition)
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
    final_datasets: list[QueryDatasource | BuildDatasource],
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
    grain = BuildGrain()
    for source in final_datasets:
        if source.identifier in cardinality_preserved:
            continue
        grain += datasource_effective_grain(source)
    return grain


def rowset_source_grain(grain: BuildGrain, environment: BuildEnvironment) -> BuildGrain:
    concepts: list[str] = []
    for address in grain.components:
        concept = environment.concepts[address]
        if concept.derivation == Derivation.ROWSET and isinstance(
            concept.lineage, BuildRowsetItem
        ):
            concepts.append(concept.lineage.content.address)
        else:
            concepts.append(address)
    return BuildGrain.from_concepts(concepts, environment=environment)


def grain_satisfied_by_pregrain(
    pregrain: BuildGrain, grain: BuildGrain, environment: BuildEnvironment
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
            for address in _non_null_proofs(condition)
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


class MergeNode(StrategyNode):
    source_type = SourceType.MERGE

    def __init__(
        self,
        input_concepts: List[BuildConcept],
        output_concepts: List[BuildConcept],
        environment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        node_joins: List[NodeJoin] | None = None,
        join_concepts: Optional[List] = None,
        force_join_type: Optional[JoinType] = None,
        partial_concepts: Optional[List[BuildConcept]] = None,
        rollup_concepts: Optional[List[BuildConcept]] = None,
        nullable_concepts: Optional[List[BuildConcept]] = None,
        force_group: bool | None = None,
        depth: int = 0,
        grain: BuildGrain | None = None,
        conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        preexisting_conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        hidden_concepts: set[str] | None = None,
        virtual_output_concepts: List[BuildConcept] | None = None,
        existence_concepts: List[BuildConcept] | None = None,
        ordering: BuildOrderBy | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            rollup_concepts=rollup_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            grain=grain,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            virtual_output_concepts=virtual_output_concepts,
            existence_concepts=existence_concepts,
            ordering=ordering,
        )
        self.join_concepts = join_concepts
        self.force_join_type = force_join_type
        self.node_joins: List[NodeJoin] | None = node_joins

        final_joins: List[NodeJoin] = []
        if self.node_joins is not None:
            for join in self.node_joins:
                if join.left_node.resolve().name == join.right_node.resolve().name:
                    continue
                final_joins.append(join)
            self.node_joins = final_joins

    def translate_node_joins(self, node_joins: List[NodeJoin]) -> List[BaseJoin]:
        joins = []
        for join in node_joins:
            left = join.left_node.resolve()
            right = join.right_node.resolve()
            if left.identifier == right.identifier:
                raise SyntaxError(f"Cannot join node {left.identifier} to itself")
            joins.append(
                BaseJoin(
                    left_datasource=left,
                    right_datasource=right,
                    join_type=join.join_type,
                    concepts=join.concepts,
                    concept_pairs=join.concept_pairs,
                    modifiers=join.modifiers,
                )
            )
        return joins

    def create_full_joins(self, dataset_list: List[QueryDatasource | BuildDatasource]):
        joins = []
        seen = set()
        for left_value in dataset_list:
            for right_value in dataset_list:
                if left_value.identifier == right_value.identifier:
                    continue
                if left_value.identifier in seen and right_value.identifier in seen:
                    continue
                joins.append(
                    BaseJoin(
                        left_datasource=left_value,
                        right_datasource=right_value,
                        join_type=JoinType.FULL,
                        concepts=[],
                    )
                )
                seen.add(left_value.identifier)
                seen.add(right_value.identifier)
        return joins

    def generate_joins(
        self,
        final_datasets,
        final_joins: List[NodeJoin] | None,
        pregrain: BuildGrain,
        grain: BuildGrain,
        environment: BuildEnvironment,
    ) -> List[BaseJoin | UnnestJoin]:
        # only finally, join between them for unique values
        dataset_list: List[QueryDatasource | BuildDatasource] = sorted(
            final_datasets,
            key=lambda x: (-len(x.grain.components), x.identifier),
        )

        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} Merge node has {len(dataset_list)} parents, starting merge"
        )
        if final_joins is None:
            if not pregrain.components:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} no grain components, doing full join"
                )
                joins = self.create_full_joins(dataset_list)
            else:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} inferring node joins to target grain {str(grain)}"
                )
                joins = get_node_joins(dataset_list, environment=environment)
        elif final_joins:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} translating provided node joins {len(final_joins)}"
            )
            joins = self.translate_node_joins(final_joins)
        else:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} Final joins is not null {final_joins} but is empty, skipping join generation"
            )
            return []
        if self.force_join_type is not None:
            for j in joins:
                if isinstance(j, BaseJoin):
                    j.join_type = self.force_join_type
        return joins

    def _resolve(self) -> QueryDatasource:
        parent_sources: List[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]
        merged: dict[str, QueryDatasource | BuildDatasource] = {}
        final_joins: List[NodeJoin] | None = self.node_joins
        for source in parent_sources:
            if source.identifier in merged:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} merging parent node with {source.identifier} into existing"
                )
                merged[source.identifier] = merged[source.identifier] + source
            else:
                merged[source.identifier] = source

        # it's possible that we have more sources than we need
        final_joins, merged = deduplicate_nodes_and_joins(
            final_joins, merged, self.logging_prefix, self.environment
        )
        # early exit if we can just return the parent
        final_datasets: List[QueryDatasource | BuildDatasource] = sorted(
            merged.values(), key=lambda source: source.identifier
        )

        existence_final = [
            x
            for x in final_datasets
            if all([y in self.existence_concepts for y in x.output_concepts])
        ]
        if len(merged.keys()) == 1:
            final: QueryDatasource | BuildDatasource = list(merged.values())[0]
            if (
                set([c.address for c in final.output_concepts])
                == set([c.address for c in self.output_concepts])
                and not self.conditions
                and isinstance(final, QueryDatasource)
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node has only one parent with the same"
                    " outputs as this merge node, dropping merge node "
                )
                # push up any conditions we need
                final.ordering = self.ordering
                return final

        # if we have multiple candidates, see if one is good enough
        for dataset in final_datasets:
            output_set = set(
                [
                    c.address
                    for c in dataset.output_concepts
                    if c.address not in [x.address for x in dataset.partial_concepts]
                ]
            )
            if (
                all([c.address in output_set for c in self.all_concepts])
                and not self.conditions
                and isinstance(dataset, QueryDatasource)
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node not required as parent node {dataset.source_type}"
                    f" has all required output properties with partial {[c.address for c in dataset.partial_concepts]}"
                    f" and self has no conditions ({self.conditions})"
                )
                dataset.ordering = self.ordering
                return dataset

        pregrain = BuildGrain()

        for source in final_datasets:
            if all(
                [x.address in self.existence_concepts for x in source.output_concepts]
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} skipping existence only source with {source.output_concepts} from grain accumulation"
                )
                continue
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} adding source grain {source.grain} from source {source.identifier} to pregrain"
            )
            pregrain += source.grain
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} pregrain is now {pregrain}"
            )

        raw_pregrain = BuildGrain.from_concepts(
            pregrain.components, environment=self.environment
        )

        grain = self.grain if self.grain else raw_pregrain
        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} has pre grain {raw_pregrain} and final merge node grain {grain}"
        )
        join_candidates = [x for x in final_datasets if x not in existence_final]
        if len(join_candidates) > 1:
            joins: List[BaseJoin | UnnestJoin] = self.generate_joins(
                join_candidates, final_joins, raw_pregrain, grain, self.environment
            )
        else:
            joins = []

        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} Final join count for CTE parent count {len(join_candidates)} is {len(joins)}"
        )
        for join in joins:
            _downgrade_join_for_condition(join, self.conditions, final_datasets)
        full_join_concepts = []
        for join in joins:
            if isinstance(join, BaseJoin) and join.join_type == JoinType.FULL:
                full_join_concepts += join.input_concepts
        pregrain = BuildGrain.from_concepts(
            calculate_joined_pregrain(
                final_datasets, joins, grain, self.environment
            ).components,
            environment=self.environment,
        )
        pregrain += condition_key_grain(self.conditions, self.environment)
        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} effective joined pregrain is {pregrain}"
        )
        condition_key_requires_group = has_condition_key_outside_grain(
            self.conditions, grain, self.environment
        )

        if self.force_group is True:
            rowset_output = any(
                concept.derivation == Derivation.ROWSET
                for concept in self.output_concepts
            )
            force_group = condition_key_requires_group or not (
                rowset_output
                and grain_satisfied_by_pregrain(pregrain, grain, self.environment)
            )
        elif self.whole_grain:
            force_group = False
        elif condition_key_requires_group:
            force_group = True
        elif self.force_group is False:
            force_group = not grain_satisfied_by_pregrain(
                pregrain, grain, self.environment
            )
        elif not grain_satisfied_by_pregrain(pregrain, grain, self.environment):
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} no parents include full grain {grain} and pregrain {pregrain} does not match, assume must group to grain. Have {[str(d.grain) for d in final_datasets]}"
            )
            force_group = True
        else:
            force_group = None

        qd_joins: List[BaseJoin | UnnestJoin] = [*joins]

        source_map = resolve_concept_map(
            final_datasets,
            targets=self.output_concepts,
            inherited_inputs=self.input_concepts + self.existence_concepts,
            full_joins=full_join_concepts,
        )
        nullable_concepts = find_nullable_concepts(
            source_map=source_map, joins=joins, datasources=final_datasets
        )
        rollup_concepts = unique(
            self.rollup_concepts
            + [
                c
                for source in final_datasets
                if isinstance(source, QueryDatasource)
                for c in source.rollup_concepts
                if c.address in {out.address for out in self.output_concepts}
            ],
            "address",
        )
        if force_group:

            grain = BuildGrain.from_concepts(
                self.output_concepts, environment=self.environment
            )
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} forcing group by to achieve grain {grain}"
            )
        qds = QueryDatasource(
            input_concepts=unique(self.input_concepts, "address"),
            output_concepts=unique(self.output_concepts, "address"),
            datasources=final_datasets,
            source_type=self.source_type,
            source_map=source_map,
            joins=qd_joins,
            grain=grain,
            nullable_concepts=[
                x for x in self.output_concepts if x.address in nullable_concepts
            ],
            partial_concepts=self.partial_concepts,
            rollup_concepts=rollup_concepts,
            force_group=force_group,
            condition=self.conditions,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
        )
        return qds

    def copy(self) -> "MergeNode":
        return MergeNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            force_group=self.force_group,
            grain=self.grain,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            nullable_concepts=list(self.nullable_concepts),
            hidden_concepts=set(self.hidden_concepts),
            virtual_output_concepts=list(self.virtual_output_concepts),
            node_joins=list(self.node_joins) if self.node_joins else None,
            join_concepts=list(self.join_concepts) if self.join_concepts else None,
            force_join_type=self.force_join_type,
            existence_concepts=list(self.existence_concepts),
            ordering=self.ordering,
        )
