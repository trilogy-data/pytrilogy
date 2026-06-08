from typing import List, Optional, Tuple

from trilogy.constants import logger
from trilogy.core.enums import Derivation, JoinType, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildOrderBy,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import BaseJoin, QueryDatasource, UnnestJoin
from trilogy.core.processing.grain_utility import (
    calculate_joined_pregrain,
    condition_key_grain,
    downgrade_join_for_condition,
    downgrade_join_for_proofs,
    grain_satisfied_by_pregrain,
    has_condition_key_outside_grain,
    non_null_proofs,
)
from trilogy.core.processing.join_resolution import (
    _collect_deep_partial_addresses,
    compute_outer_null_status,
    get_node_joins,
    prune_outer_join_pairs,
)
from trilogy.core.processing.nodes.base_node import (
    NodeJoin,
    StrategyNode,
    resolve_concept_map,
    resolve_existence_map,
)
from trilogy.core.processing.utility import find_nullable_concepts
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"


def _has_applied_condition(source: QueryDatasource | BuildDatasource) -> bool:
    if isinstance(source, QueryDatasource):
        return bool(source.condition) or any(
            _has_applied_condition(parent) for parent in source.datasources
        )
    return bool(source.where)


def _collect_applied_conditions(source: QueryDatasource | BuildDatasource) -> list:
    """All filter conditions applied anywhere within a parent branch's tree."""
    out: list = []
    if isinstance(source, QueryDatasource):
        if source.condition is not None:
            out.append(source.condition)
        for parent in source.datasources:
            out.extend(_collect_applied_conditions(parent))
    return out


def deduplicate_nodes(
    merged: dict[str, QueryDatasource | BuildDatasource],
    logging_prefix: str,
    environment: BuildEnvironment,
) -> tuple[bool, dict[str, QueryDatasource | BuildDatasource], set[str]]:
    duplicates = False
    removed: set[str] = set()
    set_map: dict[str, set[str]] = {}
    for k, v in merged.items():
        # Hidden concepts are excluded by ``resolve_concept_map`` for
        # QueryDatasources, so a parent that hides a concept does not
        # actually supply it downstream — don't let it shadow another
        # parent that exposes the same concept publicly.
        hidden = set(v.hidden_concepts) if isinstance(v, QueryDatasource) else set()
        unique_outputs = [
            # the concept may be a in a different environment for a rowset.
            (environment.concepts.get(x.address) or x).address
            for x in v.output_concepts
            if x not in v.partial_concepts and x.address not in hidden
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
                and not _has_applied_condition(merged[k2])
                and not _has_applied_condition(merged[k1])
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
        conditions: BoolExpr | None = None,
        preexisting_conditions: BoolExpr | None = None,
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
            if any(
                other.identifier != dataset.identifier and _has_applied_condition(other)
                for other in final_datasets
            ):
                continue
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

        # Accumulate grain components from non-existence sources directly; we
        # rebuild via from_concepts below, which drops where_clauses anyway,
        # so a per-source BuildGrain accumulator would be wasted work.
        raw_pregrain_components: set[str] = set()
        for source in final_datasets:
            if all(
                [x.address in self.existence_concepts for x in source.output_concepts]
            ):
                logger.debug(
                    f"{self.logging_prefix}{LOGGER_PREFIX} skipping existence-only source {source.identifier}"
                )
                continue
            raw_pregrain_components.update(source.grain.components)
            logger.debug(
                f"{self.logging_prefix}{LOGGER_PREFIX} added grain {source.grain} from {source.identifier}; pregrain components now {raw_pregrain_components}"
            )

        raw_pregrain = BuildGrain.from_concepts(
            raw_pregrain_components, environment=self.environment
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
            downgrade_join_for_condition(join, self.conditions, final_datasets)
        # A query-level filter (e.g. a HAVING like ``customer_state > scaled``)
        # is sometimes pushed into the single branch that exposes its columns
        # rather than kept on this merge. It still constrains the FINAL output,
        # so honor it here: any output concept it proves non-null must not be
        # re-nulled by an outer join. Only for inferred-join merges — a
        # MULTISELECT align supplies explicit ``node_joins`` whose FULL is
        # intentional (each arm's rows survive even where the other arm, with
        # its own HAVING, has none), so an arm-local proof must not force INNER.
        if self.node_joins is None:
            output_addresses = {c.address for c in self.output_concepts}
            branch_proofs: set[str] = set()
            for source in final_datasets:
                for condition in _collect_applied_conditions(source):
                    branch_proofs |= non_null_proofs(condition)
            branch_proofs &= output_addresses
            # A branch-local filter proves its column non-null, but that only
            # constrains the FINAL output when no branch supplies the column
            # completely. When one branch has it COMPLETE (non-partial) and
            # another has it PARTIAL — e.g. a rowset's `where order_id...` over
            # its base key outer-joined back to the unfiltered base — the merge
            # legitimately spans rows outside the filter, so the outer join must
            # keep them: the column is non-null in the filtered branch but not
            # complete there. (A column complete on one branch and merely absent
            # from the rest, e.g. q81's dimension `...address.state`, still
            # forces INNER — it isn't partial anywhere.)
            complete_addresses: set[str] = set()
            partial_addresses: set[str] = set()
            for source in final_datasets:
                source_partial = _collect_deep_partial_addresses(source)
                source_outputs = {c.address for c in source.output_concepts}
                complete_addresses |= source_outputs - source_partial
                partial_addresses |= source_outputs & source_partial
            branch_proofs -= complete_addresses & partial_addresses
            if branch_proofs:
                for join in joins:
                    downgrade_join_for_proofs(join, branch_proofs, final_datasets)
        # Compute per-datasource NULL-ability based on the resolved join graph.
        # Used to (a) order ``final_datasets`` so the preserved side wins
        # ``resolve_concept_map``'s first-pass for shared concepts and
        # (b) prune NULL-able-side ``ConceptPair`` entries from JOIN ON when
        # a preserved alternative exists. Both reduce redundant ``coalesce``.
        null_status = compute_outer_null_status(joins)
        prune_outer_join_pairs(joins, null_status)
        # ``full_join_concepts`` covers FULL JOINs only — both sides may be
        # NULL, so source_map needs every input that supplies the address. For
        # LEFT/RIGHT OUTER the preserved-side ordering above is sufficient.
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
        logger.debug(
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

        # Preserved sides first — first-wins inside ``resolve_concept_map``
        # naturally picks the always-non-NULL source when multiple datasources
        # supply the same concept.
        ordered_datasets = sorted(
            final_datasets,
            key=lambda ds: (null_status.get(ds.identifier, 0), ds.identifier),
        )
        source_map = resolve_concept_map(
            ordered_datasets,
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
            existence_source_map=resolve_existence_map(
                final_datasets, self.existence_concepts
            ),
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
        return type(self)(
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


class MultiSelectMergeNode(MergeNode):
    """The outer FULL JOIN of a multiselect's aligned arms.

    A distinct type so the regroup pass (``group_if_required_v2``) can recognize
    it unambiguously: this node is always already at the align-key grain and must
    never be regrouped, even when hidden derive-arg columns inflate the joined
    pregrain past that grain (forcing a GROUP BY would omit the raw aggregate
    projections and produce invalid SQL). Inherits all behavior — the subclass
    is purely a marker — and ``copy()`` preserves it via ``type(self)``.
    """
