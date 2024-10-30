from typing import List, Optional, Tuple


from trilogy.constants import logger
from trilogy.core.models import (
    BaseJoin,
    Grain,
    JoinType,
    QueryDatasource,
    Datasource,
    SourceType,
    Concept,
    UnnestJoin,
    Conditional,
    Comparison,
    Parenthetical,
    Environment,
)
from trilogy.utility import unique
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    NodeJoin,
)
from trilogy.core.processing.utility import get_node_joins, find_nullable_concepts

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"


def deduplicate_nodes(
    merged: dict[str, QueryDatasource | Datasource],
    logging_prefix: str,
    environment: Environment,
) -> tuple[bool, dict[str, QueryDatasource | Datasource], set[str]]:
    duplicates = False
    removed: set[str] = set()
    set_map: dict[str, set[str]] = {}
    for k, v in merged.items():
        unique_outputs = [
            environment.concepts[x.address].address
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
                    f"{logging_prefix}{LOGGER_PREFIX} extraneous parent node that is subset of another parent node {og.grain.issubset(subset_to.grain)} {og.grain.set} {subset_to.grain.set}"
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
    merged: dict[str, QueryDatasource | Datasource],
    logging_prefix: str,
    environment: Environment,
) -> Tuple[List[NodeJoin] | None, dict[str, QueryDatasource | Datasource]]:
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
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        node_joins: List[NodeJoin] | None = None,
        join_concepts: Optional[List] = None,
        force_join_type: Optional[JoinType] = None,
        partial_concepts: Optional[List[Concept]] = None,
        nullable_concepts: Optional[List[Concept]] = None,
        force_group: bool | None = None,
        depth: int = 0,
        grain: Grain | None = None,
        conditions: Conditional | Comparison | Parenthetical | None = None,
        preexisting_conditions: Conditional | Comparison | Parenthetical | None = None,
        hidden_concepts: List[Concept] | None = None,
        virtual_output_concepts: List[Concept] | None = None,
        existence_concepts: List[Concept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            grain=grain,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            virtual_output_concepts=virtual_output_concepts,
            existence_concepts=existence_concepts,
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
                )
            )
        return joins

    def create_full_joins(self, dataset_list: List[QueryDatasource | Datasource]):
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
        pregrain: Grain,
        grain: Grain,
        environment: Environment,
    ) -> List[BaseJoin | UnnestJoin]:
        # only finally, join between them for unique values
        dataset_list: List[QueryDatasource | Datasource] = sorted(
            final_datasets, key=lambda x: -len(x.grain.components_copy)
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

        for join in joins:
            logger.info(f"{self.logging_prefix}{LOGGER_PREFIX} final join {str(join)}")
        return joins

    def _resolve(self) -> QueryDatasource:
        parent_sources: List[QueryDatasource | Datasource] = [
            p.resolve() for p in self.parents
        ]
        merged: dict[str, QueryDatasource | Datasource] = {}
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
        final_datasets: List[QueryDatasource | Datasource] = list(merged.values())

        existence_final = [
            x
            for x in final_datasets
            if all([y in self.existence_concepts for y in x.output_concepts])
        ]
        if len(merged.keys()) == 1:
            final: QueryDatasource | Datasource = list(merged.values())[0]
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
                return dataset

        pregrain = Grain()
        for source in final_datasets:
            pregrain += source.grain

        grain = self.grain if self.grain else pregrain
        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} has pre grain {pregrain} and final merge node grain {grain}"
        )
        join_candidates = [x for x in final_datasets if x not in existence_final]
        if len(join_candidates) > 1:
            joins: List[BaseJoin | UnnestJoin] = self.generate_joins(
                join_candidates, final_joins, pregrain, grain, self.environment
            )
        else:
            joins = []
        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} Final join count for CTE parent count {len(join_candidates)} is {len(joins)}"
        )
        full_join_concepts = []
        for join in joins:
            if isinstance(join, BaseJoin) and join.join_type == JoinType.FULL:
                full_join_concepts += join.input_concepts
        if self.whole_grain:
            force_group = False
        elif self.force_group is False:
            force_group = False
        elif not any(
            [d.grain.issubset(grain) for d in final_datasets]
        ) and not pregrain.issubset(grain):
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} no parents include full grain {grain} and pregrain {pregrain} does not match, assume must group to grain. Have {[str(d.grain) for d in final_datasets]}"
            )
            force_group = True
        else:
            force_group = None

        qd_joins: List[BaseJoin | UnnestJoin] = [*joins]
        source_map = resolve_concept_map(
            list(merged.values()),
            targets=self.output_concepts,
            inherited_inputs=self.input_concepts + self.existence_concepts,
            full_joins=full_join_concepts,
        )
        nullable_concepts = find_nullable_concepts(
            source_map=source_map, joins=joins, datasources=final_datasets
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
            force_group=force_group,
            condition=self.conditions,
            hidden_concepts=self.hidden_concepts,
        )
        return qds

    def copy(self) -> "MergeNode":
        return MergeNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            g=self.g,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            force_group=self.force_group,
            grain=self.grain,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            nullable_concepts=list(self.nullable_concepts),
            hidden_concepts=list(self.hidden_concepts),
            virtual_output_concepts=list(self.virtual_output_concepts),
            node_joins=list(self.node_joins) if self.node_joins else None,
            join_concepts=list(self.join_concepts) if self.join_concepts else None,
            force_join_type=self.force_join_type,
            existence_concepts=list(self.existence_concepts),
        )
