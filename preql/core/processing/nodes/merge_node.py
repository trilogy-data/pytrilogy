from typing import List, Optional


from preql.constants import logger
from preql.core.models import (
    BaseJoin,
    Grain,
    JoinType,
    QueryDatasource,
    SourceType,
    Concept,
)
from preql.utility import unique
from preql.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    NodeJoin,
)
from preql.core.processing.utility import get_node_joins

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"


class MergeNode(StrategyNode):
    source_type = SourceType.SELECT

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
        depth: int = 0,
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
        )
        self.join_concepts = join_concepts
        self.force_join_type = force_join_type
        self.node_joins = node_joins

    def translate_node_joins(self, node_joins: List[NodeJoin]) -> List[BaseJoin]:
        joins = []
        for join in node_joins:
            joins.append(
                BaseJoin(
                    left_datasource=join.left_node.resolve(),
                    right_datasource=join.right_node.resolve(),
                    join_type=join.join_type,
                    concepts=join.concepts,
                )
            )
        return joins

    def create_full_joins(self, dataset_list: List[QueryDatasource]):
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

    def _resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]
        merged: dict[str, QueryDatasource] = {}
        for source in parent_sources:
            if source.full_name in merged:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} merging two nodes with {source.full_name}"
                )
                merged[source.full_name] = merged[source.full_name] + source
            else:
                merged[source.full_name] = source
        # early exit if we can just return the parent
        final_datasets: List[QueryDatasource] = list(merged.values())
        if len(merged.keys()) == 1:
            final: QueryDatasource = list(merged.values())[0]
            if set([c.address for c in final.output_concepts]) == set(
                [c.address for c in self.output_concepts]
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
                    if c not in dataset.partial_concepts
                ]
            )
            if all([c.address in output_set for c in self.all_concepts]):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node not required as parent node {dataset.identifier} {dataset.source_type}"
                    f"has all required output properties with partial {[c.address for c in dataset.partial_concepts]}"
                )
                return dataset

        grain = Grain()
        for source in final_datasets:
            grain += source.grain
        # only finally, join between them for unique values
        dataset_list: List[QueryDatasource] = sorted(
            final_datasets, key=lambda x: -len(x.grain.components_copy)
        )
        if not dataset_list:
            raise SyntaxError("Empty merge node")
        logger.info(
            f"{self.logging_prefix}{LOGGER_PREFIX} Merge node has {len(dataset_list)} parents, starting merge"
        )
        for item in dataset_list:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} potential merge keys {[x.address for x in item.output_concepts]} for {item.full_name}"
            )

        if not self.node_joins:
            if not grain.components:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} no grain components, doing full join"
                )
                joins = self.create_full_joins(dataset_list)
            else:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} inferring node joins"
                )
                joins = get_node_joins(dataset_list)
        else:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} translating provided node joins"
            )
            joins = self.translate_node_joins(self.node_joins)
        return QueryDatasource(
            input_concepts=unique(self.input_concepts, "address"),
            output_concepts=unique(self.output_concepts, "address"),
            datasources=final_datasets,
            source_type=self.source_type,
            source_map=resolve_concept_map(
                parent_sources, self.output_concepts, self.input_concepts
            ),
            joins=joins,
            grain=grain,
            partial_concepts=self.partial_concepts,
        )
