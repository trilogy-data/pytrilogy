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
from preql.core.enums import Purpose
from preql.utility import unique
from preql.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    NodeJoin,
)

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"


class MergeNode(StrategyNode):
    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts: List[Concept],
        optional_concepts: List[Concept],
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
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
            partial_concepts=partial_concepts,
            depth=depth,
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

    def create_inferred_joins(self, dataset_list: List[QueryDatasource], grain: Grain):
        base = dataset_list[0]
        joins = []
        all_concepts = unique(
            self.mandatory_concepts + self.optional_concepts, "address"
        )

        join_concepts = self.join_concepts or all_concepts

        if dataset_list[1:]:
            for right_value in dataset_list[1:]:
                if not grain.components:
                    joins.append(
                        BaseJoin(
                            left_datasource=base,
                            right_datasource=right_value,
                            join_type=self.force_join_type
                            if self.force_join_type
                            else JoinType.FULL,
                            concepts=[],
                        )
                    )

                if all(
                    [c.purpose == Purpose.CONSTANT for c in right_value.output_concepts]
                ):
                    joins.append(
                        BaseJoin(
                            left_datasource=base,
                            right_datasource=right_value,
                            join_type=self.force_join_type
                            if self.force_join_type
                            else JoinType.FULL,
                            concepts=[],
                        )
                    )
                    continue
                joins.append(
                    BaseJoin(
                        left_datasource=base,
                        right_datasource=right_value,
                        join_type=self.force_join_type
                        if self.force_join_type
                        else JoinType.LEFT_OUTER,
                        concepts=join_concepts,
                        filter_to_mutual=True,
                    )
                )
        return joins

    def _resolve(self):
        parent_sources = [p.resolve() for p in self.parents]
        merged = {}
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
                [c.address for c in self.all_concepts]
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node has only one parent with the same"
                    " outputs as this merge node, dropping merge node "
                )
                return final
        # if we have multiple candidates, see if one is good enough
        for dataset in final_datasets:
            output_set = set([c.address for c in dataset.output_concepts])
            if all([c.address in output_set for c in self.all_concepts]):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Merge node not required as one"
                    " parent node has all required output properties"
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
            joins = self.create_inferred_joins(dataset_list, grain)
        else:
            joins = self.translate_node_joins(self.node_joins)
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        outputs = unique(self.mandatory_concepts + self.optional_concepts, "address")

        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=outputs,
            datasources=final_datasets,
            source_type=self.source_type,
            source_map=resolve_concept_map(parent_sources, outputs),
            joins=joins,
            grain=grain,
            partial_concepts=self.partial_concepts,
        )
