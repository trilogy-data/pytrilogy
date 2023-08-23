from typing import List, Optional


from preql.constants import logger
from preql.core.models import (
    BaseJoin,
    Grain,
    JoinType,
    QueryDatasource,
    SourceType,
)
from preql.utility import unique
from preql.core.processing.nodes.base_node import StrategyNode, resolve_concept_map

LOGGER_PREFIX = "[CONCEPT DETAIL - MERGE NODE]"


class MergeNode(StrategyNode):
    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        join_concepts: Optional[List] = None,
        force_join_type: Optional[JoinType] = None,
        partial_concepts: Optional[List] = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
            partial_concepts=partial_concepts,
        )
        self.join_concepts = join_concepts
        self.force_join_type = force_join_type

    def _resolve(self):
        parent_sources = [p.resolve() for p in self.parents]
        merged = {}
        for source in parent_sources:
            if source.full_name in merged:
                merged[source.full_name] = merged[source.full_name] + source
            else:
                merged[source.full_name] = source
        # early exit if we can just return the parent
        final_datasets = list(merged.values())
        if len(merged.keys()) == 1:
            final = list(merged.values())[0]
            if set([c.address for c in final.output_concepts]) == set(
                [c.address for c in self.all_concepts]
            ):
                logger.info(
                    f"{LOGGER_PREFIX} Merge node has only one parent with the same"
                    " outputs as this merge node, dropping merge node "
                )
                return final
        # if we have multiple candidates, see if one is good enough
        for dataset in final_datasets:
            output_set = set([c.address for c in dataset.output_concepts])
            if all([c.address in output_set for c in self.all_concepts]):
                return dataset

        grain = Grain()
        for source in final_datasets:
            grain += source.grain
        # only finally, join between them for unique values
        dataset_list = sorted(
            final_datasets, key=lambda x: -len(x.grain.components_copy)
        )
        if not dataset_list:
            raise SyntaxError("Empty merge node")
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
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        outputs = unique(self.mandatory_concepts + self.optional_concepts, "address")

        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=outputs,
            datasources=final_datasets,
            source_type=self.source_type,
            source_map=resolve_concept_map(parent_sources),
            joins=joins,
            grain=grain,
        )
