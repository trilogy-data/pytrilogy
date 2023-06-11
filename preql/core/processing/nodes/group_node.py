from typing import List


from preql.constants import logger
from preql.core.models import Grain, QueryDatasource, SourceType
from preql.utility import unique
from preql.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    concept_list_to_grain,
)


LOGGER_PREFIX = "[CONCEPT DETAIL - GROUP NODE]"


class GroupNode(StrategyNode):
    source_type = SourceType.GROUP

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )

    def _resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        # a group by node only outputs the actual keys grouped by
        outputs = self.all_concepts
        grain = concept_list_to_grain(outputs, [])
        comp_grain = Grain()
        for source in parent_sources:
            comp_grain += source.grain

        # dynamically select if we need to group
        # because sometimes, we are already at required grain
        if comp_grain == grain and set([c.address for c in outputs]) == set(
            [c.address for c in input_concepts]
        ):
            # if there is no group by, and inputs equal outputs
            # return the parent
            logger.info(
                f"{LOGGER_PREFIX} Output of group by node equals input of group by node"
                f" {[c.address for c in outputs]}"
            )
            if len(parent_sources) == 1:
                logger.info(
                    f"{LOGGER_PREFIX} No group by required, returning parent node"
                )
                return parent_sources[0]
            # otherwise if no group by, just treat it as a select
            source_type = SourceType.SELECT
        else:
            source_type = SourceType.GROUP
        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=unique(outputs, "address"),
            datasources=parent_sources,
            source_type=source_type,
            source_map=resolve_concept_map(parent_sources),
            joins=[],
            grain=grain,
        )
