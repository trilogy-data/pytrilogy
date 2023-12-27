from typing import List, Optional

from preql.constants import logger
from preql.core.models import Grain, QueryDatasource, SourceType, Concept, Environment
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
        output_concepts: List[Concept],
        input_concepts: List[Concept],
        environment: Environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: Optional[List[Concept]] = None,
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

    def _resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]

        grain = concept_list_to_grain(self.output_concepts, [])
        comp_grain = Grain()
        for source in parent_sources:
            comp_grain += source.grain

        # dynamically select if we need to group
        # because sometimes, we are already at required grain
        if comp_grain == grain and set(
            [c.address for c in self.output_concepts]
        ) == set([c.address for c in self.input_concepts]):
            # if there is no group by, and inputs equal outputs
            # return the parent
            logger.info(
                f"{LOGGER_PREFIX} Output of group by node equals input of group by node"
                f" {[c.address for c in self.output_concepts]}"
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
            input_concepts=unique(self.input_concepts, "address"),
            output_concepts=unique(self.output_concepts, "address"),
            datasources=parent_sources,
            source_type=source_type,
            source_map=resolve_concept_map(
                parent_sources,
                targets=unique(self.output_concepts, "address"),
                inherited_inputs=unique(self.input_concepts, "address"),
            ),
            joins=[],
            grain=grain,
            partial_concepts=self.partial_concepts,
        )
