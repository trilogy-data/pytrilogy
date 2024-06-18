from typing import List, Optional

from preql.constants import logger
from preql.core.models import (
    Grain,
    QueryDatasource,
    SourceType,
    Concept,
    Environment,
    LooseConceptList,
)
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
        parent_sources: list[QueryDatasource] = [p.resolve() for p in self.parents]

        grain = concept_list_to_grain(self.output_concepts, [])
        comp_grain = Grain()
        for source in parent_sources:
            comp_grain += source.grain

        # dynamically select if we need to group
        # because sometimes, we are already at required grain
        if comp_grain == grain and self.output_lcl == self.input_lcl:
            # if there is no group by, and inputs equal outputs
            # return the parent
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} Output of group by node equals input of group by node"
                f" {self.output_lcl}"
                f" grains {comp_grain} and {grain}"
            )
            if (
                len(parent_sources) == 1
                and LooseConceptList(concepts=parent_sources[0].output_concepts)
                == self.output_lcl
            ):
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} No group by required, returning parent node"
                )
                return parent_sources[0]
            # otherwise if no group by, just treat it as a select
            source_type = SourceType.SELECT
        else:

            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} Group node has different output than input, forcing group"
                f" {self.input_lcl}"
                " vs"
                f" {self.output_lcl}"
                " and"
                f" upstream grains {[str(source.grain) for source in parent_sources]}"
                " vs"
                f" target grain {grain}"
            )
            for parent in parent_sources:
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} Parent node"
                    f" {[c.address for c in parent.output_concepts]}"
                    " grain"
                    f" {parent.grain}"
                )
            source_type = SourceType.GROUP
        return QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=self.output_concepts,
            datasources=parent_sources,
            source_type=source_type,
            source_map=resolve_concept_map(
                parent_sources,
                targets=self.output_concepts,
                inherited_inputs=self.input_concepts,
            ),
            joins=[],
            grain=grain,
            partial_concepts=self.partial_concepts,
            condition=self.conditions,
        )
