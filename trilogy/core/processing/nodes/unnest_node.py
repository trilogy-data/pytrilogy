from typing import List

from trilogy.core.enums import SourceType
from trilogy.core.models.build import BuildConcept, BuildFunction
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.nodes.base_node import StrategyNode


class UnnestNode(StrategyNode):
    """Unnest nodes represent an expansion of an array or other
    column into rows.
    """

    source_type = SourceType.UNNEST

    def __init__(
        self,
        unnest_concepts: List[BuildConcept],
        input_concepts: List[BuildConcept],
        output_concepts: List[BuildConcept],
        environment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
        )
        self.unnest_concepts = unnest_concepts

    def _resolve(self) -> QueryDatasource:
        """We need to ensure that any filtered values are removed from the output to avoid inappropriate references"""
        base = super()._resolve()
        lineage = self.unnest_concepts[0].lineage
        assert isinstance(lineage, BuildFunction)
        final = "_".join(set([c.address for c in self.unnest_concepts]))
        unnest = UnnestJoin(
            concepts=self.unnest_concepts,
            parent=lineage,
            alias=f'unnest_{final.replace(".", "_")}',
        )
        base.joins.append(unnest)
        for unnest_concept in self.unnest_concepts:
            base.source_map[unnest_concept.address] = {unnest}
            base.join_derived_concepts = [unnest_concept]
        return base

    def copy(self) -> "UnnestNode":
        return UnnestNode(
            unnest_concepts=self.unnest_concepts,
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
        )
