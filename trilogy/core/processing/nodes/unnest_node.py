from typing import List


from trilogy.core.models import (
    QueryDatasource,
    SourceType,
    Concept,
    UnnestJoin,
    Function,
)
from trilogy.core.processing.nodes.base_node import StrategyNode


class UnnestNode(StrategyNode):
    """Unnest nodes represent an expansion of an array or other
    column into rows.
    """

    source_type = SourceType.UNNEST

    def __init__(
        self,
        unnest_concepts: List[Concept],
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
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
        )
        self.unnest_concepts = unnest_concepts

    def _resolve(self) -> QueryDatasource:
        """We need to ensure that any filtered values are removed from the output to avoid inappropriate references"""
        base = super()._resolve()
        lineage = self.unnest_concepts[0].lineage
        assert isinstance(lineage, Function)
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
            g=self.g,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
        )
