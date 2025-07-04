from typing import List

from trilogy.core.enums import SourceType
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.nodes.base_node import StrategyNode


class UnionNode(StrategyNode):
    """Union nodes represent combining two keyspaces"""

    source_type = SourceType.UNION

    def __init__(
        self,
        input_concepts: List[BuildConcept],
        output_concepts: List[BuildConcept],
        environment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: List[BuildConcept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
        )
        if self.partial_concepts != []:
            raise ValueError(
                f"UnionNode should not have partial concepts, has {self.partial_concepts}, was given {partial_concepts}"
            )
        self.partial_concepts = []

    def _resolve(self) -> QueryDatasource:
        """We need to ensure that any filtered values are removed from the output to avoid inappropriate references"""
        base = super()._resolve()
        return base

    def copy(self) -> "UnionNode":
        return UnionNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=[x.copy() for x in self.parents] if self.parents else None,
            depth=self.depth,
            partial_concepts=self.partial_concepts,
        )
