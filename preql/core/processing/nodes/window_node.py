from typing import List


from preql.core.models import SourceType, Concept, Grain
from preql.core.processing.nodes.base_node import StrategyNode, QueryDatasource


class WindowNode(StrategyNode):
    source_type = SourceType.WINDOW

    def __init__(
        self,
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

    def _resolve(self) -> QueryDatasource:
        base = super()._resolve()
        base.grain = Grain(components=self.input_concepts)
        return base
