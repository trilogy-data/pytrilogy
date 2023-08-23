from typing import List


from preql.core.models import QueryDatasource, SourceType, Concept
from preql.core.processing.nodes.base_node import StrategyNode


class FilterNode(StrategyNode):
    source_type = SourceType.FILTER

    def __init__(
        self,
        mandatory_concepts: List[Concept],
        optional_concepts: List[Concept],
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
        """We need to ensure that any filtered values are removed from the output to avoid inappropriate references"""
        base = super()._resolve()
        # [c for c in self.mandatory_concepts if isinstance(c.lineage, FilterItem)]
        base.source_map = {key: value for key, value in base.source_map.items()}
        return base
