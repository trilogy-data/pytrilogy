from typing import List


from preql.core.models import QueryDatasource, SourceType, Concept, Grain, BaseJoin, UnnestJoin
from preql.core.processing.nodes.base_node import StrategyNode
from preql.core.enums import JoinType

class UnnestNode(StrategyNode):
    """Unnest nodes represent an expansion of an array or other
    column into rows.
    """

    source_type = SourceType.UNNEST

    def __init__(
        self,
        unnest_concept:Concept,
        optional_concepts: List[Concept],
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
    ):
        super().__init__(
            [unnest_concept],
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
        )
        self.unnest_concept = unnest_concept

    def _resolve(self) -> QueryDatasource:
        """We need to ensure that any filtered values are removed from the output to avoid inappropriate references"""
        base = super()._resolve()

        unnest = UnnestJoin(concept=self.unnest_concept, alias=f'unnest_{self.unnest_concept.address.replace(".", "_ ")}')
        base.joins.append(unnest)
            
        # raise SyntaxError(base)
        base.source_map[self.unnest_concept.address] = unnest
        return base
