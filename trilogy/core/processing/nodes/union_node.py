from trilogy.core.enums import SetOperator, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildGrain,
)
from trilogy.core.processing.nodes.base_node import StrategyNode


class UnionNode(StrategyNode):
    """Union nodes represent combining two keyspaces. ``set_operator`` picks
    the SQL combinator (UNION ALL / EXCEPT / INTERSECT); for EXCEPT the parent
    order is semantic (left-fold), preserved through the QueryDatasource."""

    source_type = SourceType.UNION

    def __init__(
        self,
        input_concepts: list[BuildConcept],
        output_concepts: list[BuildConcept],
        environment,
        parents: list["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: list[BuildConcept] | None = None,
        preexisting_conditions: BoolExpr | None = None,
        grain: BuildGrain | None = None,
        set_operator: SetOperator = SetOperator.UNION_ALL,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            preexisting_conditions=preexisting_conditions,
            # A union's grain is always its stacked output columns.
            grain=grain
            or BuildGrain.from_concepts(output_concepts, environment=environment),
        )
        # partial_concepts carries only intrinsic column-level partials (``~col``
        # inside a ``partial datasource``); those survive a covering UNION.
        self.set_operator = set_operator

    def add_output_concepts(self, concepts, rebuild=True, unhide=True):
        for x in self.parents:
            x.add_output_concepts(concepts, rebuild, unhide)
        super().add_output_concepts(concepts, rebuild, unhide)

    def copy(self) -> "UnionNode":
        return UnionNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            parents=[x.copy() for x in self.parents] if self.parents else None,
            depth=self.depth,
            partial_concepts=self.partial_concepts,
            preexisting_conditions=self.preexisting_conditions,
            grain=self.grain,
            set_operator=self.set_operator,
        )
