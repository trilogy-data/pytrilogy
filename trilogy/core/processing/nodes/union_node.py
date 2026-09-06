from trilogy.core.enums import SetOperator, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.nodes.base_node import StrategyNode


def column_level_partial_addresses(ds: BuildDatasource | QueryDatasource) -> set[str]:
    """Column-level (``~col``) partial addresses of the leaf tables under ``ds``."""
    if isinstance(ds, BuildDatasource):
        return set(ds.column_level_partial_addresses)
    out: set[str] = set()
    for sub in ds.datasources:
        out |= column_level_partial_addresses(sub)
    return out


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
        hidden_concepts: set[str] | None = None,
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
            hidden_concepts=hidden_concepts,
        )
        # partial_concepts carries only intrinsic column-level partials (``~col``
        # inside a ``partial datasource``); those survive a covering UNION.
        self.set_operator = set_operator

    def _resolve(self) -> QueryDatasource:
        qds = super()._resolve()
        # A covering UNION completes its arms' table-level partiality; only a
        # column-level `~` binding survives it.
        column_level: set[str] = set()
        for arm in qds.datasources:
            column_level |= column_level_partial_addresses(arm)
        stamped = {c.address for c in self.partial_concepts}
        qds.partial_concepts = [
            c
            for c in qds.partial_concepts
            if c.address in stamped or c.address in column_level
        ]
        return qds

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
            hidden_concepts=set(self.hidden_concepts),
        )
