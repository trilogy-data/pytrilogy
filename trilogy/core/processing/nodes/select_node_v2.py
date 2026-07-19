from typing import List, Optional

from trilogy.constants import logger
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import Derivation, Purpose, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildOrderBy,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    resolve_existence_map,
)
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


class SelectNode(StrategyNode):
    """Select nodes actually fetch raw data from a table
    Responsible for selecting the cheapest option from which to select.
    """

    source_type = SourceType.SELECT

    def __init__(
        self,
        input_concepts: List[BuildConcept],
        output_concepts: List[BuildConcept],
        environment: BuildEnvironment,
        datasource: BuildDatasource | None = None,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: List[BuildConcept] | None = None,
        rollup_concepts: List[BuildConcept] | None = None,
        nullable_concepts: List[BuildConcept] | None = None,
        accept_partial: bool = False,
        grain: Optional[BuildGrain] = None,
        force_group: bool | None = False,
        conditions: BoolExpr | None = None,
        preexisting_conditions: BoolExpr | None = None,
        hidden_concepts: set[str] | None = None,
        ordering: BuildOrderBy | None = None,
        existence_concepts: List[BuildConcept] | None = None,
    ):
        # Derive partial/nullable from datasource columns when not explicitly provided
        if datasource and partial_concepts is None:
            partial_concepts = datasource.partial_concepts
        if datasource and nullable_concepts is None:
            nullable_concepts = datasource.nullable_concepts
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            rollup_concepts=rollup_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            grain=grain,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            ordering=ordering,
            existence_concepts=existence_concepts,
        )
        self.accept_partial = accept_partial
        self.datasource = datasource

    def validate_inputs(self):
        # we do not need to validate inputs for a select node
        # as it will be a root
        return

    def resolve_from_provided_datasource(
        self,
    ) -> QueryDatasource:
        if not self.datasource:
            raise ValueError("Datasource not provided")
        datasource: BuildDatasource = self.datasource

        all_concepts_final: List[BuildConcept] = unique(self.all_concepts, "address")
        source_map: dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]] = {
            concept.address: {datasource} for concept in self.input_concepts
        }

        derived_concepts = [
            c
            for c in datasource.columns
            if isinstance(c.alias, BuildFunction) and c.concept.address in source_map
        ]
        for c in derived_concepts:
            if not isinstance(c.alias, BuildFunction):
                continue
            for x in c.alias.concept_arguments:
                source_map[x.address] = {datasource}
        for x in all_concepts_final:
            if x.address not in source_map and x.derivation in (
                Derivation.MULTISELECT,
                Derivation.FILTER,
                Derivation.BASIC,
                Derivation.ROWSET,
                Derivation.BASIC,
                Derivation.UNION,
                Derivation.CONSTANT,
            ):
                source_map[x.address] = set()

        # if we're not grouping
        # force grain to datasource grain
        # so that we merge on the same grain
        if self.force_group is False:
            grain = self.grain if self.grain else datasource.grain
        else:
            grain = self.grain or BuildGrain()
        return QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=all_concepts_final,
            source_map=source_map,
            datasources=[datasource],
            grain=grain,
            joins=[],
            partial_concepts=[
                c.concept for c in datasource.columns if not c.is_complete
            ],
            rollup_concepts=self.rollup_concepts,
            # union the node-level stamps: a BASIC computed at this scan over a
            # nullable column (`l_key + 1`) is nullable here but is not a
            # datasource column, so the column scan alone under-reports
            nullable_concepts=unique(
                [c.concept for c in datasource.columns if c.is_nullable]
                + list(self.nullable_concepts),
                "address",
            ),
            source_type=SourceType.DIRECT_SELECT,
            # we can skip rendering conditions
            condition=self.conditions,
            # select nodes should never group
            force_group=self.force_group,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
            base_datasource=datasource,
        )

    def resolve_from_constant_datasources(self) -> QueryDatasource:
        datasource = BuildDatasource(
            name=CONSTANT_DATASET, address=CONSTANT_DATASET, columns=[]
        )
        resolution = QueryDatasource(
            input_concepts=[],
            output_concepts=unique(self.all_concepts, "address"),
            source_map={concept.address: set() for concept in self.all_concepts},
            datasources=[datasource],
            grain=datasource.grain,
            condition=self.conditions,
            joins=[],
            partial_concepts=[],
            rollup_concepts=[],
            source_type=SourceType.CONSTANT,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
            base_datasource=datasource,
        )
        # A constant-LHS membership (`(1, 2) in (rs.a, rs.b)`) has no row source
        # but still checks its set via an existence subquery; carry the existence
        # parents' source map through so the membership renders (grain-less form).
        if self.parents and self.existence_concepts:
            parent_sources: List[QueryDatasource | BuildDatasource] = [
                p.resolve() for p in self.parents
            ]
            resolution.datasources += sorted(
                parent_sources, key=lambda ds: ds.identifier
            )
            resolution.existence_source_map.update(
                resolve_existence_map(parent_sources, self.existence_concepts)
            )
        return resolution

    def _resolve(self) -> QueryDatasource:
        # if we have parent nodes, we do not need to go to a datasource
        resolution: QueryDatasource | None = None
        if all(
            [
                (
                    c.derivation == Derivation.CONSTANT
                    or (
                        c.purpose == Purpose.CONSTANT
                        and c.derivation == Derivation.MULTISELECT
                    )
                )
                for c in self.all_concepts
            ]
        ):
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} have a constant datasource"
            )
            resolution = self.resolve_from_constant_datasources()
            return resolution

        if self.datasource and not resolution:
            resolution = self.resolve_from_provided_datasource()

        if self.parents:
            if not resolution:
                return super()._resolve()
            # zip in our parent source map
            parent_sources: List[QueryDatasource | BuildDatasource] = [
                p.resolve() for p in self.parents
            ]

            resolution.datasources += sorted(
                parent_sources, key=lambda ds: ds.identifier
            )

            source_map = resolve_concept_map(
                parent_sources,
                targets=self.output_concepts,
                inherited_inputs=self.input_concepts + self.existence_concepts,
            )
            for k, v in source_map.items():
                if v and k not in resolution.source_map:
                    resolution.source_map[k] = v
            resolution.existence_source_map.update(
                resolve_existence_map(parent_sources, self.existence_concepts)
            )
        if not resolution:
            raise ValueError(f"No select node could be generated for {self}")
        return resolution

    def copy(self) -> "SelectNode":
        node = SelectNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            datasource=self.datasource,
            depth=self.depth,
            parents=self.parents,
            whole_grain=self.whole_grain,
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            nullable_concepts=list(self.nullable_concepts),
            accept_partial=self.accept_partial,
            grain=self.grain,
            force_group=self.force_group,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
            existence_concepts=list(self.existence_concepts),
        )
        node.limit = self.limit
        return node


class RowsetNode(SelectNode):
    """A thin translation projection over a rowset body.

    Re-exposes the body's rowset-local concepts (`local._rs_*`) under their outer
    rowset addresses (`rs.*`). A distinct type so the regroup pass
    (``group_if_required_v2``) recognizes it and never regroups: the wrapper is a
    pure 1:1 projection of an already-final body, so forcing a GROUP BY would dedup
    rows (e.g. collapse a union-stack's duplicates) or omit raw projections.
    """

    def copy(self) -> "RowsetNode":
        node = RowsetNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            datasource=self.datasource,
            depth=self.depth,
            parents=self.parents,
            whole_grain=self.whole_grain,
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            nullable_concepts=list(self.nullable_concepts),
            accept_partial=self.accept_partial,
            grain=self.grain,
            force_group=self.force_group,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
            existence_concepts=list(self.existence_concepts),
        )
        node.limit = self.limit
        return node


class ConstantNode(SelectNode):
    source_type = SourceType.CONSTANT
    """Represents a constant value."""

    def copy(self) -> "ConstantNode":
        return ConstantNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            datasource=self.datasource,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
        )

    def _resolve(self) -> QueryDatasource:
        return self.resolve_from_constant_datasources()
