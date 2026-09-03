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

    inherits_parent_grain = True

    def __init__(
        self,
        input_concepts: list[BuildConcept],
        output_concepts: list[BuildConcept],
        environment: BuildEnvironment,
        datasource: BuildDatasource | None = None,
        parents: list["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: list[BuildConcept] | None = None,
        rollup_concepts: list[BuildConcept] | None = None,
        nullable_concepts: list[BuildConcept] | None = None,
        grain: BuildGrain | None = None,
        force_group: bool | None = False,
        conditions: BoolExpr | None = None,
        preexisting_conditions: BoolExpr | None = None,
        hidden_concepts: set[str] | None = None,
        ordering: BuildOrderBy | None = None,
        existence_concepts: list[BuildConcept] | None = None,
    ):
        if datasource and partial_concepts is None:
            partial_concepts = datasource.partial_concepts
        if datasource and nullable_concepts is None:
            nullable_concepts = datasource.nullable_concepts
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
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
        self.datasource = datasource

    def validate_inputs(self):
        # a select node is a root; nothing to validate against
        return

    def resolve_from_provided_datasource(
        self,
    ) -> QueryDatasource:
        if not self.datasource:
            raise ValueError("Datasource not provided")
        datasource: BuildDatasource = self.datasource

        all_concepts_final: list[BuildConcept] = unique(self.all_concepts, "address")
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
        # Outputs resolved at render rather than read off a column get an empty
        # entry: mapped for `validate_missing` without naming a source.
        for x in all_concepts_final:
            if x.address not in source_map and x.derivation in (
                Derivation.MULTISELECT,
                Derivation.TVF_UNION,
                Derivation.FILTER,
                Derivation.BASIC,
                Derivation.ROWSET,
                Derivation.UNION,
                Derivation.CONSTANT,
            ):
                source_map[x.address] = set()

        # when not grouping, the scan keeps the datasource grain so merges align
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
            # node-level stamps can mark a partial binding (a licensed rowset
            # handle widened onto this scan) the datasource columns cannot express
            partial_concepts=unique(
                [c.concept for c in datasource.columns if not c.is_complete]
                + list(self.partial_concepts),
                "address",
            ),
            rollup_concepts=self.rollup_concepts,
            # node-level stamps carry a BASIC computed at this scan over a
            # nullable column, which is not itself a datasource column
            nullable_concepts=unique(
                [c.concept for c in datasource.columns if c.is_nullable]
                + list(self.nullable_concepts),
                "address",
            ),
            source_type=SourceType.DIRECT_SELECT,
            condition=self.conditions,
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
        # A constant-LHS membership has no row source but still checks its set
        # via an existence subquery; carry the existence parents' source map.
        if self.parents and self.existence_concepts:
            parent_sources: list[QueryDatasource | BuildDatasource] = [
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
        resolution: QueryDatasource | None = None
        if all(
            (
                c.derivation == Derivation.CONSTANT
                or (
                    c.purpose == Purpose.CONSTANT
                    and c.derivation == Derivation.MULTISELECT
                )
            )
            for c in self.all_concepts
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
            parent_sources: list[QueryDatasource | BuildDatasource] = [
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
        node = type(self)(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            datasource=self.datasource,
            depth=self.depth,
            parents=self.parents,
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            nullable_concepts=list(self.nullable_concepts),
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

    Re-exposes the body's rowset-local concepts under their outer rowset
    addresses. A distinct type so the regroup pass never regroups it: the
    wrapper is a 1:1 projection of an already-final body, and a forced GROUP BY
    would dedup rows or omit raw projections.
    """


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
