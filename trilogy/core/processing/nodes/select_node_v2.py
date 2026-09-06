from trilogy.constants import logger
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import Derivation, Purpose, SourceType
from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildOrderBy,
    CanonicalBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import condition_proves_non_null
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    resolve_existence_map,
)
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


def scan_stamps(
    datasource: BuildDatasource,
    outputs: list[BuildConcept],
    partial_is_full: bool,
    complete_proofs: set[str],
    non_null_proofs: set[str],
) -> tuple[list[BuildConcept], list[BuildConcept]]:
    """Partial and nullable outputs of a scan: the datasource's column flags
    over the projected outputs, narrowed by the scan's proofs. An address also
    bound complete on the same datasource is fully providable, and a BASIC
    computed here over a nullable column is NULL wherever that column is."""
    complete = {c.concept.address for c in datasource.columns if c.is_complete}
    partial_lcl = CanonicalBuildConceptList(
        concepts=[
            c.concept
            for c in datasource.columns
            if not c.is_complete and c.concept.address not in complete
        ]
    )
    nullable_lcl = CanonicalBuildConceptList(
        concepts=[c.concept for c in datasource.columns if c.is_nullable]
    )
    partials = (
        []
        if partial_is_full
        else [
            c
            for c in outputs
            if c in partial_lcl and c.canonical_address not in complete_proofs
        ]
    )
    nullables = [
        c
        for c in outputs
        if (
            c in nullable_lcl
            or (
                propagates_argument_nulls(c)
                and any(arg in nullable_lcl for arg in c.concept_arguments)
            )
        )
        and not non_null_proofs.intersection(
            {c.address, c.canonical_address, *c.pseudonyms}
        )
    ]
    return partials, nullables


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
        partial_is_full: bool = False,
        complete_proofs: set[str] | None = None,
        non_null_proofs: set[str] | None = None,
    ):
        if datasource and partial_concepts is None:
            partial_concepts = datasource.partial_concepts
        if datasource and nullable_concepts is None:
            nullable_concepts = datasource.nullable_concepts
        # Proofs the scan's construction established; the stamps are recomputed
        # from them at resolve so a widened projection is stamped the same way.
        self.partial_is_full = partial_is_full
        self.complete_proofs = complete_proofs or set()
        self.non_null_proofs = non_null_proofs or set()
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

        non_null_proofs = set(self.non_null_proofs)
        if self.conditions:
            non_null_proofs |= condition_proves_non_null(self.conditions)
        partials, nullables = scan_stamps(
            datasource,
            all_concepts_final,
            self.partial_is_full,
            self.complete_proofs,
            non_null_proofs,
        )
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
            # the node stamp adds what columns cannot express (a licensed
            # rowset handle widened onto this scan)
            partial_concepts=unique(partials + list(self.partial_concepts), "address"),
            rollup_concepts=self.rollup_concepts,
            nullable_concepts=unique(
                nullables
                + [
                    c
                    for c in self.nullable_concepts
                    if not non_null_proofs.intersection(
                        {c.address, c.canonical_address, *c.pseudonyms}
                    )
                ],
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
            partial_is_full=self.partial_is_full,
            complete_proofs=set(self.complete_proofs),
            non_null_proofs=set(self.non_null_proofs),
        )
        node.limit = self.limit
        return node


class RowsetNode(SelectNode):
    """The boundary projection over a rowset body: re-exposes the body's
    columns under the outer rowset handle addresses, 1:1 with the body's
    rows. A distinct type so the boundary is recognizable by `isinstance`;
    it adds no behavior of its own (a merge above it keeps the body's rows
    through the rowset-output check in `MergeNode._resolve`)."""


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
