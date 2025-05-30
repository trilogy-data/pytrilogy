from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Set, Union

from pydantic import (
    BaseModel,
    Field,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    RECURSIVE_GATING_CONCEPT,
    MagicConstants,
    logger,
)
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionType,
    Granularity,
    JoinType,
    Modifier,
    Purpose,
    SourceType,
)
from trilogy.core.models.build import (
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildExpr,
    BuildFunction,
    BuildGrain,
    BuildOrderBy,
    BuildParamaterizedConceptReference,
    BuildParenthetical,
    BuildRowsetItem,
    DataType,
    LooseBuildConceptList,
)
from trilogy.core.models.datasource import Address
from trilogy.core.utility import safe_quote
from trilogy.utility import unique

LOGGER_PREFIX = "[MODELS_EXECUTE]"

DATASOURCE_TYPES = (BuildDatasource, BuildDatasource)


class CTE(BaseModel):
    name: str
    source: "QueryDatasource"
    output_columns: List[BuildConcept]
    source_map: Dict[str, list[str]]
    grain: BuildGrain
    base: bool = False
    group_to_grain: bool = False
    existence_source_map: Dict[str, list[str]] = Field(default_factory=dict)
    parent_ctes: List[Union["CTE", "UnionCTE"]] = Field(default_factory=list)
    joins: List[Union["Join", "InstantiatedUnnestJoin"]] = Field(default_factory=list)
    condition: Optional[
        Union[BuildComparison, BuildConditional, BuildParenthetical]
    ] = None
    partial_concepts: List[BuildConcept] = Field(default_factory=list)
    nullable_concepts: List[BuildConcept] = Field(default_factory=list)
    join_derived_concepts: List[BuildConcept] = Field(default_factory=list)
    hidden_concepts: set[str] = Field(default_factory=set)
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    base_name_override: Optional[str] = None
    base_alias_override: Optional[str] = None

    @field_validator("join_derived_concepts")
    def validate_join_derived_concepts(cls, v):
        if len(v) > 1:
            raise NotImplementedError(
                "Multiple join derived concepts not yet supported."
            )
        return unique(v, "address")

    @property
    def identifier(self):
        return self.name

    @property
    def safe_identifier(self):
        return self.name

    @computed_field  # type: ignore
    @property
    def output_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.output_columns)

    @field_validator("output_columns")
    def validate_output_columns(cls, v):
        return unique(v, "address")

    def inline_constant(self, concept: BuildConcept):
        if not concept.derivation == Derivation.CONSTANT:
            return False
        if not isinstance(concept.lineage, BuildFunction):
            return False
        if not concept.lineage.operator == FunctionType.CONSTANT:
            return False
        # remove the constant
        removed: set = set()
        if concept.address in self.source_map:
            removed = removed.union(self.source_map[concept.address])
            del self.source_map[concept.address]

        if self.condition:
            self.condition = self.condition.inline_constant(concept)
        # if we've entirely removed the need to join to someplace to get the concept
        # drop the join as well.
        for removed_cte in removed:
            still_required = any(
                [
                    removed_cte in x
                    for x in self.source_map.values()
                    or self.existence_source_map.values()
                ]
            )
            if not still_required:
                self.joins = [
                    join
                    for join in self.joins
                    if not isinstance(join, Join)
                    or (
                        isinstance(join, Join)
                        and (
                            join.right_cte.name != removed_cte
                            and any(
                                [
                                    x.cte.name != removed_cte
                                    for x in (join.joinkey_pairs or [])
                                ]
                            )
                        )
                    )
                ]
                for join in self.joins:
                    if isinstance(join, UnnestJoin) and concept in join.concepts:
                        join.rendering_required = False

                self.parent_ctes = [
                    x for x in self.parent_ctes if x.name != removed_cte
                ]
                if removed_cte == self.base_name_override:
                    candidates = [x.name for x in self.parent_ctes]
                    self.base_name_override = candidates[0] if candidates else None
                    self.base_alias_override = candidates[0] if candidates else None
        return True

    @property
    def comment(self) -> str:
        base = f"Target: {str(self.grain)}. Group: {self.group_to_grain}"
        base += f" Source: {self.source.source_type}."
        if self.parent_ctes:
            base += f" References: {', '.join([x.name for x in self.parent_ctes])}."
        if self.joins:
            base += f"\n-- Joins: {', '.join([str(x) for x in self.joins])}."
        if self.partial_concepts:
            base += (
                f"\n-- Partials: {', '.join([str(x) for x in self.partial_concepts])}."
            )
        base += f"\n-- Source Map: {self.source_map}."
        base += f"\n-- Output: {', '.join([str(x) for x in self.output_columns])}."
        if self.source.input_concepts:
            base += f"\n-- Inputs: {', '.join([str(x) for x in self.source.input_concepts])}."
        if self.hidden_concepts:
            base += f"\n-- Hidden: {', '.join([str(x) for x in self.hidden_concepts])}."
        if self.nullable_concepts:
            base += (
                f"\n-- Nullable: {', '.join([str(x) for x in self.nullable_concepts])}."
            )
        base += "\n"
        return base

    def inline_parent_datasource(
        self, parent: "CTE", force_group: bool = False
    ) -> bool:
        qds_being_inlined = parent.source
        ds_being_inlined = qds_being_inlined.datasources[0]
        if not isinstance(ds_being_inlined, DATASOURCE_TYPES):
            return False
        if any(
            [
                x.safe_identifier == ds_being_inlined.safe_identifier
                for x in self.source.datasources
            ]
        ):
            return False

        self.source.datasources = [
            ds_being_inlined,
            *[
                x
                for x in self.source.datasources
                if x.safe_identifier != qds_being_inlined.safe_identifier
            ],
        ]
        # need to identify this before updating joins
        if self.base_name == parent.name:
            self.base_name_override = ds_being_inlined.safe_location
            self.base_alias_override = ds_being_inlined.safe_identifier

        # if we have a join to the parent, we need to remove it
        for join in self.joins:
            if isinstance(join, InstantiatedUnnestJoin):
                continue
            if (
                join.left_cte
                and join.left_cte.safe_identifier == parent.safe_identifier
            ):
                join.inline_cte(parent)
            if join.joinkey_pairs:
                for pair in join.joinkey_pairs:
                    if pair.cte and pair.cte.safe_identifier == parent.safe_identifier:
                        join.inline_cte(parent)
            if join.right_cte.safe_identifier == parent.safe_identifier:
                join.inline_cte(parent)
        for k, v in self.source_map.items():
            if isinstance(v, list):
                self.source_map[k] = [
                    (
                        ds_being_inlined.safe_identifier
                        if x == parent.safe_identifier
                        else x
                    )
                    for x in v
                ]
            elif v == parent.safe_identifier:
                self.source_map[k] = [ds_being_inlined.safe_identifier]

        # zip in any required values for lookups
        for k in ds_being_inlined.output_lcl.addresses:
            if k in self.source_map and self.source_map[k]:
                continue
            self.source_map[k] = [ds_being_inlined.safe_identifier]
        self.parent_ctes = [
            x for x in self.parent_ctes if x.safe_identifier != parent.safe_identifier
        ]
        if force_group:
            self.group_to_grain = True
        return True

    def __add__(self, other: "CTE" | "UnionCTE"):
        if isinstance(other, UnionCTE):
            raise ValueError("cannot merge CTE and union CTE")
        logger.info('Merging two copies of CTE "%s"', self.name)
        if not self.grain == other.grain:
            error = (
                "Attempting to merge two ctes of different grains"
                f" {self.name} {other.name} grains {self.grain} {other.grain}| {self.group_to_grain} {other.group_to_grain}| {self.output_lcl} {other.output_lcl}"
            )
            raise ValueError(error)
        if not self.condition == other.condition:
            error = (
                "Attempting to merge two ctes with different conditions"
                f" {self.name} {other.name} conditions {self.condition} {other.condition}"
            )
            raise ValueError(error)
        mutually_hidden = set()
        for concept in self.hidden_concepts:
            if concept in other.hidden_concepts:
                mutually_hidden.add(concept)
        self.partial_concepts = unique(
            self.partial_concepts + other.partial_concepts, "address"
        )
        self.parent_ctes = merge_ctes(self.parent_ctes + other.parent_ctes)

        self.source_map = {**self.source_map, **other.source_map}

        self.output_columns = unique(
            self.output_columns + other.output_columns, "address"
        )
        self.joins = unique(self.joins + other.joins, "unique_id")
        self.partial_concepts = unique(
            self.partial_concepts + other.partial_concepts, "address"
        )
        self.join_derived_concepts = unique(
            self.join_derived_concepts + other.join_derived_concepts, "address"
        )

        self.source.source_map = {**self.source.source_map, **other.source.source_map}
        self.source.output_concepts = unique(
            self.source.output_concepts + other.source.output_concepts, "address"
        )
        self.nullable_concepts = unique(
            self.nullable_concepts + other.nullable_concepts, "address"
        )
        self.hidden_concepts = mutually_hidden
        self.existence_source_map = {
            **self.existence_source_map,
            **other.existence_source_map,
        }

        return self

    @property
    def relevant_base_ctes(self):
        return self.parent_ctes

    @property
    def is_root_datasource(self) -> bool:
        return (
            len(self.source.datasources) == 1
            and isinstance(self.source.datasources[0], DATASOURCE_TYPES)
            and not self.source.datasources[0].name == CONSTANT_DATASET
        )

    @property
    def base_name(self) -> str:
        if self.base_name_override:
            return self.base_name_override
        # if this cte selects from a single datasource, select right from it
        if self.is_root_datasource:
            return self.source.datasources[0].safe_location

        # if we have multiple joined CTEs, pick the base
        # as the root
        elif len(self.source.datasources) == 1 and len(self.parent_ctes) == 1:
            return self.parent_ctes[0].name
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        return self.source.name

    @property
    def quote_address(self) -> bool:
        if self.is_root_datasource:
            root = self.source.datasources[0]
            if isinstance(root, BuildDatasource) and isinstance(root.address, Address):
                return not root.address.is_query
            return True
        elif not self.source.datasources:
            return False
        base = self.source.datasources[0]
        if isinstance(base, BuildDatasource):
            if isinstance(base.address, Address):
                return not base.address.is_query
            return True
        return True

    @property
    def base_alias(self) -> str:
        if self.base_alias_override:
            return self.base_alias_override
        if self.is_root_datasource:
            return self.source.datasources[0].identifier
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        elif self.parent_ctes:
            return self.parent_ctes[0].name
        return self.name

    def get_concept(self, address: str) -> BuildConcept | None:
        for cte in self.parent_ctes:
            if address in cte.output_columns:
                match = [x for x in cte.output_columns if x.address == address].pop()
                if match:
                    return match

        for array in [self.source.input_concepts, self.source.output_concepts]:
            match_list = [x for x in array if x.address == address]
            if match_list:
                return match_list.pop()
        match_list = [x for x in self.output_columns if x.address == address]
        if match_list:
            return match_list.pop()
        return None

    def get_alias(self, concept: BuildConcept, source: str | None = None) -> str:
        for cte in self.parent_ctes:
            if concept.address in cte.output_columns:
                if source and source != cte.name:
                    continue
                return concept.safe_address

        try:
            source = self.source.get_alias(concept, source=source)

            if not source:
                raise ValueError("No source found")
            return source
        except ValueError as e:
            return f"INVALID_ALIAS: {str(e)}"

    @property
    def group_concepts(self) -> List[BuildConcept]:
        def check_is_not_in_group(c: BuildConcept):
            if len(self.source_map.get(c.address, [])) > 0:
                return False
            if c.derivation == Derivation.ROWSET:
                assert isinstance(c.lineage, BuildRowsetItem)
                return check_is_not_in_group(c.lineage.content)
            if c.derivation == Derivation.CONSTANT:
                return True
            if c.purpose == Purpose.METRIC:
                return True

            if c.derivation == Derivation.BASIC and c.lineage:
                if all([check_is_not_in_group(x) for x in c.lineage.concept_arguments]):
                    return True
                if (
                    isinstance(c.lineage, BuildFunction)
                    and c.lineage.operator == FunctionType.GROUP
                ):
                    return check_is_not_in_group(c.lineage.concept_arguments[0])
            return False

        return (
            unique(
                [c for c in self.output_columns if not check_is_not_in_group(c)],
                "address",
            )
            if self.group_to_grain
            else []
        )

    @property
    def render_from_clause(self) -> bool:
        if (
            all([c.derivation == Derivation.CONSTANT for c in self.output_columns])
            and not self.parent_ctes
            and not self.group_to_grain
        ):
            return False
        # if we don't need to source any concepts from anywhere
        # render without from
        # most likely to happen from inlining constants
        if not any([v for v in self.source_map.values()]):
            return False
        if (
            len(self.source.datasources) == 1
            and self.source.datasources[0].name == CONSTANT_DATASET
        ):
            return False
        return True

    @property
    def sourced_concepts(self) -> List[BuildConcept]:
        return [c for c in self.output_columns if c.address in self.source_map]


class ConceptPair(BaseModel):
    left: BuildConcept
    right: BuildConcept
    existing_datasource: Union[BuildDatasource, "QueryDatasource"]
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_partial(self):
        return Modifier.PARTIAL in self.modifiers

    @property
    def is_nullable(self):
        return Modifier.NULLABLE in self.modifiers


class CTEConceptPair(ConceptPair):
    cte: CTE


class InstantiatedUnnestJoin(BaseModel):
    object_to_unnest: BuildConcept | BuildParamaterizedConceptReference | BuildFunction
    alias: str = "unnest"


class UnnestJoin(BaseModel):
    concepts: list[BuildConcept]
    parent: BuildFunction
    alias: str = "unnest"
    rendering_required: bool = True

    def __hash__(self):
        return self.safe_identifier.__hash__()

    @property
    def safe_identifier(self) -> str:
        return self.alias + "".join([str(s.address) for s in self.concepts])


class BaseJoin(BaseModel):
    right_datasource: Union[BuildDatasource, "QueryDatasource"]
    join_type: JoinType
    concepts: Optional[List[BuildConcept]] = None
    left_datasource: Optional[Union[BuildDatasource, "QueryDatasource"]] = None
    concept_pairs: list[ConceptPair] | None = None

    @model_validator(mode="after")
    def validate_join(self) -> "BaseJoin":
        if (
            self.left_datasource
            and self.left_datasource.identifier == self.right_datasource.identifier
        ):
            raise SyntaxError(
                f"Cannot join a dataself to itself, joining {self.left_datasource} and"
                f" {self.right_datasource}"
            )

        # Early returns maintained as in original code
        if self.concept_pairs:
            return self

        if self.concepts == []:
            return self

        # Validation logic
        final_concepts = []
        assert self.left_datasource and self.right_datasource

        for concept in self.concepts or []:
            include = True
            for ds in [self.left_datasource, self.right_datasource]:
                synonyms = []
                for c in ds.output_concepts:
                    synonyms += list(c.pseudonyms)
                if (
                    concept.address not in [c.address for c in ds.output_concepts]
                    and concept.address not in synonyms
                ):
                    raise SyntaxError(
                        f"Invalid join, missing {concept} on {ds.name}, have"
                        f" {[c.address for c in ds.output_concepts]}"
                    )
            if include:
                final_concepts.append(concept)

        if not final_concepts and self.concepts:
            # if one datasource only has constants
            # we can join on 1=1
            for ds in [self.left_datasource, self.right_datasource]:
                # single rows
                if all(
                    [
                        c.granularity == Granularity.SINGLE_ROW
                        for c in ds.output_concepts
                    ]
                ):
                    self.concepts = []
                    return self
                # if everything is at abstract grain, we can skip joins
                if all([c.grain.abstract for c in ds.output_concepts]):
                    self.concepts = []
                    return self

            left_keys = [c.address for c in self.left_datasource.output_concepts]
            right_keys = [c.address for c in self.right_datasource.output_concepts]
            match_concepts = [c.address for c in self.concepts]
            raise SyntaxError(
                "No mutual join keys found between"
                f" {self.left_datasource.identifier} and"
                f" {self.right_datasource.identifier}, left_keys {left_keys},"
                f" right_keys {right_keys},"
                f" provided join concepts {match_concepts}"
            )

        self.concepts = final_concepts
        return self

    @property
    def unique_id(self) -> str:
        return str(self)

    @property
    def input_concepts(self) -> List[BuildConcept]:
        base = []
        if self.concept_pairs:
            for pair in self.concept_pairs:
                base += [pair.left, pair.right]
        elif self.concepts:
            base += self.concepts
        return base

    def __str__(self):
        if self.concept_pairs:
            return (
                f"{self.join_type.value} {self.right_datasource.name} on"
                f" {','.join([str(k.existing_datasource.name) + '.'+ str(k.left)+'='+str(k.right) for k in self.concept_pairs])}"
            )
        return (
            f"{self.join_type.value} {self.right_datasource.name} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )


class QueryDatasource(BaseModel):
    input_concepts: List[BuildConcept]
    output_concepts: List[BuildConcept]
    datasources: List[Union[BuildDatasource, "QueryDatasource"]]
    source_map: Dict[str, Set[Union[BuildDatasource, "QueryDatasource", "UnnestJoin"]]]

    grain: BuildGrain
    joins: List[BaseJoin | UnnestJoin]
    limit: Optional[int] = None
    condition: Optional[
        Union[BuildConditional, BuildComparison, BuildParenthetical]
    ] = Field(default=None)
    source_type: SourceType = SourceType.SELECT
    partial_concepts: List[BuildConcept] = Field(default_factory=list)
    hidden_concepts: set[str] = Field(default_factory=set)
    nullable_concepts: List[BuildConcept] = Field(default_factory=list)
    join_derived_concepts: List[BuildConcept] = Field(default_factory=list)
    force_group: bool | None = None
    existence_source_map: Dict[str, Set[Union[BuildDatasource, "QueryDatasource"]]] = (
        Field(default_factory=dict)
    )
    ordering: BuildOrderBy | None = None

    def __repr__(self):
        return f"{self.identifier}@<{self.grain}>"

    @property
    def safe_identifier(self):
        return self.identifier.replace(".", "_")

    @property
    def full_concepts(self) -> List[BuildConcept]:
        return [
            c
            for c in self.output_concepts
            if c.address not in [z.address for z in self.partial_concepts]
        ]

    @field_validator("joins")
    @classmethod
    def validate_joins(cls, v):
        unique_pairs = set()
        for join in v:
            if not isinstance(join, BaseJoin):
                continue
            pairing = str(join)
            if pairing in unique_pairs:
                raise SyntaxError(f"Duplicate join {str(join)}")
            unique_pairs.add(pairing)
        return v

    @field_validator("input_concepts")
    @classmethod
    def validate_inputs(cls, v):
        return unique(v, "address")

    @field_validator("output_concepts")
    @classmethod
    def validate_outputs(cls, v):
        return unique(v, "address")

    @field_validator("source_map")
    @classmethod
    def validate_source_map(cls, v: dict, info: ValidationInfo):
        values = info.data
        hidden_concepts = values.get("hidden_concepts", set())
        for key in ("input_concepts", "output_concepts"):
            if not values.get(key):
                continue
            concept: BuildConcept
            for concept in values[key]:
                if concept.address in hidden_concepts:
                    continue
                if (
                    concept.address not in v
                    and not any(x in v for x in concept.pseudonyms)
                    and CONFIG.validate_missing
                ):
                    raise SyntaxError(
                        f"On query datasource from {values} missing source map entry (map: {v}) for {concept.address} on {key} with pseudonyms {concept.pseudonyms}, have {v}"
                    )
        return v

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return (self.identifier).__hash__()

    @property
    def concepts(self):
        return self.output_concepts

    @property
    def name(self):
        return self.identifier

    @property
    def group_required(self) -> bool:
        if self.force_group is True:
            return True
        if self.force_group is False:
            return False
        if self.source_type:
            if self.source_type in [
                SourceType.FILTER,
            ]:
                return False
            elif self.source_type in [
                SourceType.GROUP,
            ]:
                return True
        return False

    def __add__(self, other) -> "QueryDatasource":
        # these are syntax errors to avoid being caught by current
        if not isinstance(other, QueryDatasource):
            raise SyntaxError("Can only merge two query datasources")
        if not other.grain == self.grain:
            raise SyntaxError(
                "Can only merge two query datasources with identical grain"
            )
        if not self.group_required == other.group_required:
            raise SyntaxError(
                "can only merge two datasources if the group required flag is the same"
            )
        if not self.join_derived_concepts == other.join_derived_concepts:
            raise SyntaxError(
                "can only merge two datasources if the join derived concepts are the same"
            )
        if not self.force_group == other.force_group:
            raise SyntaxError(
                "can only merge two datasources if the force_group flag is the same"
            )
        logger.debug(
            f"[Query Datasource] merging {self.name} with"
            f" {[c.address for c in self.output_concepts]} concepts and"
            f" {other.name} with {[c.address for c in other.output_concepts]} concepts"
        )

        merged_datasources: dict[str, Union[BuildDatasource, "QueryDatasource"]] = {}

        for ds in [*self.datasources, *other.datasources]:
            if ds.safe_identifier in merged_datasources:
                merged_datasources[ds.safe_identifier] = (
                    merged_datasources[ds.safe_identifier] + ds
                )
            else:
                merged_datasources[ds.safe_identifier] = ds

        final_source_map: defaultdict[
            str, Set[Union[BuildDatasource, QueryDatasource, UnnestJoin]]
        ] = defaultdict(set)

        # add our sources
        for key in self.source_map:
            final_source_map[key] = self.source_map[key].union(
                other.source_map.get(key, set())
            )
        # add their sources
        for key in other.source_map:
            if key not in final_source_map:
                final_source_map[key] = other.source_map[key]

        # if a ds was merged (to combine columns), we need to update the source map
        # to use the merged item
        for k, v in final_source_map.items():
            final_source_map[k] = set(
                merged_datasources.get(x.safe_identifier, x) for x in list(v)
            )
        self_hidden: set[str] = self.hidden_concepts or set()
        other_hidden: set[str] = other.hidden_concepts or set()
        # hidden is the minimum overlapping set
        hidden = self_hidden.intersection(other_hidden)
        qds = QueryDatasource(
            input_concepts=unique(
                self.input_concepts + other.input_concepts, "address"
            ),
            output_concepts=unique(
                self.output_concepts + other.output_concepts, "address"
            ),
            source_map=final_source_map,
            datasources=list(merged_datasources.values()),
            grain=self.grain,
            joins=unique(self.joins + other.joins, "unique_id"),
            condition=(
                self.condition + other.condition
                if self.condition and other.condition
                else self.condition or other.condition
            ),
            source_type=self.source_type,
            partial_concepts=unique(
                self.partial_concepts + other.partial_concepts, "address"
            ),
            join_derived_concepts=self.join_derived_concepts,
            force_group=self.force_group,
            hidden_concepts=hidden,
            ordering=self.ordering,
        )
        logger.debug(
            f"[Query Datasource] merged with {[c.address for c in qds.output_concepts]} concepts"
        )
        return qds

    @property
    def identifier(self) -> str:
        filters = abs(hash(str(self.condition))) if self.condition else ""
        grain = "_".join([str(c).replace(".", "_") for c in self.grain.components])
        group = ""
        if self.source_type == SourceType.GROUP:
            keys = [
                x.address for x in self.output_concepts if x.purpose != Purpose.METRIC
            ]
            group = "_grouped_by_" + "_".join(keys)
        return (
            "_join_".join([d.identifier for d in self.datasources])
            + group
            + (f"_at_{grain}" if grain else "_at_abstract")
            + (f"_filtered_by_{filters}" if filters else "")
        )

    def get_alias(
        self,
        concept: BuildConcept,
        use_raw_name: bool = False,
        force_alias: bool = False,
        source: str | None = None,
    ):
        for x in self.datasources:
            # query datasources should be referenced by their alias, always
            force_alias = isinstance(x, QueryDatasource)
            #
            use_raw_name = isinstance(x, DATASOURCE_TYPES) and not force_alias
            if source and x.safe_identifier != source:
                continue
            try:
                return x.get_alias(
                    concept.with_grain(self.grain),
                    use_raw_name,
                    force_alias=force_alias,
                )
            except ValueError as e:
                from trilogy.constants import logger

                logger.debug(e)
                continue
        existing = [c.with_grain(self.grain) for c in self.output_concepts]
        if concept in existing:
            return concept.name

        existing_str = [str(c) for c in existing]
        datasources = [ds.identifier for ds in self.datasources]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {str(concept)} not found on {self.identifier};"
            f" have {existing_str} from {datasources}."
        )

    @property
    def safe_location(self):
        return self.identifier


class AliasedExpression(BaseModel):
    expr: BuildExpr
    alias: str


class RecursiveCTE(CTE):

    def generate_loop_functions(
        self,
        recursive_derived: BuildConcept,
        left_recurse_concept: BuildConcept,
        right_recurse_concept: BuildConcept,
    ) -> tuple[BuildConcept, BuildConcept, BuildConcept]:

        join_gate = BuildConcept(
            name=RECURSIVE_GATING_CONCEPT,
            namespace=DEFAULT_NAMESPACE,
            grain=recursive_derived.grain,
            build_is_aggregate=False,
            datatype=DataType.BOOL,
            purpose=Purpose.KEY,
            derivation=Derivation.BASIC,
            lineage=BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=right_recurse_concept,
                            right=MagicConstants.NULL,
                            operator=ComparisonOperator.IS,
                        ),
                        expr=True,
                    ),
                    BuildCaseElse(expr=False),
                ],
                output_datatype=DataType.BOOL,
                output_purpose=Purpose.KEY,
            ),
        )
        bottom_join_gate = BuildConcept(
            name=f"{RECURSIVE_GATING_CONCEPT}_two",
            namespace=DEFAULT_NAMESPACE,
            grain=recursive_derived.grain,
            build_is_aggregate=False,
            datatype=DataType.BOOL,
            purpose=Purpose.KEY,
            derivation=Derivation.BASIC,
            lineage=BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=right_recurse_concept,
                            right=MagicConstants.NULL,
                            operator=ComparisonOperator.IS,
                        ),
                        expr=True,
                    ),
                    BuildCaseElse(expr=False),
                ],
                output_datatype=DataType.BOOL,
                output_purpose=Purpose.KEY,
            ),
        )
        bottom_derivation = BuildConcept(
            name=recursive_derived.name + "_bottom",
            namespace=recursive_derived.namespace,
            grain=recursive_derived.grain,
            build_is_aggregate=False,
            datatype=recursive_derived.datatype,
            purpose=recursive_derived.purpose,
            derivation=Derivation.RECURSIVE,
            lineage=BuildFunction(
                operator=FunctionType.CASE,
                arguments=[
                    BuildCaseWhen(
                        comparison=BuildComparison(
                            left=right_recurse_concept,
                            right=MagicConstants.NULL,
                            operator=ComparisonOperator.IS,
                        ),
                        expr=recursive_derived,
                    ),
                    BuildCaseElse(expr=right_recurse_concept),
                ],
                output_datatype=recursive_derived.datatype,
                output_purpose=recursive_derived.purpose,
            ),
        )
        return bottom_derivation, join_gate, bottom_join_gate

    @property
    def internal_ctes(self) -> List[CTE]:
        filtered_output = [
            x for x in self.output_columns if x.name != RECURSIVE_GATING_CONCEPT
        ]
        recursive_derived = [
            x for x in self.output_columns if x.derivation == Derivation.RECURSIVE
        ][0]
        if not isinstance(recursive_derived.lineage, BuildFunction):
            raise SyntaxError(
                "Recursive CTEs must have a function lineage, found"
                f" {recursive_derived.lineage}"
            )
        left_recurse_concept = recursive_derived.lineage.concept_arguments[0]
        right_recurse_concept = recursive_derived.lineage.concept_arguments[1]
        parent_ctes: List[CTE | UnionCTE]
        if self.parent_ctes:
            base = self.parent_ctes[0]
            loop_input_cte = base
            parent_ctes = [base]
            parent_identifier = base.identifier
        else:
            raise SyntaxError("Recursive CTEs must have a parent CTE currently")
        bottom_derivation, join_gate, bottom_join_gate = self.generate_loop_functions(
            recursive_derived, left_recurse_concept, right_recurse_concept
        )
        base_output = [*filtered_output, join_gate]
        bottom_output = []
        for x in filtered_output:
            if x.address == recursive_derived.address:
                bottom_output.append(bottom_derivation)
            else:
                bottom_output.append(x)

        bottom_output = [*bottom_output, bottom_join_gate]
        top = CTE(
            name=self.name,
            source=self.source,
            output_columns=base_output,
            source_map=self.source_map,
            grain=self.grain,
            existence_source_map=self.existence_source_map,
            parent_ctes=self.parent_ctes,
            joins=self.joins,
            condition=self.condition,
            partial_concepts=self.partial_concepts,
            hidden_concepts=self.hidden_concepts,
            nullable_concepts=self.nullable_concepts,
            join_derived_concepts=self.join_derived_concepts,
            group_to_grain=self.group_to_grain,
            order_by=self.order_by,
            limit=self.limit,
        )
        top_cte_array: list[CTE | UnionCTE] = [top]
        bottom_source_map = {
            left_recurse_concept.address: [top.identifier],
            right_recurse_concept.address: [parent_identifier],
            # recursive_derived.address: self.source_map[recursive_derived.address],
            join_gate.address: [top.identifier],
            recursive_derived.address: [top.identifier],
        }
        bottom = CTE(
            name=self.name,
            source=self.source,
            output_columns=bottom_output,
            source_map=bottom_source_map,
            grain=self.grain,
            existence_source_map=self.existence_source_map,
            parent_ctes=top_cte_array + parent_ctes,
            joins=[
                Join(
                    right_cte=loop_input_cte,
                    jointype=JoinType.INNER,
                    joinkey_pairs=[
                        CTEConceptPair(
                            left=recursive_derived,
                            right=left_recurse_concept,
                            existing_datasource=loop_input_cte.source,
                            modifiers=[],
                            cte=top,
                        )
                    ],
                    condition=BuildComparison(
                        left=join_gate, right=True, operator=ComparisonOperator.IS_NOT
                    ),
                )
            ],
            partial_concepts=self.partial_concepts,
            hidden_concepts=self.hidden_concepts,
            nullable_concepts=self.nullable_concepts,
            join_derived_concepts=self.join_derived_concepts,
            group_to_grain=self.group_to_grain,
            order_by=self.order_by,
            limit=self.limit,
        )
        return [top, bottom]


class UnionCTE(BaseModel):
    name: str
    source: QueryDatasource
    parent_ctes: list[CTE | UnionCTE]
    internal_ctes: list[CTE | UnionCTE]
    output_columns: List[BuildConcept]
    grain: BuildGrain
    operator: str = "UNION ALL"
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    hidden_concepts: set[str] = Field(default_factory=set)
    partial_concepts: list[BuildConcept] = Field(default_factory=list)
    existence_source_map: Dict[str, list[str]] = Field(default_factory=dict)

    @computed_field  # type: ignore
    @property
    def output_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.output_columns)

    def get_alias(self, concept: BuildConcept, source: str | None = None) -> str:
        for cte in self.parent_ctes:
            if concept.address in cte.output_columns:
                if source and source != cte.name:
                    continue
                return concept.safe_address
        return "INVALID_ALIAS"

    def get_concept(self, address: str) -> BuildConcept | None:
        for cte in self.internal_ctes:
            if address in cte.output_columns:
                match = [x for x in cte.output_columns if x.address == address].pop()
                return match

        match_list = [x for x in self.output_columns if x.address == address]
        if match_list:
            return match_list.pop()
        return None

    @property
    def source_map(self):
        return {x.address: [] for x in self.output_columns}

    @property
    def condition(self):
        return None

    @condition.setter
    def condition(self, value):
        raise NotImplementedError

    @property
    def identifier(self) -> str:
        return self.name

    @property
    def safe_identifier(self):
        return self.name

    @property
    def group_to_grain(self) -> bool:
        return False

    def __add__(self, other):
        if not isinstance(other, UnionCTE) or not other.name == self.name:
            raise SyntaxError("Cannot merge union CTEs")
        return self


class Join(BaseModel):
    right_cte: CTE | UnionCTE
    jointype: JoinType
    left_cte: CTE | None = None
    joinkey_pairs: List[CTEConceptPair] | None = None
    inlined_ctes: set[str] = Field(default_factory=set)
    quote: str | None = None
    condition: BuildConditional | BuildComparison | BuildParenthetical | None = None

    def inline_cte(self, cte: CTE):
        self.inlined_ctes.add(cte.name)

    def get_name(self, cte: CTE):
        if cte.identifier in self.inlined_ctes:
            return cte.source.datasources[0].safe_identifier
        return cte.safe_identifier

    @property
    def right_name(self) -> str:
        if self.right_cte.identifier in self.inlined_ctes:
            return self.right_cte.source.datasources[0].safe_identifier
        return self.right_cte.safe_identifier

    @property
    def right_ref(self) -> str:
        if self.quote:
            if self.right_cte.identifier in self.inlined_ctes:
                return f"{safe_quote(self.right_cte.source.datasources[0].safe_location, self.quote)} as {self.quote}{self.right_cte.source.datasources[0].safe_identifier}{self.quote}"
            return f"{self.quote}{self.right_cte.safe_identifier}{self.quote}"
        if self.right_cte.identifier in self.inlined_ctes:
            return f"{self.right_cte.source.datasources[0].safe_location} as {self.right_cte.source.datasources[0].safe_identifier}"
        return self.right_cte.safe_identifier

    @property
    def unique_id(self) -> str:
        return str(self)

    def __str__(self):
        if self.joinkey_pairs:
            return (
                f"{self.jointype.value} join"
                f" {self.right_name} on"
                f" {','.join([k.cte.name + '.'+str(k.left.address)+'='+str(k.right.address) for k in self.joinkey_pairs])}"
            )
        elif self.left_cte:
            return (
                f"{self.jointype.value} JOIN {self.left_cte.name} and"
                f" {self.right_name} on {','.join([str(k) for k in self.joinkey_pairs])}"
            )
        return f"{self.jointype.value} JOIN  {self.right_name} on {','.join([str(k) for k in self.joinkey_pairs])}"


def merge_ctes(ctes: List[CTE | UnionCTE]) -> List[CTE | UnionCTE]:
    final_ctes_dict: Dict[str, CTE | UnionCTE] = {}
    # merge CTEs
    for cte in ctes:
        if cte.name not in final_ctes_dict:
            final_ctes_dict[cte.name] = cte
        else:
            final_ctes_dict[cte.name] = final_ctes_dict[cte.name] + cte

    final_ctes = list(final_ctes_dict.values())
    return final_ctes


class CompiledCTE(BaseModel):
    name: str
    statement: str


UnionCTE.model_rebuild()
