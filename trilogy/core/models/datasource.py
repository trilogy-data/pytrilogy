from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, ItemsView, List, Optional, Union, ValuesView

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from trilogy.constants import DEFAULT_NAMESPACE, MagicConstants, logger
from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ComparisonOperator,
    DatasourceState,
    Modifier,
)
from trilogy.core.models.author import (
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    Grain,
    HasUUID,
    LooseConceptList,
    Namespaced,
    WhereClause,
)

LOGGER_PREFIX = "[MODELS_DATASOURCE]"

if TYPE_CHECKING:
    from trilogy.core.models.environment import Environment
    from trilogy.core.statements.author import SelectStatement


class UpdateKeyType(Enum):
    INCREMENTAL_KEY = "incremental_key"
    UPDATE_TIME = "update_time"
    KEY_HASH = "key_hash"


@dataclass
class UpdateKey:
    """Represents a key used to track data freshness for incremental updates."""

    concept_name: str
    type: UpdateKeyType
    value: str | int | float | datetime | date | None

    def to_comparison(self, environment: "Environment") -> "Comparison":
        """Convert this update key to a Comparison for use in WHERE clauses."""

        concept = environment.concepts[self.concept_name]
        right_value = self.value if self.value is not None else MagicConstants.NULL
        return Comparison(
            left=concept.reference,
            right=right_value,
            operator=ComparisonOperator.GT,
        )


@dataclass
class UpdateKeys:
    """Collection of update keys for a datasource."""

    keys: dict[str, UpdateKey] = field(default_factory=dict)

    def to_where_clause(self, environment: "Environment") -> WhereClause | None:
        """Convert update keys to a WhereClause for filtering."""

        comparisons = [
            key.to_comparison(environment)
            for key in self.keys.values()
            if key.value is not None
        ]
        if not comparisons:
            return None
        if len(comparisons) == 1:
            return WhereClause(conditional=comparisons[0])
        conditional = Conditional(
            left=comparisons[0],
            right=comparisons[1],
            operator=BooleanOperator.AND,
        )
        for comp in comparisons[2:]:
            conditional = Conditional(
                left=conditional, right=comp, operator=BooleanOperator.AND
            )
        return WhereClause(conditional=conditional)


class RawColumnExpr(BaseModel):
    text: str


class ColumnAssignment(BaseModel):
    alias: str | RawColumnExpr | Function
    concept: ConceptRef
    modifiers: List[Modifier] = Field(default_factory=list)

    @field_validator("concept", mode="before")
    def force_reference(cls, v: ConceptRef, info: ValidationInfo):
        if isinstance(v, Concept):
            return v.reference
        return v

    def __eq__(self, other):
        if not isinstance(other, ColumnAssignment):
            return False
        return (
            self.alias == other.alias
            and self.concept == other.concept
            and self.modifiers == other.modifiers
        )

    @property
    def is_concrete(self) -> bool:
        return isinstance(self.alias, str)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers

    def with_namespace(self, namespace: str) -> "ColumnAssignment":
        return ColumnAssignment.model_construct(
            alias=(
                self.alias.with_namespace(namespace)
                if isinstance(self.alias, Function)
                else self.alias
            ),
            concept=self.concept.with_namespace(namespace),
            modifiers=self.modifiers,
        )

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "ColumnAssignment":
        return ColumnAssignment.model_construct(
            alias=self.alias,
            concept=self.concept.with_merge(source, target, modifiers),
            modifiers=(
                modifiers if self.concept.address == source.address else self.modifiers
            ),
        )


class Address(BaseModel):
    location: str
    quoted: bool = False
    type: AddressType = AddressType.TABLE

    @property
    def is_query(self):
        return self.type == AddressType.QUERY

    @property
    def is_file(self):
        return self.type in {
            AddressType.PYTHON_SCRIPT,
            AddressType.CSV,
            AddressType.TSV,
            AddressType.PARQUET,
            AddressType.SQL,
        }


@dataclass
class Query:
    text: str


@dataclass
class File:
    path: str
    type: AddressType


class DatasourceMetadata(BaseModel):
    freshness_concept: Concept | None
    partition_fields: List[Concept] = Field(default_factory=list)
    line_no: int | None = None


def safe_grain(v) -> Grain:
    if isinstance(v, dict):
        return Grain.model_validate(v)
    elif isinstance(v, Grain):
        return v
    elif not v:
        return Grain(components=set())
    else:
        raise ValueError(f"Invalid input type to safe_grain {type(v)}")


class Datasource(HasUUID, Namespaced, BaseModel):
    name: str
    columns: List[ColumnAssignment]
    address: Union[Address, str]
    grain: Grain = Field(
        default_factory=lambda: Grain(components=set()), validate_default=True
    )
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: DatasourceMetadata = Field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )
    where: Optional[WhereClause] = None
    non_partial_for: Optional[WhereClause] = None
    status: DatasourceState = Field(default=DatasourceState.PUBLISHED)
    incremental_by: List[ConceptRef] = Field(default_factory=list)
    partition_by: List[ConceptRef] = Field(default_factory=list)
    is_root: bool = False

    @property
    def safe_address(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address

    def __eq__(self, other):
        if not isinstance(other, Datasource):
            return False
        return (
            self.name == other.name
            and self.namespace == other.namespace
            and self.grain == other.grain
            and self.address == other.address
            and self.where == other.where
            and self.columns == other.columns
            and self.non_partial_for == other.non_partial_for
        )

    def duplicate(self) -> "Datasource":
        return self.model_copy(deep=True)

    @property
    def concrete_columns(self) -> dict[str, ColumnAssignment]:
        return {c.alias: c for c in self.columns if c.is_concrete}  # type: ignore[misc]

    @property
    def hidden_concepts(self) -> List[Concept]:
        return []

    def merge_concept(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ):
        original = [c for c in self.columns if c.concept.address == source.address]
        early_exit_check = [
            c for c in self.columns if c.concept.address == target.address
        ]
        if early_exit_check:
            logger.info(
                f"No concept merge needed on merge of {source} to {target}, have {[x.concept.address for x in self.columns]}"
            )
            return None
        if len(original) != 1:
            raise ValueError(
                f"Expected exactly one column to merge, got {len(original)} for {source.address}, {[x.alias for x in original]}"
            )
        # map to the alias with the modifier, and the original
        self.columns = [
            c.with_merge(source, target, modifiers)
            for c in self.columns
            if c.concept.address != source.address
        ] + original
        self.grain = self.grain.with_merge(source, target, modifiers)
        self.where = (
            self.where.with_merge(source, target, modifiers) if self.where else None
        )

        self.add_column(target, original[0].alias, modifiers)

    @property
    def identifier(self) -> str:
        if not self.namespace or self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"

    @property
    def safe_identifier(self) -> str:
        return self.identifier.replace(".", "_")

    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_concepts)

    @property
    def non_partial_concept_addresses(self) -> set[str]:
        return set([c.address for c in self.full_concepts])

    @field_validator("namespace", mode="plain")
    @classmethod
    def namespace_validation(cls, v):
        return v or DEFAULT_NAMESPACE

    @field_validator("address")
    @classmethod
    def address_enforcement(cls, v):
        if isinstance(v, str):
            v = Address(location=v)
        return v

    @field_validator("grain", mode="before")
    @classmethod
    def grain_enforcement(cls, v: Grain, info: ValidationInfo):
        grain: Grain = safe_grain(v)
        return grain

    def add_column(
        self,
        concept: Concept,
        alias: str | RawColumnExpr | Function,
        modifiers: List[Modifier] | None = None,
    ):
        self.columns.append(
            ColumnAssignment(
                alias=alias, concept=concept.reference, modifiers=modifiers or []
            )
        )

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this is not a valid operation"
            )
        return self

    def __repr__(self):
        return f"Datasource<{self.identifier}@<{self.grain}>"

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return self.identifier.__hash__()

    def with_namespace(self, namespace: str):
        new_namespace = (
            namespace + "." + self.namespace
            if self.namespace and self.namespace != DEFAULT_NAMESPACE
            else namespace
        )
        new = Datasource.model_construct(
            name=self.name,
            namespace=new_namespace,
            grain=self.grain.with_namespace(namespace),
            address=self.address,
            columns=[c.with_namespace(namespace) for c in self.columns],
            where=self.where.with_namespace(namespace) if self.where else None,
            non_partial_for=(
                self.non_partial_for.with_namespace(namespace)
                if self.non_partial_for
                else None
            ),
            status=self.status,
            incremental_by=[c.with_namespace(namespace) for c in self.incremental_by],
            partition_by=[c.with_namespace(namespace) for c in self.partition_by],
            is_root=self.is_root,
        )
        return new

    def create_update_statement(
        self,
        environment: "Environment",
        where: Optional[WhereClause] = None,
        line_no: int | None = None,
    ) -> "SelectStatement":
        from trilogy.core.statements.author import Metadata, SelectItem, SelectStatement

        return SelectStatement.from_inputs(
            environment=environment,
            selection=[
                SelectItem(
                    content=ConceptRef(address=col.concept.address),
                    modifiers=[],
                )
                for col in self.columns
            ],
            where_clause=where,
            meta=Metadata(line_number=line_no) if line_no else None,
        )

    @property
    def concepts(self) -> List[ConceptRef]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def full_concepts(self) -> List[ConceptRef]:
        return [c.concept for c in self.columns if Modifier.PARTIAL not in c.modifiers]

    @property
    def nullable_concepts(self) -> List[ConceptRef]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def output_concepts(self) -> List[ConceptRef]:
        return self.concepts

    @property
    def partial_concepts(self) -> List[ConceptRef]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]


class EnvironmentDatasourceDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def __getitem__(self, key: str) -> Datasource:
        try:
            return super(EnvironmentDatasourceDict, self).__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key)
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1])
            raise

    def values(self) -> ValuesView[Datasource]:  # type: ignore
        return super().values()

    def items(self) -> ItemsView[str, Datasource]:  # type: ignore
        return super().items()

    def duplicate(self) -> "EnvironmentDatasourceDict":
        new = EnvironmentDatasourceDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        return new
