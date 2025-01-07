from typing import ItemsView, List, Optional, Union, ValuesView

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import Modifier
from trilogy.core.models_author import (
    Concept,
    Function,
    Grain,
    HasUUID,
    LooseConceptList,
    Namespaced,
    WhereClause,
)

LOGGER_PREFIX = "[MODELS_DATASOURCE]"


class RawColumnExpr(BaseModel):
    text: str


class ColumnAssignment(BaseModel):
    alias: str | RawColumnExpr | Function
    concept: Concept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers

    def with_namespace(self, namespace: str) -> "ColumnAssignment":
        return ColumnAssignment(
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
        return ColumnAssignment(
            alias=self.alias,
            concept=self.concept.with_merge(source, target, modifiers),
            modifiers=(
                modifiers if self.concept.address == source.address else self.modifiers
            ),
        )


class Address(BaseModel):
    location: str
    is_query: bool = False
    quoted: bool = False


class Query(BaseModel):
    text: str


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

    def duplicate(self) -> "Datasource":
        return self.model_copy(deep=True)

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
    def condition(self):
        return None

    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_concepts)

    @property
    def can_be_inlined(self) -> bool:
        if isinstance(self.address, Address) and self.address.is_query:
            return False
        # for x in self.columns:
        #     if not isinstance(x.alias, str):
        #         return False
        return True

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
            ColumnAssignment(alias=alias, concept=concept, modifiers=modifiers or [])
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
        return Datasource(
            name=self.name,
            namespace=new_namespace,
            grain=self.grain.with_namespace(namespace),
            address=self.address,
            columns=[c.with_namespace(namespace) for c in self.columns],
            where=self.where.with_namespace(namespace) if self.where else None,
        )

    @property
    def concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def full_concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL not in c.modifiers]

    @property
    def nullable_concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def output_concepts(self) -> List[Concept]:
        return self.concepts

    @property
    def partial_concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]

    def get_alias(
        self, concept: Concept, use_raw_name: bool = True, force_alias: bool = False
    ) -> Optional[str | RawColumnExpr] | Function:
        # 2022-01-22
        # this logic needs to be refined.
        # if concept.lineage:
        # #     return None
        for x in self.columns:
            if x.concept == concept or x.concept.with_grain(concept.grain) == concept:
                if use_raw_name:
                    return x.alias
                return concept.safe_address
        existing = [str(c.concept.with_grain(self.grain)) for c in self.columns]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {concept} not found on {self.identifier}; have"
            f" {existing}."
        )

    @property
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address


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
