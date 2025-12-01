from dataclasses import dataclass, field
from typing import List, Optional, Union

from trilogy.core.enums import (
    CreateMode,
    IOType,
    PersistMode,
    PublishAction,
    ValidationScope,
)
from trilogy.core.models.author import ConceptRef, HavingClause, WhereClause
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildOrderBy,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.models.environment import EnvironmentConceptDict
from trilogy.core.models.execute import CTE, UnionCTE


@dataclass
class CopyQueryMixin:
    target: str
    target_type: IOType


@dataclass
class MaterializedDataset:
    address: Address


@dataclass
class PersistQueryMixin:
    output_to: MaterializedDataset
    datasource: Datasource
    persist_mode: PersistMode
    partition_by: List[str]
    partition_types: List[DataType]


@dataclass
class SelectTypeMixin:
    where_clause: Union["WhereClause", None] = field(default=None)
    having_clause: Union["HavingClause", None] = field(default=None)

    @property
    def output_components(self) -> List[ConceptRef]:
        raise NotImplementedError


@dataclass
class ProcessedQuery:
    output_columns: List[ConceptRef]
    ctes: List[CTE | UnionCTE]
    base: CTE | UnionCTE
    hidden_columns: set[str] = field(default_factory=set)
    limit: Optional[int] = None
    order_by: Optional[BuildOrderBy] = None
    local_concepts: EnvironmentConceptDict = field(
        default_factory=EnvironmentConceptDict
    )
    locally_derived: set[str] = field(default_factory=set)


@dataclass
class ProcessedQueryPersist(ProcessedQuery, PersistQueryMixin):
    pass


@dataclass
class ProcessedCopyStatement(ProcessedQuery, CopyQueryMixin):
    pass


@dataclass
class ProcessedRawSQLStatement:
    text: str


@dataclass
class ProcessedValidateStatement:
    scope: ValidationScope
    targets: Optional[List[str]]


@dataclass
class ProcessedMockStatement:
    scope: ValidationScope
    targets: list[str]


@dataclass
class ColumnInfo:
    name: str
    type: DataType
    nullable: bool = True
    primary_key: bool = False
    description: Optional[str] = None


@dataclass
class CreateTableInfo:
    name: str
    columns: List[ColumnInfo]
    partition_keys: list[str] = field(default_factory=list)


@dataclass
class ProcessedCreateStatement:
    scope: ValidationScope
    create_mode: CreateMode
    targets: list[CreateTableInfo]


@dataclass
class ProcessedPublishStatement:
    scope: ValidationScope
    targets: list[str]
    action: PublishAction


@dataclass
class ProcessedStaticValueOutput:
    values: List[dict]


@dataclass
class ProcessedShowStatement:
    output_columns: List[ConceptRef]
    output_values: List[
        Union[
            BuildConcept,
            BuildDatasource,
            ProcessedQuery,
            ProcessedQueryPersist,
            ProcessedCopyStatement,
            ProcessedValidateStatement,
            ProcessedStaticValueOutput,
        ]
    ]


PROCESSED_STATEMENT_TYPES = (
    ProcessedCopyStatement
    | ProcessedQuery
    | ProcessedRawSQLStatement
    | ProcessedQueryPersist
    | ProcessedShowStatement
    | ProcessedValidateStatement
    | ProcessedCreateStatement
    | ProcessedPublishStatement
    | ProcessedMockStatement
)
