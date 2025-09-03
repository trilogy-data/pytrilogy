from dataclasses import dataclass, field
from typing import List, Optional, Union

from trilogy.core.enums import IOType, ValidationScope
from trilogy.core.models.author import ConceptRef, HavingClause, WhereClause
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildOrderBy,
)
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
)
