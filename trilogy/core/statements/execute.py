from dataclasses import dataclass, field
from typing import Any, Union

from trilogy.core.enums import (
    CreateMode,
    IOType,
    PersistMode,
    PublishAction,
    ScaleType,
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
from trilogy.core.scope_diagnostics import DerivedValueScope


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
    partition_by: list[str]
    partition_types: list[DataType]


@dataclass
class SelectTypeMixin:
    where_clause: Union["WhereClause", None] = field(default=None)
    having_clause: Union["HavingClause", None] = field(default=None)

    @property
    def output_components(self) -> list[ConceptRef]:
        raise NotImplementedError


@dataclass
class ProcessedQuery:
    output_columns: list[ConceptRef]
    ctes: list[CTE | UnionCTE]
    base: CTE | UnionCTE
    hidden_columns: set[str] = field(default_factory=set)
    limit: int | None = None
    order_by: BuildOrderBy | None = None
    local_concepts: EnvironmentConceptDict = field(
        default_factory=EnvironmentConceptDict
    )
    locally_derived: set[str] = field(default_factory=set)
    parameters: dict[str, Any] = field(default_factory=dict)
    # in-query JOIN source address -> canonical target address, so the output
    # projection can render a collapsed source under the name the user wrote.
    scoped_merge_map: dict[str, str] = field(default_factory=dict)
    # observational scope diagnostics (docs/SPEC_query_derived_value_scopes.md);
    # empty when the query has no aggregate/window values or extraction failed.
    derived_value_scopes: list[DerivedValueScope] = field(default_factory=list)


@dataclass
class ProcessedQueryPersist(ProcessedQuery, PersistQueryMixin):
    pass


@dataclass
class ProcessedCopyStatement(ProcessedQuery, CopyQueryMixin):
    column_aliases: dict[str, str] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessedRawSQLStatement:
    text: str


@dataclass
class ProcessedValidateStatement:
    scope: ValidationScope
    targets: list[str] | None


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
    description: str | None = None


@dataclass
class CreateTableInfo:
    name: str
    columns: list[ColumnInfo]
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
    values: list[dict]


@dataclass
class ProcessedShowStatement:
    output_columns: list[ConceptRef]
    output_values: list[
        BuildConcept | BuildDatasource | ProcessedQuery | ProcessedQueryPersist | ProcessedCopyStatement | ProcessedValidateStatement | ProcessedStaticValueOutput
    ]


@dataclass
class ProcessedChartLayer:
    layer_type: "ChartType"
    query: ProcessedQuery | None = None
    x_fields: list[str] = field(default_factory=list)
    y_fields: list[str] = field(default_factory=list)
    color_field: str | None = None
    size_field: str | None = None
    group_field: str | None = None
    x_trellis_field: str | None = None
    y_trellis_field: str | None = None
    geo_field: str | None = None
    annotation_field: str | None = None


@dataclass
class ProcessedChartStatement:
    layers: list[ProcessedChartLayer]
    placements: list["ChartPlacement"] = field(default_factory=list)
    hide_legend: bool = False
    show_title: bool = False
    scale_x: ScaleType | None = None
    scale_y: ScaleType | None = None


@dataclass
class ProcessedChartCopyStatement(CopyQueryMixin):
    chart: ProcessedChartStatement = field(
        default_factory=lambda: ProcessedChartStatement(layers=[])
    )
    options: dict[str, Any] = field(default_factory=dict)


# Import here to avoid circular import
from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartPlacement

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
    | ProcessedChartStatement
    | ProcessedChartCopyStatement
)
