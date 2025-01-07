from typing import Annotated, List, Optional, Union

from pydantic import BaseModel, Field
from pydantic.functional_validators import PlainValidator

from trilogy.core.models_author import (
    Concept,
    Grain,
    HavingClause,
    OrderBy,
    WhereClause,
)
from trilogy.core.models_datasource import Datasource
from trilogy.core.models_environment import EnvironmentConceptDict, validate_concepts
from trilogy.core.models_execute import CTE, Join, UnionCTE
from trilogy.core.statements_common import CopyQueryMixin, PersistQueryMixin


class ProcessedQuery(BaseModel):
    output_columns: List[Concept]
    ctes: List[CTE | UnionCTE]
    base: CTE | UnionCTE
    joins: List[Join]
    grain: Grain
    hidden_columns: set[str] = Field(default_factory=set)
    limit: Optional[int] = None
    where_clause: Optional[WhereClause] = None
    having_clause: Optional[HavingClause] = None
    order_by: Optional[OrderBy] = None
    local_concepts: Annotated[
        EnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=EnvironmentConceptDict)


class ProcessedQueryPersist(ProcessedQuery, PersistQueryMixin):
    pass


class ProcessedCopyStatement(ProcessedQuery, CopyQueryMixin):
    pass


class ProcessedRawSQLStatement(BaseModel):
    text: str


class ProcessedShowStatement(BaseModel):
    output_columns: List[Concept]
    output_values: List[Union[Concept, Datasource, ProcessedQuery]]
