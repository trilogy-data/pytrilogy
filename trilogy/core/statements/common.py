from typing import List, Union

from pydantic import BaseModel, Field

from trilogy.core.enums import IOType
from trilogy.core.models.author import ConceptRef, HavingClause, WhereClause
from trilogy.core.models.datasource import Address, Datasource


class CopyQueryMixin(BaseModel):
    target: str
    target_type: IOType


class MaterializedDataset(BaseModel):
    address: Address


class PersistQueryMixin(BaseModel):
    output_to: MaterializedDataset
    datasource: Datasource


class SelectTypeMixin(BaseModel):
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)

    @property
    def output_components(self) -> List[ConceptRef]:
        raise NotImplementedError
