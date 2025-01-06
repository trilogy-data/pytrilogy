from typing import List, Union

from pydantic import BaseModel, Field

from trilogy.core.enums import Derivation, IOType, SelectFiltering
from trilogy.core.models_author import Concept, HavingClause, WhereClause
from trilogy.core.models_datasource import Address, Datasource


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
    def output_components(self) -> List[Concept]:
        raise NotImplementedError

    @property
    def implicit_where_clause_selections(self) -> List[Concept]:
        if not self.where_clause:
            return []
        filter = set(
            [
                str(x.address)
                for x in self.where_clause.row_arguments
                if not x.derivation == Derivation.CONSTANT
            ]
        )
        query_output = set([str(z.address) for z in self.output_components])
        delta = filter.difference(query_output)
        if delta:
            return [
                x for x in self.where_clause.row_arguments if str(x.address) in delta
            ]
        return []

    @property
    def where_clause_category(self) -> SelectFiltering:
        if not self.where_clause:
            return SelectFiltering.NONE
        elif self.implicit_where_clause_selections:
            return SelectFiltering.IMPLICIT
        return SelectFiltering.EXPLICIT
