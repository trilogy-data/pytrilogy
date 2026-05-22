from typing import List, Union

from trilogy.core.models.author import ConceptRef, HavingClause, WhereClause


class SelectTypeMixin:
    having_clause: Union["HavingClause", None]

    @property
    def where_clause(self) -> Union["WhereClause", None]:
        raise NotImplementedError

    @property
    def output_components(self) -> List[ConceptRef]:
        raise NotImplementedError
