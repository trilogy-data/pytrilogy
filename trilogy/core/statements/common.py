from typing import List, Union

from trilogy.core.models.author import ConceptRef, HavingClause, WhereClause


class SelectTypeMixin:
    where_clause: Union["WhereClause", None]
    having_clause: Union["HavingClause", None]

    @property
    def output_components(self) -> List[ConceptRef]:
        raise NotImplementedError
