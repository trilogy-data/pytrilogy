from typing import Union

from trilogy.core.models.author import ConceptRef, HavingClause


class SelectTypeMixin:
    # `where_clause` is deliberately not declared here: SelectStatement derives
    # it read-only from its `then where` stages while MultiSelectStatement
    # stores it, and a shared writable annotation would describe neither.
    having_clause: Union["HavingClause", None]

    @property
    def output_components(self) -> list[ConceptRef]:
        raise NotImplementedError
