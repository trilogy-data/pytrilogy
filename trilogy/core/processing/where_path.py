from __future__ import annotations

from dataclasses import dataclass

from trilogy.core.models.build import BuildWhereClause, combine_build_where_clauses


@dataclass(frozen=True)
class BuildWherePath:
    applied: tuple[BuildWhereClause, ...] = ()
    pending: tuple[BuildWhereClause, ...] = ()

    @classmethod
    def from_clauses(cls, clauses: list[BuildWhereClause]) -> "BuildWherePath | None":
        filtered = tuple(clause for clause in clauses if clause is not None)
        if not filtered:
            return None
        return cls(pending=filtered)

    @property
    def active_condition(self) -> BuildWhereClause | None:
        if not self.pending:
            return combine_build_where_clauses(self.applied)
        return combine_build_where_clauses([*self.applied, self.pending[0]])

    @property
    def current_condition(self) -> BuildWhereClause | None:
        if not self.pending:
            return None
        return self.pending[0]

    @property
    def applied_condition(self) -> BuildWhereClause | None:
        return combine_build_where_clauses(self.applied)

    @property
    def full_condition(self) -> BuildWhereClause | None:
        return combine_build_where_clauses([*self.applied, *self.pending])

    @property
    def is_complete(self) -> bool:
        return not self.pending

    @property
    def cache_key(self) -> str:
        applied = " && ".join(str(clause) for clause in self.applied)
        pending = " && ".join(str(clause) for clause in self.pending)
        return f"applied=[{applied}]|pending=[{pending}]"

    def advance(self) -> "BuildWherePath":
        if not self.pending:
            return self
        return BuildWherePath(
            applied=(*self.applied, self.pending[0]),
            pending=self.pending[1:],
        )
