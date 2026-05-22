from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypeAlias

from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    decompose_condition,
)

ConditionInput: TypeAlias = "BuildConditionContext | BuildWhereClause | None"


def _atomize(clause: BuildWhereClause | None) -> tuple[BoolExpr, ...]:
    if clause is None:
        return ()
    return tuple(decompose_condition(clause.conditional))


def _combine_atoms(atoms: Sequence[BoolExpr]) -> BuildWhereClause | None:
    condition = combine_condition_atoms(list(atoms))
    if condition is None:
        return None
    return BuildWhereClause(conditional=condition)


def _concept_dependency_addresses(
    concept: BuildConcept,
    seen: set[str] | None = None,
) -> set[str]:
    seen = seen if seen is not None else set()
    if concept.address in seen:
        return seen
    seen.add(concept.address)
    for source in concept.sources:
        _concept_dependency_addresses(source, seen)
    for argument in concept.concept_arguments:
        _concept_dependency_addresses(argument, seen)
    return seen


def _atom_depends_on(atom: BoolExpr, concept: BuildConcept) -> bool:
    addresses: set[str] = set()
    for arg in atom.row_arguments:
        addresses.update(_concept_dependency_addresses(arg))
    return concept.address in addresses


def _atom_has_aggregate_argument(atom: BoolExpr) -> bool:
    return any(getattr(arg, "is_aggregate", False) for arg in atom.row_arguments)


def _atom_has_row_argument(atom: BoolExpr) -> bool:
    return any(not getattr(arg, "is_aggregate", False) for arg in atom.row_arguments)


def _filter_owner_atoms(
    stage: tuple[BoolExpr, ...],
    owner: BuildConcept,
) -> tuple[BoolExpr, ...]:
    return tuple(atom for atom in stage if not _atom_depends_on(atom, owner))


@dataclass(frozen=True)
class BuildConditionContext:
    applied: tuple[tuple[BoolExpr, ...], ...] = ()
    pending: tuple[tuple[BoolExpr, ...], ...] = ()

    @classmethod
    def from_where_clause(
        cls, clause: BuildWhereClause | None
    ) -> "BuildConditionContext | None":
        atoms = _atomize(clause)
        if not atoms:
            return None
        return cls(pending=(atoms,))

    @classmethod
    def from_where_clauses(
        cls, clauses: Sequence[BuildWhereClause]
    ) -> "BuildConditionContext | None":
        stages = tuple(stage for stage in (_atomize(clause) for clause in clauses) if stage)
        if not stages:
            return None
        return cls(pending=stages)

    @classmethod
    def normalize(cls, conditions: ConditionInput) -> "BuildConditionContext | None":
        if conditions is None:
            return None
        if isinstance(conditions, BuildConditionContext):
            return conditions
        return cls.from_where_clause(conditions)

    @property
    def current_stage(self) -> tuple[BoolExpr, ...]:
        if not self.pending:
            return ()
        return self.pending[0]

    @property
    def current_where(self) -> BuildWhereClause | None:
        return _combine_atoms(self.current_stage)

    @property
    def discovery_where(self) -> BuildWhereClause | None:
        if self.pending:
            return self.current_where
        return self.active_where

    @property
    def active_atoms(self) -> tuple[BoolExpr, ...]:
        if not self.pending:
            return tuple(atom for stage in self.applied for atom in stage)
        return tuple(atom for stage in (*self.applied, self.pending[0]) for atom in stage)

    @property
    def applied_atoms(self) -> tuple[BoolExpr, ...]:
        return tuple(atom for stage in self.applied for atom in stage)

    @property
    def full_atoms(self) -> tuple[BoolExpr, ...]:
        return tuple(atom for stage in (*self.applied, *self.pending) for atom in stage)

    @property
    def active_where(self) -> BuildWhereClause | None:
        return _combine_atoms(self.active_atoms)

    @property
    def applied_where(self) -> BuildWhereClause | None:
        return _combine_atoms(self.applied_atoms)

    @property
    def full_where(self) -> BuildWhereClause | None:
        return _combine_atoms(self.full_atoms)

    @property
    def conditional(self) -> BoolExpr:
        active = self.active_where
        if active is None:
            raise ValueError("Condition context has no active condition.")
        return active.conditional

    @property
    def concept_arguments(self) -> list[BuildConcept]:
        active = self.active_where
        return list(active.concept_arguments) if active else []

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        active = self.active_where
        return active.row_arguments if active else []

    @property
    def existence_arguments(self) -> Sequence[tuple[BuildConcept, ...]]:
        active = self.active_where
        return active.existence_arguments if active else []

    @property
    def is_complete(self) -> bool:
        return not self.pending

    @property
    def cache_key(self) -> str:
        def render(stages: tuple[tuple[BoolExpr, ...], ...]) -> str:
            return " || ".join(
                " && ".join(str(atom) for atom in stage) for stage in stages
            )

        return f"applied=[{render(self.applied)}]|pending=[{render(self.pending)}]"

    def advance(self) -> "BuildConditionContext":
        if not self.pending:
            return self
        return BuildConditionContext(
            applied=(*self.applied, self.pending[0]),
            pending=self.pending[1:],
        )

    def focus(self, local_where: BuildWhereClause | None) -> "BuildConditionContext | None":
        stage = _atomize(local_where)
        if not self.applied and not stage:
            return None
        pending = (stage,) if stage else ()
        return BuildConditionContext(applied=self.applied, pending=pending)

    def for_child(self, owner: BuildConcept) -> "BuildConditionContext | None":
        applied = tuple(
            stage
            for stage in (_filter_owner_atoms(stage, owner) for stage in self.applied)
            if stage
        )
        current = self.current_stage
        current_has_aggregate_filter = any(
            _atom_has_aggregate_argument(atom) for atom in current
        )
        current_has_row_filter = any(_atom_has_row_argument(atom) for atom in current)
        if current and (
            any(_atom_depends_on(atom, owner) for atom in current)
            or (
                owner.is_aggregate
                and current_has_aggregate_filter
                and not current_has_row_filter
            )
        ):
            pending: tuple[tuple[BoolExpr, ...], ...] = ()
        else:
            pending = (current,) if current else ()
        if not applied and not pending:
            return None
        return BuildConditionContext(applied=applied, pending=pending)

    def for_children(
        self,
        owners: Sequence[BuildConcept],
    ) -> "BuildConditionContext | None":
        context: BuildConditionContext | None = self
        for owner in owners:
            if context is None:
                return None
            context = context.for_child(owner)
        return context

    def with_only_addresses(
        self,
        addresses: set[str],
    ) -> "BuildConditionContext | None":
        def filter_stage(stage: tuple[BoolExpr, ...]) -> tuple[BoolExpr, ...]:
            return tuple(
                atom
                for atom in stage
                if all(c.address in addresses for c in atom.concept_arguments)
            )

        applied = tuple(stage for stage in (filter_stage(s) for s in self.applied) if stage)
        pending = tuple(stage for stage in (filter_stage(s) for s in self.pending) if stage)
        if not applied and not pending:
            return None
        return BuildConditionContext(applied=applied, pending=pending)

    def __repr__(self) -> str:
        active = self.active_where
        return str(active) if active else ""

    def __str__(self) -> str:
        return self.__repr__()
