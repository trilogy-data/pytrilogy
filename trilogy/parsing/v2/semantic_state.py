from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator

from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment


class ConceptUpdateKind(Enum):
    TOP_LEVEL_DECLARATION = "top_level_declaration"
    PROPERTY_DECLARATION = "property_declaration"
    DATASOURCE_PROPERTY = "datasource_property"
    SELECT_LOCAL = "select_local"
    MULTISELECT_OUTPUT = "multiselect_output"
    ROWSET_OUTPUT = "rowset_output"
    VIRTUAL_HELPER = "virtual_helper"


@dataclass
class ConceptUpdate:
    concept: Concept
    kind: ConceptUpdateKind
    meta: Any | None = None


class _Absent:
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<absent>"


_ABSENT: Any = _Absent()


@dataclass
class _MirrorEntry:
    address: str
    prior: Any


@dataclass
class SemanticState:
    """Parser-owned concept state with explicit commit/rollback.

    v2 hydration must make newly declared concepts visible to later rules in
    the same parse. SemanticState records each pending write with its intent
    (``ConceptUpdateKind``) and mirrors it into ``environment.concepts`` so
    existing address-based lookups still resolve. The mirror is bookkept so
    ``rollback`` can reverse compatibility writes if the parse fails partway
    through.

    ``commit`` finalizes the pending updates. The compatibility mirror has
    already applied them, so commit is a bookkeeping step that seals the
    rollback window and returns the applied updates.
    """

    environment: Environment
    concepts: list[ConceptUpdate] = field(default_factory=list)
    _mirror: list[_MirrorEntry] = field(default_factory=list)
    _seen: set[str] = field(default_factory=set)
    _committed: bool = False

    def add(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> Concept:
        address = concept.address
        if address not in self._seen:
            prior = self.environment.concepts.get(address, _ABSENT)
            self._mirror.append(_MirrorEntry(address=address, prior=prior))
            self._seen.add(address)
        resolved = self.environment.add_concept(concept, meta, force=force)
        self.concepts.append(
            ConceptUpdate(concept=resolved, kind=kind, meta=meta)
        )
        return resolved

    def lookup(self, address: str) -> Concept | None:
        return self.environment.concepts.get(address)

    def pending(self) -> Iterator[ConceptUpdate]:
        return iter(self.concepts)

    def commit(
        self, environment: Environment | None = None
    ) -> list[ConceptUpdate]:
        if environment is not None and environment is not self.environment:
            raise ValueError(
                "SemanticState committed against a different environment"
            )
        committed = list(self.concepts)
        self._mirror.clear()
        self._seen.clear()
        self._committed = False
        return committed

    def rollback(self) -> None:
        if self._committed:
            return
        for entry in reversed(self._mirror):
            if entry.prior is _ABSENT:
                self.environment.concepts.pop(entry.address, None)
            else:
                self.environment.concepts[entry.address] = entry.prior
        self._mirror.clear()
        self._seen.clear()
        self.concepts.clear()
