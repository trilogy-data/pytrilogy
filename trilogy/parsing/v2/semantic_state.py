from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Iterator

from trilogy.core.models.author import Concept, ConceptRef
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

    ``concepts`` accumulates the full update history across committed batches
    so callers can inspect what was applied. The in-flight batch is bounded
    by ``_pending_start``; ``commit`` advances that boundary and ``rollback``
    drops the in-flight entries and restores the mirrored environment keys.
    """

    environment: Environment
    concepts: list[ConceptUpdate] = field(default_factory=list)
    _mirror: list[_MirrorEntry] = field(default_factory=list)
    _seen: set[str] = field(default_factory=set)
    _pending_by_address: dict[str, Concept] = field(default_factory=dict)
    _pending_start: int = 0

    def add(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> Concept:
        address = concept.address
        if address not in self._seen:
            # Read through the raw dict to avoid EnvironmentConceptDict's
            # namespace/fallback resolution muddying the rollback snapshot.
            prior = self.environment.concepts.data.get(address, _ABSENT)
            self._mirror.append(_MirrorEntry(address=address, prior=prior))
            self._seen.add(address)
        resolved = self.environment.add_concept(concept, meta, force=force)
        self._pending_by_address[address] = resolved
        self.concepts.append(ConceptUpdate(concept=resolved, kind=kind, meta=meta))
        return resolved

    def lookup(self, address: str) -> Concept | None:
        pending = self._pending_by_address.get(address)
        if pending is not None:
            return pending
        return self.environment.concepts.data.get(address)

    def pending_lookup(self, address: str) -> Concept | None:
        return self._pending_by_address.get(address)

    def pending_concepts(self) -> Iterable[tuple[str, Concept]]:
        return self._pending_by_address.items()

    def pending(self) -> Iterator[ConceptUpdate]:
        return iter(self.concepts[self._pending_start :])

    def commit(self, environment: Environment | None = None) -> list[ConceptUpdate]:
        if environment is not None and environment is not self.environment:
            raise ValueError("SemanticState committed against a different environment")
        committed = self.concepts[self._pending_start :]
        self._mirror.clear()
        self._seen.clear()
        self._pending_by_address.clear()
        self._pending_start = len(self.concepts)
        return list(committed)

    def rollback(self) -> None:
        data = self.environment.concepts.data
        for entry in reversed(self._mirror):
            if entry.prior is _ABSENT:
                data.pop(entry.address, None)
            else:
                data[entry.address] = entry.prior
        del self.concepts[self._pending_start :]
        self._mirror.clear()
        self._seen.clear()
        self._pending_by_address.clear()


class ConceptLookup:
    """Parser-owned concept lookup facade.

    Resolves concepts by address, preferring the current parse's in-flight
    (uncommitted) concepts from ``SemanticState`` and falling back to the
    base ``Environment``. Rule modules in ``trilogy.parsing.v2`` should go
    through this instead of reading ``environment.concepts`` directly, so
    we can eventually stop mirroring pending writes into the environment.
    """

    __slots__ = ("_state", "_env")

    def __init__(self, state: SemanticState) -> None:
        self._state = state
        self._env = state.environment

    def require(self, address: str) -> Concept:
        pending = self._state.pending_lookup(address)
        if pending is not None:
            return pending
        return self._env.concepts[address]  # type: ignore[return-value]

    def get(self, address: str) -> Concept | None:
        pending = self._state.pending_lookup(address)
        if pending is not None:
            return pending
        return self._env.concepts.get(address)

    def contains(self, address: str) -> bool:
        if self._state.pending_lookup(address) is not None:
            return True
        return address in self._env.concepts

    def __contains__(self, address: object) -> bool:
        return isinstance(address, str) and self.contains(address)

    def __getitem__(self, address: str) -> Concept:
        return self.require(address)

    def values(self) -> list[Concept]:
        merged: dict[str, Concept] = {}
        for concept in self._env.concepts.values():
            merged[concept.address] = concept
        for address, concept in self._state.pending_concepts():
            merged[address] = concept
        return list(merged.values())

    def reference(self, address: str) -> ConceptRef:
        return self.require(address).reference
